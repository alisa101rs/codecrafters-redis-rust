use std::{collections::HashMap, net::SocketAddr};

use bytes::{Buf, Bytes, BytesMut};
use eyre::WrapErr;
use futures_util::{stream::FuturesUnordered, StreamExt};
use nom::{AsBytes, FindSubstring};
use serde::{de::DeserializeOwned, Serialize};
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::TcpStream,
};
use tracing::instrument;

use crate::{
    error::{RedisError, RedisError::ResponseFailed},
    replication::{NodeRole, OffsetId, ReplicationId},
    response::Response,
};

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash)]
pub struct NodeId {
    addr: SocketAddr,
    connection_addr: SocketAddr,
    role: NodeRole,
}

impl NodeId {
    pub fn master(addr: SocketAddr) -> Self {
        Self {
            addr,
            connection_addr: addr,
            role: NodeRole::Master,
        }
    }

    pub fn replica(addr: SocketAddr) -> Self {
        Self {
            addr,
            connection_addr: addr,
            role: NodeRole::Replica,
        }
    }

    pub fn with_connection_addr(mut self, connection_addr: SocketAddr) -> Self {
        self.connection_addr = connection_addr;
        self
    }

    pub fn addr(&self) -> SocketAddr {
        self.addr
    }
}

pub trait Network {
    async fn send_raw(&mut self, target: &NodeId, data: Bytes) -> Result<(), RedisError>;
    async fn send<T: Serialize>(&mut self, target: &NodeId, data: &T) -> Result<(), RedisError>;
    async fn receive_rdb(&mut self, target: &NodeId) -> Result<Bytes, RedisError>;
    async fn receive<T: DeserializeOwned>(
        &mut self,
        target: &NodeId,
    ) -> Result<(T, usize), RedisError>;
    async fn broadcast<T: Serialize>(&mut self, data: &T) -> Result<usize, RedisError>;
}

pub trait NetworkExt: Network {
    async fn respond(&mut self, node: &NodeId, response: Response) -> Result<(), RedisError> {
        match response {
            Response::Raw(data) => {
                self.send_raw(node, data).await?;
            }
            Response::Empty => {}
            Response::Upgrade { .. } => {}
        }
        Ok(())
    }
    async fn ping(&mut self, target: &NodeId) -> Result<(), RedisError> {
        self.send(target, &vec![Bytes::from_static(b"ping")])
            .await?;

        let _ = self.receive::<String>(target).await?;

        Ok(())
    }
    async fn psync(&mut self, target: &NodeId) -> Result<(ReplicationId, OffsetId), RedisError> {
        self.send(
            target,
            &vec![
                Bytes::from_static(b"PSYNC"),
                Bytes::from_static(b"?"),
                Bytes::from_static(b"-1"),
            ],
        )
        .await?;

        let (resp, _) = self.receive::<String>(target).await?;
        let (c, resp) = resp
            .split_once(" ")
            .ok_or_else(|| RedisError::ResponseFailed)?;
        assert_eq!(c, "FULLRESYNC", "Only support fullresync now");

        let (replication_id, offset_id) = resp
            .split_once(" ")
            .ok_or_else(|| RedisError::ResponseFailed)?;

        let replication = replication_id.parse()?;
        let offset: u64 = offset_id.parse().map_err(|_| RedisError::ResponseFailed)?;
        let offset = OffsetId::from(offset);

        Ok((replication, offset))
    }
    async fn exec(
        &mut self,
        target: &NodeId,
        command: &'static str,
        args: Vec<Bytes>,
    ) -> Result<(), RedisError> {
        let body = Iterator::chain(
            [Bytes::from_static(command.as_bytes())].into_iter(),
            args.into_iter(),
        )
        .collect::<Vec<Bytes>>();

        self.send(target, &body).await?;
        let (response, _) = self.receive::<String>(target).await?;

        tracing::debug!(%response, "Received response");

        Ok(())
    }

    async fn send_rdb(&mut self, target: &NodeId, data: Bytes) -> Result<(), RedisError> {
        use std::fmt::Write;

        let mut prefix = BytesMut::new();
        write!(&mut prefix, "${}\r\n", data.len()).unwrap();
        self.send_raw(target, prefix.freeze()).await?;
        self.send_raw(target, data).await?;
        Ok(())
    }
}

impl<T: Network> NetworkExt for T {}

pub struct RedisNetwork {
    connections: HashMap<NodeId, OpenedConnection>,
}

impl RedisNetwork {
    pub async fn new(init: impl IntoIterator<Item = NodeId>) -> eyre::Result<Self> {
        let mut connections = HashMap::new();
        for node in init {
            let connection = OpenedConnection::open(&node).await?;
            connections.insert(node, connection);
        }

        Ok(Self { connections })
    }

    pub(crate) fn add_connection(
        &mut self,
        target: &NodeId,
        stream: TcpStream,
    ) -> eyre::Result<()> {
        self.connections.insert(
            target.clone(),
            OpenedConnection {
                stream,
                buf: BytesMut::new(),
            },
        );

        Ok(())
    }

    async fn get_connection(&mut self, target: &NodeId) -> eyre::Result<&mut OpenedConnection> {
        if self.connections.contains_key(target) {
            return Ok(self.connections.get_mut(target).unwrap());
        }
        let new_connection = OpenedConnection::open(target).await?;

        Ok(self
            .connections
            .entry(target.clone())
            .or_insert(new_connection))
    }
}

impl Network for RedisNetwork {
    async fn send_raw(&mut self, target: &NodeId, data: Bytes) -> Result<(), RedisError> {
        let connection = self.get_connection(target).await?;
        connection.request_raw(data).await?;
        Ok(())
    }

    #[instrument(skip(self, data), ret, err)]
    async fn send<T: Serialize>(&mut self, target: &NodeId, data: &T) -> Result<(), RedisError> {
        let connection = self.get_connection(target).await?;
        connection.request(data).await?;
        Ok(())
    }

    async fn receive_rdb(&mut self, target: &NodeId) -> Result<Bytes, RedisError> {
        let connection = self.get_connection(target).await?;

        connection.receive_rdb().await
    }

    #[instrument(skip(self), err)]
    async fn receive<T: DeserializeOwned>(
        &mut self,
        target: &NodeId,
    ) -> Result<(T, usize), RedisError> {
        let connection = self.get_connection(target).await?;

        connection.receive().await
    }

    #[instrument(skip(self, data), ret, err)]
    async fn broadcast<T: Serialize>(&mut self, data: &T) -> Result<usize, RedisError> {
        let mut f = FuturesUnordered::new();
        let data = crate::encoding::resp2::to_bytes(data).unwrap();
        let sent_bytes = data.len();

        for (node, mut connection) in self.connections.drain() {
            let data = data.clone();
            f.push(async move {
                let result = connection.request_raw(data).await;

                (node, connection, result)
            });
        }
        let mut error = None;

        while let Some((node, connection, result)) = f.next().await {
            self.connections.insert(node, connection);
            error = error.or(result.err());
        }

        Ok(sent_bytes)
    }
}

struct OpenedConnection {
    stream: TcpStream,
    buf: BytesMut,
}

impl OpenedConnection {
    async fn open(node: &NodeId) -> eyre::Result<Self> {
        let stream = TcpStream::connect(&node.addr)
            .await
            .wrap_err("Connection to node")?;

        Ok(Self {
            stream,
            buf: BytesMut::new(),
        })
    }

    #[instrument(skip(self), err)]
    async fn request_raw(&mut self, buffer: Bytes) -> Result<(), RedisError> {
        tracing::debug!(?buffer, "Sending request");
        self.stream.write_all(buffer.as_bytes()).await?;
        Ok(())
    }

    #[instrument(skip(self, body), err)]
    async fn request<T: Serialize>(&mut self, body: &T) -> Result<(), RedisError> {
        let buffer = crate::encoding::resp2::to_bytes(&body).expect("to serialize request");
        self.request_raw(buffer).await
    }

    #[instrument(skip(self), err)]
    async fn receive_rdb(&mut self) -> Result<Bytes, RedisError> {
        let OpenedConnection { stream, buf, .. } = self;

        // size loop
        let size: usize = loop {
            if let Some(pos) = buf.as_bytes().find_substring("\r\n") {
                let prefix = buf.split_to(pos + 2);
                let len = prefix
                    .strip_prefix(b"$")
                    .ok_or_else(|| ResponseFailed)?
                    .strip_suffix(b"\r\n")
                    .unwrap();
                let len = std::str::from_utf8(len).map_err(|_| ResponseFailed)?;

                break len.parse().map_err(|_| ResponseFailed)?;
            }

            let read = stream.read_buf(buf).await.map_err(|_| ResponseFailed)?;
            if read == 0 && buf.is_empty() {
                tracing::trace!("EOF too early");
                return Err(ResponseFailed);
            }
        };
        tracing::trace!("Expecting {size} to receive bytes");
        buf.reserve(size);

        loop {
            if buf.len() >= size {
                break;
            }
            let read = stream.read_buf(buf).await.map_err(|_| ResponseFailed)?;

            if read == 0 {
                tracing::trace!("EOF too early");
                return Err(ResponseFailed);
            }
        }

        Ok(buf.split_to(size).freeze())
    }

    #[instrument(skip(self), err)]
    async fn receive<T: DeserializeOwned>(&mut self) -> Result<(T, usize), RedisError> {
        let OpenedConnection { stream, buf, .. } = self;
        loop {
            if let Ok((result, read)) = crate::encoding::resp2::from_bytes(buf.as_bytes()) {
                buf.advance(read);
                return Ok((result, read));
            };

            let read = stream
                .read_buf(buf)
                .await
                .map_err(|_| RedisError::ResponseFailed)?;

            if read == 0 {
                return Err(RedisError::ResponseFailed);
            }
        }
    }
}
