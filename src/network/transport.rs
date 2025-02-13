use std::future::Future;

use bytes::{Bytes, BytesMut};
use eyre::Context;
use nom::AsBytes;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    net::{TcpListener, TcpStream},
};

pub trait Transport: Sized + Send + Sync + 'static {
    type Address;
    type Listener: Listener<Transport = Self>;
    async fn bind(address: &Self::Address) -> eyre::Result<Self::Listener>;
    async fn connect(address: &Self::Address) -> eyre::Result<Self>;

    async fn write(&mut self, buffer: Bytes) -> eyre::Result<()>;
    async fn read(&mut self, buffer: &mut BytesMut) -> eyre::Result<usize>;
}

pub trait Listener: Sized + Send + Sync + 'static {
    type Transport: Transport;

    fn accept(
        &mut self,
    ) -> impl Future<Output = eyre::Result<(Self::Transport, <Self::Transport as Transport>::Address)>>
           + Send;
}

impl Transport for TcpStream {
    type Address = String;
    type Listener = TcpListener;

    async fn bind(address: &Self::Address) -> eyre::Result<Self::Listener> {
        TcpListener::bind(address).await.wrap_err("bind")
    }

    async fn connect(address: &Self::Address) -> eyre::Result<Self> {
        Self::connect(address).await.wrap_err("Could not connect")
    }

    async fn write(&mut self, buffer: Bytes) -> eyre::Result<()> {
        self.write_all(buffer.as_bytes())
            .await
            .wrap_err("writing to socket")?;
        Ok(())
    }

    async fn read(&mut self, buffer: &mut BytesMut) -> eyre::Result<usize> {
        let size = self
            .read_buf(buffer)
            .await
            .wrap_err("reading from socket")?;
        Ok(size)
    }
}

impl Listener for TcpListener {
    type Transport = TcpStream;

    async fn accept(
        &mut self,
    ) -> eyre::Result<(Self::Transport, <Self::Transport as Transport>::Address)> {
        let (s, a) = TcpListener::accept(&*self)
            .await
            .wrap_err("accepting connection")?;

        Ok((s, a.to_string()))
    }
}
