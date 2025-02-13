use async_stream::stream;
use bytes::{Buf, Bytes, BytesMut};
use eyre::{bail, ContextCompat, WrapErr};
use futures_util::{stream::BoxStream, StreamExt};
use nom::FindSubstring;
use serde::{de::DeserializeOwned, Serialize};

use crate::network::transport::{Listener, Transport};

#[derive(Debug)]
pub struct Connection<T> {
    transport: T,
    buffer: BytesMut,
}

impl<T: Transport> Connection<T> {
    pub async fn bind(addr: &T::Address) -> eyre::Result<BoxStream<'static, Self>> {
        let mut listener = T::bind(addr).await?;

        let s = stream! {
            loop {
                let Ok((transport, _)) = listener.accept().await else { continue };

                yield Self {
                    transport,
                    buffer: BytesMut::new(),
                }
            }
        };

        Ok(s.boxed())
    }

    pub async fn open(addr: &T::Address) -> eyre::Result<Self> {
        Ok(Self {
            transport: T::connect(addr).await?,
            buffer: BytesMut::new(),
        })
    }

    async fn request_raw(&mut self, buffer: Bytes) -> eyre::Result<()> {
        self.transport.write(buffer).await?;
        Ok(())
    }

    async fn request<B: Serialize>(&mut self, body: &B) -> eyre::Result<()> {
        let buffer = crate::encoding::resp2::to_bytes(&body).expect("to serialize request");
        self.request_raw(buffer).await
    }

    async fn receive_rdb(&mut self) -> eyre::Result<Bytes> {
        let Connection {
            transport, buffer, ..
        } = self;

        // size loop
        let size: usize = loop {
            if let Some(pos) = buffer.as_ref().find_substring("\r\n") {
                let prefix = buffer.split_to(pos + 2);
                let len = prefix
                    .strip_prefix(b"$")
                    .wrap_err("Expecting to receive RDB but got something else")?
                    .strip_suffix(b"\r\n")
                    .unwrap();

                let len = std::str::from_utf8(len).wrap_err("Expected len to be valid utf8")?;

                break len.parse().wrap_err("expected length to be valid number")?;
            }

            let read = transport
                .read(buffer)
                .await
                .wrap_err("failed to read data")?;
            if read == 0 && buffer.is_empty() {
                bail!("EOF too early");
            }
        };
        buffer.reserve(size);

        loop {
            if buffer.len() >= size {
                break;
            }
            let read = transport
                .read(buffer)
                .await
                .wrap_err("failed while trying to receive the rest of rdb")?;

            if read == 0 {
                bail!("EOF too early")
            }
        }

        Ok(buffer.split_to(size).freeze())
    }

    async fn receive<B: DeserializeOwned>(&mut self) -> eyre::Result<(B, usize)> {
        let Connection {
            transport, buffer, ..
        } = self;
        loop {
            if let Ok((result, read)) = crate::encoding::resp2::from_bytes(buffer.as_ref()) {
                buffer.advance(read);
                return Ok((result, read));
            };

            let read = transport
                .read(buffer)
                .await
                .wrap_err("failed while trying to receive the rest of request")?;

            if read == 0 {
                bail!("EOF too early")
            }
        }
    }
}
