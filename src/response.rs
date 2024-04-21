use bytes::Bytes;
use eyre::WrapErr;
use nom::AsBytes;
use serde::Serialize;
use tokio::io::{AsyncWrite, AsyncWriteExt};

use crate::{encoding::resp2, error::RedisError, replication::OffsetId};

pub enum Response {
    Raw(Bytes),
    Empty,
    Upgrade { offset: OffsetId },
}

impl Response {
    pub fn new(v: impl AsRef<[u8]>) -> Self {
        Self::Raw(Bytes::copy_from_slice(v.as_ref()))
    }

    pub fn is_upgrade(&self) -> bool {
        matches!(self, Self::Upgrade { .. })
    }

    pub async fn write(self, write: &mut (impl AsyncWrite + Unpin)) -> eyre::Result<()> {
        match self {
            Self::Raw(b) => {
                write
                    .write_all(b.as_bytes())
                    .await
                    .wrap_err("Failed to write response")?;
                Ok(())
            }
            Self::Empty => Ok(()),
            Response::Upgrade { .. } => unreachable!(),
        }
    }
}

pub trait IntoResponse: Sized {
    fn into_response(self) -> Response;
}

impl IntoResponse for Response {
    fn into_response(self) -> Response {
        self
    }
}

impl IntoResponse for Bytes {
    fn into_response(self) -> Response {
        Response::Raw(resp2::to_bytes(&self).expect("shouldn't really fail"))
    }
}

impl<T: IntoResponse> IntoResponse for Option<T> {
    fn into_response(self) -> Response {
        match self {
            Some(v) => v.into_response(),
            None => Response::Raw(resp2::to_bytes(&Option::<()>::None).unwrap()),
        }
    }
}

macro_rules! impl_for_primitive {
    ($t: ty) => {
        impl IntoResponse for $t {
            fn into_response(self) -> Response {
                Response::Raw(resp2::to_bytes(&self).expect("shouldn't really fail"))
            }
        }
    };
    ($( $t: ty ),*) => {
        $( impl_for_primitive!($t); )*
    };
}

impl_for_primitive!(
    usize,
    u8,
    u16,
    u32,
    u64,
    isize,
    i8,
    i16,
    i32,
    i64,
    &'static str,
    String
);

impl<T: IntoResponse> IntoResponse for Result<T, RedisError> {
    fn into_response(self) -> Response {
        match self {
            Ok(v) => v.into_response(),
            Err(er) => er.into_response(),
        }
    }
}

impl IntoResponse for () {
    fn into_response(self) -> Response {
        Response::Empty
    }
}

#[derive(Debug, Serialize)]
#[serde(transparent)]
pub struct Resp2<T>(pub T);

impl<T: Serialize> IntoResponse for Resp2<T> {
    fn into_response(self) -> Response {
        Response::Raw(resp2::to_bytes(&self).expect("shouldn't really fail"))
    }
}

impl<L: IntoResponse, R: IntoResponse> IntoResponse for Result<L, R> {
    fn into_response(self) -> Response {
        match self {
            Ok(l) => l.into_response(),
            Err(r) => r.into_response(),
        }
    }
}
