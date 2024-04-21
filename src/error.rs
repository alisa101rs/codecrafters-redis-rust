use bytes::Bytes;
use thiserror::Error;

use crate::routing::Response;

#[derive(Debug, Error)]
pub enum RedisError {
    #[error("Error during IO")]
    Io(#[from] std::io::Error),
    #[error(transparent)]
    SerializationError(#[from] crate::encoding::Error),
    #[error("Something happened")]
    Smth,
    #[error("Unknown command")]
    UnknownCommand,

    #[error(transparent)]
    Unhandled(#[from] eyre::Report),

    #[error("Failed to receive response")]
    ResponseFailed,

    #[error("Request can't be processed by replica node")]
    NotMaster,

    #[error("Expected to receive a number")]
    ExpectedNumber(#[from] std::num::ParseIntError),

    #[error("Expected type `{0}`")]
    InvalidType(&'static str),
}

impl RedisError {
    pub fn into_response(&self) -> Response {
        Response::Raw(Bytes::from(format!("-{self}\r\n")))
    }
}
