use std::fmt::Display;

use serde::{de, ser};
use thiserror::Error;

#[derive(Clone, Debug, Error)]
pub enum Error {
    #[error("{0}")]
    Message(String),
    #[error("TODO")]
    SmthFailed,
    #[error("Input contains extra characters that were not consumed")]
    TrailingCharacters,
    #[error("Expected array `*<length>`, got something else")]
    ExpectedArray,
    #[error("Expected string, got something else")]
    ExpectedString,
    #[error("Bad number")]
    ExpectedNumber,
}

impl ser::Error for Error {
    fn custom<T>(msg: T) -> Self
    where
        T: Display,
    {
        Error::Message(msg.to_string())
    }
}

impl de::Error for Error {
    fn custom<T: Display>(msg: T) -> Self {
        Error::Message(msg.to_string())
    }
}

impl From<nom::Err<nom::error::Error<&[u8]>>> for Error {
    fn from(_value: nom::Err<nom::error::Error<&[u8]>>) -> Self {
        Self::SmthFailed
    }
}
