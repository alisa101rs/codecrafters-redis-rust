mod stream;

use serde::Serialize;

pub use self::stream::{Stream, StreamId, StreamRange};

#[derive(Debug, Clone)]
pub enum RedisValue {
    String(String),
    Stream(Stream),
}

impl RedisValue {
    pub fn ty(&self) -> ValueType {
        match self {
            Self::String { .. } => ValueType::String,
            Self::Stream { .. } => ValueType::Stream,
        }
    }
}

#[derive(Debug, Copy, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum ValueType {
    String,
    Stream,
}

impl ValueType {
    #[allow(dead_code)]
    pub fn into_u8(self) -> u8 {
        match self {
            ValueType::String => 0,
            ValueType::Stream => 21,
        }
    }
}
