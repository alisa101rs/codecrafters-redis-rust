mod extension;
mod flag;

use std::str::FromStr;

use async_trait::async_trait;

pub use self::extension::Extension;
use crate::{error::RedisError, state::ConnectionState, util::Extensions};

#[derive(Debug, Clone)]
pub struct Request {
    pub command: String,
    pub args: Vec<String>,
    state: ConnectionState,
    extensions: Extensions,
}

impl Request {
    pub fn from_command_line(
        args: Vec<String>,
        state: ConnectionState,
    ) -> Result<Self, RedisError> {
        let mut args = args.into_iter();
        let command = args.next().ok_or_else(|| RedisError::Smth)?.to_lowercase();

        Ok(Self {
            command,
            args: args.collect(),
            state,
            extensions: Default::default(),
        })
    }

    pub fn extensions(&self) -> &Extensions {
        &self.extensions
    }

    pub fn extensions_mut(&mut self) -> &mut Extensions {
        &mut self.extensions
    }

    pub fn state(&self) -> &ConnectionState {
        &self.state
    }
}

#[async_trait]
pub trait FromRequest: Sized {
    async fn from_request(request: Request) -> Result<Self, RedisError>;
}

#[async_trait]
impl FromRequest for Request {
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        Ok(request)
    }
}

#[async_trait]
impl<T: FromRequest> FromRequest for Option<T> {
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        Ok(T::from_request(request).await.ok())
    }
}

pub struct Arg<const N: usize>(pub String);

#[async_trait]
impl<const N: usize> FromRequest for Arg<N> {
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        if N == 0 {
            return Ok(Arg(request.command.clone()));
        }

        match request.args.get(N - 1) {
            None => Err(RedisError::Smth),
            Some(v) => Ok(Arg(v.clone())),
        }
    }
}

pub struct ArgParse<T, const N: usize>(pub T);

#[async_trait]
impl<T, const N: usize> FromRequest for ArgParse<T, N>
where
    T: FromStr,
    RedisError: From<<T as FromStr>::Err>,
{
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        let Arg(value) = Arg::<N>::from_request(request).await?;
        let parsed = value.parse()?;

        Ok(Self(parsed))
    }
}
