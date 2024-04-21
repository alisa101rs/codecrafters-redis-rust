use std::{
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::BytesMut;
use eyre::WrapErr;

use crate::{
    config::Config,
    engine::SharedEngine,
    error::RedisError,
    flag,
    replication::ReplicationState,
    request::{Arg, Extension},
    response::{IntoResponse, Resp2},
};

pub mod repl;
pub mod stream;

pub async fn ping() -> impl IntoResponse {
    "PONG"
}

pub async fn echo(Arg(msg): Arg<1>) -> impl IntoResponse {
    msg
}

pub async fn get(
    Extension(storage): Extension<SharedEngine>,
    Arg(key): Arg<1>,
) -> Result<Option<String>, RedisError> {
    let value = storage.get(&key)?;
    Ok(value)
}

flag!(Px, "px");

pub async fn set(
    Extension(storage): Extension<SharedEngine>,
    Arg(key): Arg<1>,
    Arg(value): Arg<2>,
    exp: Option<Px>,
) -> Result<impl IntoResponse, RedisError> {
    let eol = match exp {
        Some(Px(v)) => {
            let millis: u64 = v.parse().wrap_err("invalid px arg")?;
            Some(SystemTime::now() + Duration::from_millis(millis))
        }
        _ => None,
    };

    storage.set(&key, value, eol).await?;
    Ok("OK")
}

pub async fn info(
    Extension(state): Extension<ReplicationState>,
    _section: Option<Arg<1>>,
) -> Result<impl IntoResponse, RedisError> {
    use std::fmt::Write;

    let mut output = BytesMut::new();
    writeln!(output, "# Replication").unwrap();
    writeln!(output, "role:{}", state.role()).unwrap();

    writeln!(output, "master_replid:{}", state.id()).unwrap();
    writeln!(output, "master_repl_offset:{}", state.offset()).unwrap();

    Ok(output.freeze())
}

flag!(Get, "get");

pub async fn config(Extension(config): Extension<Arc<Config>>, Get(key): Get) -> impl IntoResponse {
    match &*key {
        "dir" => config
            .dir
            .as_ref()
            .map(|it| Resp2(("dir", it.display().to_string()))),
        "dbfilename" => config
            .dbfilename
            .clone()
            .map(|it| Resp2(("dbfilename", it))),
        _ => None,
    }
}

pub async fn keys(
    Extension(storage): Extension<SharedEngine>,
    Arg(pattern): Arg<1>,
) -> Result<impl IntoResponse, RedisError> {
    if pattern != "*" {
        return Err(RedisError::Smth);
    };

    let keys = storage.keys()?;

    Ok(Resp2(keys))
}

pub async fn key_type(
    Extension(storage): Extension<SharedEngine>,
    Arg(key): Arg<1>,
) -> Result<impl IntoResponse, RedisError> {
    Ok(Resp2(storage.get_type(&key)?.ok_or("none")))
}
