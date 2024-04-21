use std::{sync::Arc, time::SystemTime};

use async_trait::async_trait;
use bytes::Bytes;
use tokio::sync::mpsc;

use crate::{
    config::Config,
    engine::wait::WaitBuilder,
    error::RedisError,
    replication::master::ReplicationCommand,
    storage,
    value::{StreamId, StreamRange, ValueType},
};

mod redis;
mod wait;

pub use self::redis::RedisEngine;

#[async_trait]
pub trait Engine {
    fn keys(&self) -> Result<Vec<String>, RedisError>;
    fn get(&self, key: &str) -> Result<Option<String>, RedisError>;
    fn get_type(&self, key: &str) -> Result<Option<ValueType>, RedisError>;
    fn append(
        &self,
        stream: &str,
        key: StreamId,
        value: Vec<String>,
    ) -> Result<StreamId, RedisError>;
    fn range(
        &self,
        stream: &str,
        range: StreamRange,
        count: usize,
    ) -> Result<Vec<(StreamId, Vec<String>)>, RedisError>;
    async fn set(
        &self,
        key: &str,
        value: String,
        eol: Option<SystemTime>,
    ) -> Result<(), RedisError>;
    fn wait(&self) -> WaitBuilder;
    fn dump(&self) -> Bytes;
}

pub type SharedEngine = Arc<dyn Engine + Send + Sync + 'static>;

pub fn create_engine(
    config: &Config,
) -> eyre::Result<(SharedEngine, mpsc::Receiver<ReplicationCommand>)> {
    let (tx, rx) = mpsc::channel(128);

    let memstore = storage::Memory::default();

    Ok(match config.db_file() {
        None => (Arc::new(RedisEngine::new(memstore, tx)), rx),

        Some(db) => {
            let persisted = storage::Persisted::new(memstore, db)?;
            (Arc::new(RedisEngine::new(persisted, tx)), rx)
        }
    })
}
