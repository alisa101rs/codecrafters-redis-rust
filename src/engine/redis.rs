use std::time::SystemTime;

use async_trait::async_trait;
use bytes::Bytes;
use eyre::eyre;
use parking_lot::Mutex;
use tokio::sync::broadcast;

use crate::{
    encoding::rdb,
    engine::{wait::WaitBuilder, Engine},
    error::RedisError,
    replication::master::{ReplicationCommand, ReplicationCommandQueue},
    storage::Storage,
    value::{RedisValue, Stream, StreamId, StreamRange, ValueType},
};

pub struct RedisEngine<S: Storage> {
    storage: Mutex<S>,
    replication_queue: ReplicationCommandQueue,
    updates: broadcast::Sender<String>,
}

impl<S: Storage> RedisEngine<S> {
    pub fn new(storage: S, replication_queue: ReplicationCommandQueue) -> Self {
        Self {
            storage: Mutex::new(storage),
            replication_queue,
            updates: broadcast::channel(128).0,
        }
    }
}

#[async_trait]
impl<S: Storage> Engine for RedisEngine<S> {
    fn keys(&self) -> Result<Vec<String>, RedisError> {
        Ok(self
            .storage
            .lock()
            .get_keys()?
            .into_iter()
            .map(|it| it.to_owned())
            .collect())
    }

    fn get(&self, key: &str) -> Result<Option<String>, RedisError> {
        let Some(data) = self.storage.lock().get(key)? else {
            return Ok(None);
        };

        let RedisValue::String(value) = data else {
            return Err(RedisError::InvalidType("string"));
        };

        Ok(Some(value))
    }

    fn get_type(&self, key: &str) -> Result<Option<ValueType>, RedisError> {
        Ok(self.storage.lock().get(key)?.map(|it| it.ty()))
    }

    fn append(
        &self,
        stream: &str,
        key: StreamId,
        value: Vec<String>,
    ) -> Result<StreamId, RedisError> {
        let mut storage = self.storage.lock();
        let RedisValue::Stream(s) =
            storage.get_or_insert(stream, || RedisValue::Stream(Stream::new()))?
        else {
            return Err(RedisError::InvalidType("stream"));
        };

        let key = s.append(key, value)?;
        drop(storage);

        let _ = self.updates.send(stream.to_owned());

        Ok(key)
    }

    fn range(
        &self,
        stream: &str,
        range: StreamRange,
        count: usize,
    ) -> Result<Vec<(StreamId, Vec<String>)>, RedisError> {
        let mut storage = self.storage.lock();
        let Some(RedisValue::Stream(stream)) = storage.get_mut(stream)? else {
            return Ok(vec![]);
        };
        let values = stream
            .range(range)
            .map(|(k, v)| (k, v.clone()))
            .take(count)
            .collect();

        Ok(values)
    }

    async fn set(
        &self,
        key: &str,
        value: String,
        expiration: Option<SystemTime>,
    ) -> Result<(), RedisError> {
        self.storage
            .lock()
            .set(key, RedisValue::String(value.clone()), expiration)?;

        self.replication_queue
            .send(ReplicationCommand::Write {
                key: key.to_owned(),
                value,
                expiration,
            })
            .await
            .map_err(|_| eyre!("Replication is broken"))?;

        let _ = self.updates.send(key.to_owned());

        Ok(())
    }

    fn wait(&self) -> WaitBuilder {
        WaitBuilder::new(self.updates.subscribe())
    }

    fn dump(&self) -> Bytes {
        Bytes::from_static(rdb::EMPTY)
    }
}
