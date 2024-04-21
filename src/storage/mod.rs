mod memory;
mod persisted;

use std::{fmt, time::SystemTime};

use eyre::Result;
pub use memory::Memory;
pub use persisted::Persisted;

use crate::value::RedisValue;

pub trait Storage: fmt::Debug + Send + Sync {
    fn get_keys(&mut self) -> Result<impl IntoIterator<Item = &str>>;
    /// Gets a value for a key, if it exists.
    fn get(&mut self, key: &str) -> Result<Option<RedisValue>>;
    fn get_mut(&mut self, key: &str) -> Result<Option<&mut RedisValue>>;
    fn get_or_insert(
        &mut self,
        key: &str,
        value: impl FnOnce() -> RedisValue,
    ) -> Result<&mut RedisValue>;

    /// Sets a value for a key, replacing the existing value if any.
    fn set(&mut self, key: &str, value: RedisValue, expiration: Option<SystemTime>) -> Result<()>;

    /// Deletes a key, or does nothing if it does not exist.
    fn delete(&mut self, key: &str) -> Result<()>;

    /// Flushes any buffered data to the underlying storage medium.
    fn flush(&mut self) -> Result<()>;
}
