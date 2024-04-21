use std::{
    fs::File,
    io::Read,
    path::{Path, PathBuf},
    time::SystemTime,
};

use eyre::WrapErr;
use tracing::instrument;

use crate::{
    encoding,
    storage::{Memory, Storage},
    value::RedisValue,
};

#[derive(Debug)]
pub struct Persisted {
    memory: Memory,
    #[allow(dead_code)]
    db: File,
}

impl Persisted {
    #[instrument(skip(memory), err)]
    pub fn new(mut memory: Memory, db: PathBuf) -> eyre::Result<Self> {
        let db = match File::open(&db) {
            Ok(mut f) => {
                Self::load(&mut memory, &mut f)?;
                f
            }
            Err(_) => Self::create(&db).wrap_err("Couldn't create db dump file")?,
        };

        Ok(Self { memory, db })
    }

    fn create(path: impl AsRef<Path>) -> eyre::Result<File> {
        Ok(File::create(path).wrap_err("Couldn't create db file")?)
    }

    #[instrument(skip(memory, file), err)]
    fn load(memory: &mut Memory, file: &mut File) -> eyre::Result<()> {
        let mut data = vec![];
        file.read_to_end(&mut data)
            .wrap_err("Failed to read rdb file")?;

        let (aux_data, entries) = encoding::read_rdb_file(data.as_slice())?;

        aux_data
            .into_iter()
            .for_each(|(key, value)| memory.set_aux(key, value));

        for (key, value, meta) in entries {
            tracing::debug!(%key, "Loading key");
            memory
                .set(&key, value, meta)
                .wrap_err("failed to write into memory")?;
        }

        Ok(())
    }
}

impl super::Storage for Persisted {
    fn get_keys(&mut self) -> eyre::Result<impl IntoIterator<Item = &str>> {
        self.memory.get_keys()
    }

    fn get(&mut self, key: &str) -> eyre::Result<Option<RedisValue>> {
        self.memory.get(key)
    }

    fn get_mut(&mut self, key: &str) -> eyre::Result<Option<&mut RedisValue>> {
        self.memory.get_mut(key)
    }

    fn get_or_insert(
        &mut self,
        key: &str,
        value: impl FnOnce() -> RedisValue,
    ) -> eyre::Result<&mut RedisValue> {
        self.memory.get_or_insert(key, value)
    }

    fn set(
        &mut self,
        key: &str,
        value: RedisValue,
        expiration: Option<SystemTime>,
    ) -> eyre::Result<()> {
        self.memory.set(key, value, expiration)
    }

    fn delete(&mut self, key: &str) -> eyre::Result<()> {
        self.memory.delete(key)
    }

    fn flush(&mut self) -> eyre::Result<()> {
        todo!()
    }
}
