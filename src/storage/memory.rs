use std::{
    collections::{btree_map::Entry, BTreeMap, HashMap},
    time::SystemTime,
};

use crate::value::RedisValue;

#[derive(Debug, Default)]
pub struct Memory {
    aux: HashMap<String, String>,
    data: BTreeMap<String, (RedisValue, Option<SystemTime>)>,
}

impl Memory {
    pub fn set_aux(&mut self, key: String, value: String) {
        self.aux.insert(key, value);
    }

    fn is_expired(expiration: Option<SystemTime>) -> bool {
        let Some(exp) = expiration else {
            return false;
        };

        SystemTime::now() > exp
    }
}

impl super::Storage for Memory {
    fn get_keys(&mut self) -> eyre::Result<impl IntoIterator<Item = &str>> {
        Ok(self.data.keys().map(|it| it.as_ref()))
    }

    fn get(&mut self, key: &str) -> eyre::Result<Option<RedisValue>> {
        let Some((v, expiration)) = self.data.get(key) else {
            return Ok(None);
        };

        if Self::is_expired(*expiration) {
            let _ = self.delete(key);
            return Ok(None);
        }

        Ok(Some(v.clone()))
    }

    fn get_mut(&mut self, key: &str) -> eyre::Result<Option<&mut RedisValue>> {
        match self.data.get(key) {
            Some((_, exp)) if Self::is_expired(*exp) => {
                self.data.remove(key);
                return Ok(None);
            }
            None => return Ok(None),
            _ => {}
        }

        let (v, _) = self.data.get_mut(key).unwrap();

        return Ok(Some(v));
    }

    fn get_or_insert(
        &mut self,
        key: &str,
        value: impl FnOnce() -> RedisValue,
    ) -> eyre::Result<&mut RedisValue> {
        match self.data.entry(key.to_owned()) {
            Entry::Occupied(o) if !Self::is_expired(o.get().1) => Ok(&mut o.into_mut().0),
            Entry::Occupied(mut o) => {
                o.insert((value(), None));
                Ok(&mut o.into_mut().0)
            }
            Entry::Vacant(v) => Ok(&mut v.insert((value(), None)).0),
        }
    }

    fn set(
        &mut self,
        key: &str,
        value: RedisValue,
        expiration: Option<SystemTime>,
    ) -> eyre::Result<()> {
        self.data.insert(key.to_owned(), (value, expiration));
        Ok(())
    }

    fn delete(&mut self, key: &str) -> eyre::Result<()> {
        self.data.remove(key);
        Ok(())
    }

    fn flush(&mut self) -> eyre::Result<()> {
        Ok(())
    }
}
