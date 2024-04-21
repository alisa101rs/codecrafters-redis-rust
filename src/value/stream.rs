use std::{
    collections::BTreeMap,
    ops::{Bound, RangeBounds},
    str::FromStr,
    time::{SystemTime, UNIX_EPOCH},
};

use derive_more::{Display, From, Into};
use eyre::eyre;
use serde::Serialize;

use crate::error::RedisError;

#[derive(Debug, Clone, Copy, Eq, PartialOrd, PartialEq, Ord, From, Into, Display, Serialize)]
#[display(fmt = "{}-{}", _0, _1)]
#[serde(into = "String")]
pub struct StreamId(u64, u64);

impl StreamId {
    pub const MAX: StreamId = StreamId(u64::MAX, u64::MAX);
    pub const MIN: StreamId = StreamId(u64::MIN, u64::MIN);
}
impl Into<String> for StreamId {
    fn into(self) -> String {
        self.to_string()
    }
}

impl FromStr for StreamId {
    type Err = RedisError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "*" => return Ok(StreamId::MAX),
            _ => {}
        }
        let Some((ts, c)) = s.split_once("-") else {
            return Err(RedisError::Unhandled(eyre!("Could not parse stream id")));
        };

        let ts = ts.parse().map_err(|_| eyre!("Could not parse stream id"))?;

        let c = match c {
            "*" => u64::MAX,
            _ => c.parse().map_err(|_| eyre!("Could not parse stream id"))?,
        };
        Ok(StreamId::from((ts, c)))
    }
}

#[derive(Debug, Clone, Copy, From)]
pub struct StreamRange(pub Bound<StreamId>, pub Bound<StreamId>);

impl RangeBounds<StreamId> for StreamRange {
    fn start_bound(&self) -> Bound<&StreamId> {
        self.0.as_ref()
    }

    fn end_bound(&self) -> Bound<&StreamId> {
        self.1.as_ref()
    }
}

#[derive(Debug, Clone)]
pub struct Stream {
    entries: BTreeMap<StreamId, Vec<String>>,
    last_id: StreamId,
    #[allow(dead_code)]
    first_id: StreamId,
    #[allow(dead_code)]
    max_deleted_entry_id: Option<StreamId>,
    entries_added: usize,
}

impl Stream {
    pub fn new() -> Self {
        Self {
            entries: Default::default(),
            last_id: StreamId(0, 0),
            first_id: StreamId(0, 0),
            max_deleted_entry_id: None,
            entries_added: 0,
        }
    }

    #[allow(dead_code)]
    pub fn len(&self) -> usize {
        self.entries.len()
    }

    fn map_key_allocation(&mut self, mut key: StreamId) -> StreamId {
        if key.0 == u64::MAX {
            key.0 = SystemTime::now()
                .duration_since(UNIX_EPOCH)
                .unwrap()
                .as_millis() as u64;
        }

        key.1 = match key.1 {
            u64::MAX if self.last_id.0 == key.0 => self.last_id.1 + 1,
            u64::MAX => 0,
            x => x,
        };

        key
    }

    pub fn append(&mut self, key: StreamId, value: Vec<String>) -> Result<StreamId, RedisError> {
        if key == StreamId(0, 0) {
            return Err(eyre!("ERR The ID specified in XADD must be greater than 0-0").into());
        }
        if key <= self.last_id {
            return Err(RedisError::Unhandled(eyre!(
                "ERR The ID specified in XADD is equal or smaller than the target stream top item"
            )));
        }
        let key = self.map_key_allocation(key);

        self.entries.insert(key, value);
        self.last_id = key;
        if self.entries_added == 0 {
            self.first_id = key;
        }
        self.entries_added += 1;

        Ok(key)
    }

    pub fn range(&self, mut range: StreamRange) -> impl Iterator<Item = (StreamId, &Vec<String>)> {
        range.0 = range.0.map(|it| {
            if it == StreamId::MAX {
                self.last_id
            } else {
                it
            }
        });
        self.entries.range(range).map(|(k, v)| (*k, v))
    }
}
