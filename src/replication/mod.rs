pub mod master;
pub mod replica;

use std::{fmt, ops::AddAssign, str::FromStr, sync::Arc};

use derive_more::{Add, AddAssign, Display, From, Into};
use eyre::WrapErr;
use parking_lot::Mutex;

use crate::{error::RedisError, network::NodeId};

pub type SharedTopology = Arc<Topology>;

#[derive(Clone, Debug)]
pub struct ReplicationState {
    offset: Arc<Mutex<OffsetId>>,
    id: Arc<Mutex<ReplicationId>>,
    role: Arc<NodeRole>,
}

impl ReplicationState {
    pub fn master() -> Self {
        Self {
            offset: Arc::new(Mutex::new(OffsetId::default())),
            id: Arc::new(Mutex::new(ReplicationId::random())),
            role: Arc::new(NodeRole::Master),
        }
    }

    pub fn replica() -> Self {
        Self {
            offset: Arc::new(Mutex::new(OffsetId::default())),
            id: Arc::new(Mutex::new(ReplicationId::default())),
            role: Arc::new(NodeRole::Replica),
        }
    }
    pub fn offset(&self) -> OffsetId {
        self.offset.lock().clone()
    }
    pub fn set_offset(&self, value: impl Into<OffsetId>) {
        *self.offset.lock() = value.into();
    }

    pub fn increment_offset(&self, value: u64) {
        *self.offset.lock() += value;
    }

    pub fn id(&self) -> ReplicationId {
        self.id.lock().clone()
    }
    pub fn set_id(&self, id: ReplicationId) {
        *self.id.lock() = id;
    }

    pub fn role(&self) -> NodeRole {
        *self.role
    }
}

#[derive(Debug, Copy, Clone, Default, Into, From)]
pub struct ReplicationId([u8; 20]);

impl ReplicationId {
    pub fn random() -> Self {
        Self(rand::random())
    }
}

impl FromStr for ReplicationId {
    type Err = eyre::Report;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let mut decoded = [0u8; 20];
        hex::decode_to_slice(s.as_bytes(), &mut decoded)
            .wrap_err("Invalid replication id string")?;
        Ok(Self(decoded))
    }
}

impl fmt::Display for ReplicationId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let mut encoded = [0u8; 40];
        hex::encode_to_slice(&self.0, &mut encoded).unwrap();
        write!(f, "{}", std::str::from_utf8(&encoded).unwrap())
    }
}

#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, Display, Into, From, AddAssign, Add, PartialOrd, Ord,
)]
pub struct OffsetId(u64);

impl OffsetId {
    pub fn incr(&mut self) {
        self.0 += 1
    }
}

impl AddAssign<u64> for OffsetId {
    fn add_assign(&mut self, rhs: u64) {
        self.0 += rhs;
    }
}

impl AddAssign<usize> for OffsetId {
    fn add_assign(&mut self, rhs: usize) {
        self.0 += rhs as u64;
    }
}

#[derive(Debug)]
pub enum Topology {
    Master { replicas: Mutex<Vec<NodeId>> },
    Replica { master: NodeId },
}

impl Topology {
    pub fn master() -> SharedTopology {
        Arc::new(Self::Master {
            replicas: Mutex::new(vec![]),
        })
    }

    pub fn replica(master: NodeId) -> SharedTopology {
        Arc::new(Self::Replica { master })
    }

    pub fn add(&self, replica: NodeId) -> Result<(), RedisError> {
        let Self::Master { replicas } = self else {
            return Err(RedisError::NotMaster);
        };

        let mut replicas = replicas.lock();
        replicas.push(replica);

        Ok(())
    }
}

#[derive(Debug, Clone, Copy, PartialOrd, PartialEq, Eq, Hash, Display)]
pub enum NodeRole {
    #[display(fmt = "master")]
    Master,
    #[display(fmt = "slave")]
    Replica,
}
