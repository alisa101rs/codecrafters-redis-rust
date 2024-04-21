use std::{fmt, net::SocketAddr, sync::Arc};

use async_trait::async_trait;
use parking_lot::Mutex;

use crate::{
    error::RedisError,
    network::NodeId,
    request::{FromRequest, Request},
};

#[derive(Clone)]
pub struct ConnectionState(Arc<Mutex<ConnectionStateInner>>);

impl ConnectionState {
    pub fn new(addr: SocketAddr) -> Self {
        Self(Arc::new(Mutex::new(ConnectionStateInner::new(addr))))
    }

    pub fn node_id(&self) -> Option<NodeId> {
        self.0.lock().node_id
    }

    pub fn set_node_id(&self, id: NodeId) {
        self.0.lock().node_id = Some(id);
    }

    pub fn addr(&self) -> SocketAddr {
        self.0.lock().addr
    }
}

impl fmt::Debug for ConnectionState {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "ConnectionState")
    }
}

struct ConnectionStateInner {
    addr: SocketAddr,
    node_id: Option<NodeId>,
}

impl ConnectionStateInner {
    pub fn new(addr: SocketAddr) -> Self {
        Self {
            addr,
            node_id: None,
        }
    }
}

#[async_trait]
impl FromRequest for ConnectionState {
    async fn from_request(request: Request) -> Result<Self, RedisError> {
        Ok(request.state().clone())
    }
}
