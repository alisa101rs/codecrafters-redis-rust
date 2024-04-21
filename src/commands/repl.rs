use std::{net::ToSocketAddrs, sync::Arc, time::Duration};

use eyre::{eyre, WrapErr};
use tokio::{
    sync::{oneshot, Notify},
    time::Instant,
};
use tracing::instrument;

use crate::{
    error::RedisError,
    network::NodeId,
    replication::{
        master::ReplicationWaitQueue, NodeRole, OffsetId, ReplicationState, SharedTopology,
    },
    request::{Arg, ArgParse, Extension},
    response::{IntoResponse, Response},
    state::ConnectionState,
};

pub async fn config(
    Extension(topology): Extension<SharedTopology>,
    state: ConnectionState,
    Arg(key): Arg<1>,
    Arg(value): Arg<2>,
) -> Result<impl IntoResponse, RedisError> {
    match key.as_str() {
        "listening-port" => {
            let port: u16 = value.parse().wrap_err("invalid argument, expected port")?;
            let mut addrs = ("localhost", port)
                .to_socket_addrs()
                .wrap_err("can't resolve")?;
            let addr = addrs
                .next()
                .ok_or_else(|| eyre!("Can't resolve replica address"))?;
            let id = NodeId::replica(addr).with_connection_addr(state.addr());
            topology.add(id)?;
            state.set_node_id(id);
        }
        _ => {}
    }

    Ok("OK")
}

#[instrument(err)]
pub async fn psync(
    Extension(state): Extension<ReplicationState>,
    Arg(_former_replication_id): Arg<1>,
    Arg(offset_id): Arg<2>,
) -> Result<impl IntoResponse, RedisError> {
    if !matches!(state.role(), NodeRole::Master) {
        return Err(RedisError::NotMaster);
    }

    let offset: i64 = offset_id
        .parse()
        .map_err(|_| eyre!("Failed to parse offset id"))?;
    let offset: OffsetId = if offset == -1 { 0u64 } else { offset as u64 }.into();
    if offset != state.offset() {
        return Err(RedisError::Unhandled(eyre!(
            "Replication from non-zero offset is not supported"
        )));
    }

    Ok(Response::Upgrade {
        offset: offset.into(),
    })
}

pub async fn wait(
    Extension(wait_queue): Extension<ReplicationWaitQueue>,
    ArgParse(count): ArgParse<usize, 1>,
    ArgParse(timeout): ArgParse<u64, 2>,
) -> Result<impl IntoResponse, RedisError> {
    let (sync, receive) = oneshot::channel();
    let not_interested = Arc::new(Notify::new());

    let sleep_until = tokio::time::sleep_until(Instant::now() + Duration::from_millis(timeout));
    {
        let not_interested = not_interested.clone();
        tokio::task::spawn(async move {
            sleep_until.await;
            not_interested.notify_waiters();
        });
    }

    let _ = wait_queue.send((count, sync, not_interested)).await;

    return receive
        .await
        .map_err(|_| RedisError::Unhandled(eyre!("Receiver dropped")));
}
