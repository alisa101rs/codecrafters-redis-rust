use bytes::Bytes;
use tokio::{select, sync::mpsc};
use tower::ServiceExt;
use tracing::instrument;

use crate::{
    engine::SharedEngine,
    flag,
    network::{Network, NetworkExt, NodeId, RedisNetwork},
    replication::{master::ReplicationCommand, ReplicationState},
    request::{Arg, Extension, Request},
    response::{IntoResponse, Resp2},
    routing::Router,
    state::ConnectionState,
};

#[instrument(skip(network, engine, acks), err)]
pub async fn start(
    mut network: RedisNetwork,
    master: NodeId,
    engine: SharedEngine,
    state: ReplicationState,
    mut acks: mpsc::Receiver<ReplicationCommand>,
) -> eyre::Result<()> {
    let _ = network.receive_rdb(&master).await?;
    tracing::info!("Received serialized rdb state");

    let router = Router::new()
        .route("set", set)
        .route("replconf", replconf)
        .route("ping", ping)
        .layer(Extension(state.clone()))
        .layer(Extension(engine.clone()));

    let connection = ConnectionState::new(master.addr());

    loop {
        select! {
            Ok((request, count)) = network.receive::<Vec<String>>(&master) => {
                tracing::debug!(?request, "Received command from master");

                let request = Request::from_command_line(request, connection.clone())?;
                let response = router.clone().oneshot(request).await.into_response();
                network.respond(&master, response).await?;
                state.increment_offset(count as u64);
            }
            Some(_) = acks.recv() => {

            }
        }
    }
}

async fn ping() {}

async fn set(Extension(storage): Extension<SharedEngine>, Arg(key): Arg<1>, Arg(value): Arg<2>) {
    let _ = storage.set(&key, value, None).await;
}

flag!(GetAck, "GETACK");

async fn replconf(
    Extension(state): Extension<ReplicationState>,
    GetAck(_k): GetAck,
) -> impl IntoResponse {
    let value = state.offset();

    Resp2(vec![
        Bytes::from_static(b"REPLCONF"),
        Bytes::from_static(b"ACK"),
        Bytes::from(value.to_string()),
    ])
}
