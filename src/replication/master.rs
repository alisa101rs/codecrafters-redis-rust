use std::{
    cmp::min,
    collections::HashMap,
    sync::Arc,
    time::{Duration, SystemTime},
};

use bytes::Bytes;
use tokio::{
    net::TcpStream,
    select,
    sync::{mpsc, oneshot, Notify},
    time::timeout,
};
use tracing::instrument;

use crate::{
    engine::SharedEngine,
    network::{Network, NetworkExt, NodeId, RedisNetwork},
    replication::{OffsetId, ReplicationState, SharedTopology},
};

pub type ReplicaConnectionQueue = mpsc::Sender<(TcpStream, NodeId, OffsetId)>;
pub type ReplicationCommandQueue = mpsc::Sender<ReplicationCommand>;
pub type ReplicationWaitQueue = mpsc::Sender<(usize, oneshot::Sender<usize>, Arc<Notify>)>;

pub fn initiate(
    engine: SharedEngine,
    topology: SharedTopology,
    state: ReplicationState,
    replications: mpsc::Receiver<ReplicationCommand>,
) -> eyre::Result<(ReplicaConnectionQueue, ReplicationWaitQueue)> {
    let (tx, clients) = mpsc::channel(4);
    let (txw, rxw) = mpsc::channel(1);

    tokio::spawn(replication_loop(
        state,
        replications,
        topology,
        clients,
        engine,
        rxw,
    ));

    Ok((tx, txw))
}

pub enum ReplicationCommand {
    Write {
        key: String,
        value: String,
        expiration: Option<SystemTime>,
    },
}

async fn replication_loop(
    state: ReplicationState,
    mut commands: mpsc::Receiver<ReplicationCommand>,
    topology: SharedTopology,
    mut clients: mpsc::Receiver<(TcpStream, NodeId, OffsetId)>,
    engine: SharedEngine,
    mut waits: mpsc::Receiver<(usize, oneshot::Sender<usize>, Arc<Notify>)>,
) -> eyre::Result<()> {
    let mut network = RedisNetwork::new(None).await?;
    let mut offsets = HashMap::new();

    loop {
        select! {
            Some((connection, node, offset)) = clients.recv() => {
                tracing::info!(?node, ?offset, "Adding new replication node");
                network.add_connection(&node, connection)?;
                topology.add(node).unwrap();
                offsets.insert(node, offset);
                let fullresync = format!("FULLRESYNC {} {}", state.id(), 0);
                network.send(&node, &fullresync).await?;
                network.send_rdb(&node, engine.dump()).await?;
            },
            Some(command) = commands.recv() => {
                tracing::trace!("Replication command received");

                match command {
                    ReplicationCommand::Write{ key, value, expiration } => {
                        assert!(expiration.is_none(), "Can't propagate writes with eol yet");
                        let size = network.broadcast(&vec![Bytes::from_static(b"SET"), Bytes::from(key), Bytes::from(value)]).await?;
                        state.increment_offset(size as u64);
                    }
                }
            },
            Some((count, notify, not_intersted)) = waits.recv() => {
                let target_offset = state.offset();
                collect_offsets(
                    state.clone(),
                    target_offset,
                    &mut offsets,
                    count,
                    &mut network,
                    notify,
                    not_intersted
                ).await;
            }
        }
    }
}

async fn collect_offsets(
    state: ReplicationState,
    target_offset: OffsetId,
    offsets: &mut HashMap<NodeId, OffsetId>,
    required_count: usize,
    network: &mut RedisNetwork,
    notify: oneshot::Sender<usize>,
    not_interested: Arc<Notify>,
) {
    let required_count = min(required_count, offsets.len());
    let mut current_count = offsets
        .iter()
        .filter_map(|(_, &o)| if o == target_offset { Some(()) } else { None })
        .count();

    if current_count >= required_count {
        let _ = notify.send(current_count);
        return;
    }

    loop {
        select! {
            Some(c) = ack_round(state.clone(), network, offsets, target_offset) => {
                current_count = c;
            },
            _ = not_interested.notified() => {
                break
            }
        }
        if current_count >= required_count {
            break;
        }
    }

    let _ = notify.send(current_count);
}

#[instrument(skip(network), ret)]
async fn ack_round(
    state: ReplicationState,
    network: &mut RedisNetwork,
    offsets: &mut HashMap<NodeId, OffsetId>,
    target: OffsetId,
) -> Option<usize> {
    let Ok(bytes_sent) = network
        .broadcast(&[
            Bytes::from_static(b"REPLCONF"),
            Bytes::from_static(b"GETACK"),
            Bytes::from_static(b"*"),
        ])
        .await
    else {
        return None;
    };

    state.increment_offset(bytes_sent as u64);

    for (node, offset) in offsets.iter_mut() {
        let Ok(Ok((response, _))) = timeout(
            Duration::from_millis(100),
            network.receive::<Vec<String>>(&node),
        )
        .await
        else {
            continue;
        };
        let Some(ack) = response.get(2) else { continue };
        let Ok(ack) = ack.parse::<u64>() else {
            continue;
        };
        *offset = (ack + bytes_sent as u64).into();

        tracing::info!(?node, %offset, "Received ack from replica");
    }

    Some(
        offsets
            .iter()
            .filter_map(|(_, &o)| if o >= target { Some(()) } else { None })
            .count(),
    )
}
