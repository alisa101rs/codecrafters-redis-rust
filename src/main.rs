mod commands;
mod config;
mod encoding;
mod engine;
mod error;
mod network;
mod replication;
mod request;
mod response;
mod routing;
mod state;
mod storage;
mod util;
mod value;

use std::{net::SocketAddr, path::PathBuf, sync::Arc, time::Duration};

use bytes::{Buf, Bytes, BytesMut};
use clap::Parser;
use encoding::resp2;
use eyre::{bail, eyre, WrapErr};
use tokio::{
    io::AsyncReadExt,
    net::{TcpListener, TcpStream},
    sync::mpsc,
};
use tower::ServiceExt;

use crate::{
    config::Config,
    error::RedisError,
    network::{NetworkExt, NodeId, RedisNetwork},
    replication::{master::ReplicaConnectionQueue, ReplicationState, Topology},
    request::Extension,
    response::IntoResponse,
    routing::{Request, Response, Router},
    state::ConnectionState,
};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(long, default_value = "6379")]
    port: u16,

    #[arg(long, number_of_values = 2)]
    replicaof: Option<Vec<String>>,

    #[arg(long)]
    pub dir: Option<PathBuf>,

    #[arg(long)]
    pub dbfilename: Option<String>,
}

#[tokio::main]
async fn main() -> eyre::Result<()> {
    stable_eyre::install()?;
    logging();
    let Args {
        port,
        replicaof,
        dir,
        dbfilename,
    } = Args::parse();
    let config = Arc::new(Config { dir, dbfilename });

    let replicaof = match replicaof.as_deref() {
        Some([host, port]) => {
            use std::net::ToSocketAddrs;
            let port: u16 = port.parse().unwrap();

            Some(
                (host.as_ref(), port)
                    .to_socket_addrs()
                    .unwrap()
                    .next()
                    .unwrap(),
            )
        }
        None => None,
        _ => panic!("Wrong arguments"),
    };

    let listener = TcpListener::bind(("0.0.0.0", port)).await?;
    tracing::info!(addr = ?listener.local_addr().unwrap(), replica_of = ?replicaof, ?config, "Starting to listen on");

    match replicaof {
        None => {
            master(listener, config).await?;
        }
        Some(addr) => {
            replica(listener, port, addr, config).await?;
        }
    }

    Ok(())
}

async fn master(listener: TcpListener, config: Arc<Config>) -> eyre::Result<()> {
    let (storage, replication_queue) = engine::create_engine(&config)?;
    let state = ReplicationState::master();
    let topology = Topology::master();
    let (new_replicas, wait_queue) = replication::master::initiate(
        storage.clone(),
        topology.clone(),
        state.clone(),
        replication_queue,
    )?;

    let router = Router::new()
        .route("ping", commands::ping)
        .route("echo", commands::echo)
        .route("get", commands::get)
        .route("set", commands::set)
        .route("info", commands::info)
        .route("replconf", commands::repl::config)
        .route("psync", commands::repl::psync)
        .route("wait", commands::repl::wait)
        .route("config", commands::config)
        .route("keys", commands::keys)
        .route("type", commands::key_type)
        .route("xadd", commands::stream::xadd)
        .route("xrange", commands::stream::xrange)
        .route("xread", commands::stream::xread)
        .layer(Extension(config))
        .layer(Extension(wait_queue))
        .layer(Extension(state))
        .layer(Extension(topology))
        .layer(Extension(storage));

    serve_connections(listener, router, Some(new_replicas)).await
}

async fn replica(
    listener: TcpListener,
    port: u16,
    master: SocketAddr,
    config: Arc<Config>,
) -> eyre::Result<()> {
    let master = NodeId::master(master);
    let topology = Topology::replica(master);
    let mut network = RedisNetwork::new(Some(master)).await?;
    let state = handshake(master, port, &mut network).await?;
    let (storage, acks) = engine::create_engine(&config)?;

    tokio::spawn(replication::replica::start(
        network,
        master,
        storage.clone(),
        state.clone(),
        acks,
    ));

    let router = Router::new()
        .route("ping", commands::ping)
        .route("echo", commands::echo)
        .route("get", commands::get)
        .route("info", commands::info)
        .route("replconf", commands::repl::config)
        .route("config", commands::config)
        .route("keys", commands::keys)
        .route("type", commands::key_type)
        .route("xrange", commands::stream::xrange)
        .route("xread", commands::stream::xread)
        .layer(Extension(config))
        .layer(Extension(state))
        .layer(Extension(topology))
        .layer(Extension(storage));

    serve_connections(listener, router, None).await
}

async fn serve_connections(
    listener: TcpListener,
    router: Router,
    new_replicas: Option<ReplicaConnectionQueue>,
) -> eyre::Result<()> {
    loop {
        let (incoming, addr) = listener.accept().await?;
        let router = router.clone();
        let new_replicas = new_replicas.clone();
        tokio::spawn(async move {
            serve(addr, incoming, router, new_replicas.clone())
                .await
                .unwrap();
        });
    }
}

async fn handshake(
    master: NodeId,
    port: u16,
    network: &mut RedisNetwork,
) -> eyre::Result<ReplicationState> {
    {
        let mut attempt = 0;
        loop {
            let Err(_) = network.ping(&master).await else {
                break;
            };
            attempt += 1;

            if attempt > 10 {
                bail!("Failed to connect to master")
            }
            tokio::time::sleep(Duration::from_millis(200)).await;
        }
    }

    network
        .exec(
            &master,
            "REPLCONF",
            vec![
                Bytes::from_static(b"listening-port"),
                Bytes::from(port.to_string()),
            ],
        )
        .await?;

    network
        .exec(
            &master,
            "REPLCONF",
            vec![Bytes::from_static(b"capa"), Bytes::from_static(b"psync2")],
        )
        .await?;

    let (replication, offset) = network.psync(&master).await?;
    let replication_state = ReplicationState::replica();
    replication_state.set_id(replication);
    replication_state.set_offset(offset);

    Ok(replication_state)
}

async fn serve(
    addr: SocketAddr,
    mut connection: TcpStream,
    router: Router,
    new_replicas: Option<ReplicaConnectionQueue>,
) -> eyre::Result<()> {
    tracing::info!(addr = %addr, "Accepted new connection");
    let mut buf = BytesMut::new();
    let (mut read, mut write) = connection.split();
    let state = ConnectionState::new(addr);

    loop {
        let res = read
            .read_buf(&mut buf)
            .await
            .wrap_err("Failed to read input")?;

        if res == 0 && buf.is_empty() {
            break;
        }

        tracing::debug!(count = res, buffer = ?buf, "read bytes");

        let Ok((request, count)) = resp2::from_bytes::<Vec<String>>(buf.as_ref())
            .wrap_err("Failed to deserialize request")
        else {
            continue;
        };
        buf.advance(count);

        tracing::debug!(?request, "received a request");

        if request.len() == 0 {
            continue;
        }
        let request = Request::from_command_line(request, state.clone())?;
        let response = router.clone().oneshot(request).await.into_response();

        if let &Response::Upgrade { offset } = &response {
            return match new_replicas
                .unwrap()
                .send((connection, state.node_id().unwrap(), offset))
                .await
            {
                Ok(_) => Ok(()),
                Err(mpsc::error::SendError((mut connection, _, _))) => {
                    RedisError::Unhandled(eyre!("Can't add new replica"))
                        .into_response()
                        .write(&mut connection)
                        .await?;

                    Err(eyre!("Can't add new replica"))
                }
            };
        }

        response.write(&mut write).await?;
    }

    Ok(())
}

fn logging() {
    tracing_subscriber::fmt()
        .pretty()
        .with_thread_names(true)
        // enable everything
        .with_max_level(tracing::Level::TRACE)
        // sets this to be the default, global collector for this application.
        .init();
}
