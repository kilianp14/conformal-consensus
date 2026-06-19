use benchmark::common::{
    ClientId, ClusterMessage, DaemonToServer, IPC_SOCKET_PATH, NodeId, RegistrationMessage,
    ServerMessage, ServerToDaemon, frame_cluster_connection, frame_ipc_daemon_side,
    frame_registration_connection, frame_servers_connection, resolve_addr_with_retry,
};
use config::{Config, File};
use futures::{SinkExt, StreamExt};
use log::{error, info, warn};
use serde::{Deserialize, Serialize};
use std::{
    collections::HashMap,
    env,
    net::{IpAddr, Ipv4Addr, SocketAddr},
    str::FromStr,
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::{
    net::{TcpListener, TcpStream, UnixListener},
    sync::mpsc::{self, UnboundedSender},
};

#[derive(Debug, Serialize, Deserialize, Clone)]
pub struct ClusterConfig {
    pub node_addrs: Vec<(NodeId, String)>,
}

#[tokio::main]
pub async fn main() {
    env_logger::init();
    info!("Starting OmniPaxos Network Daemon...");

    let cluster_config_file = match env::var("CLUSTER_CONFIG_FILE") {
        Ok(file_path) => file_path,
        Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
    };
    let node_id: NodeId = match env::var("NODE_ID") {
        Ok(id) => u64::from_str(&id).expect("Invalid node id"),
        Err(_) => panic!("Requires CLUSTER_CONFIG_FILE environment variable to be set"),
    };
    let config: ClusterConfig = Config::builder()
        .add_source(File::with_name(&cluster_config_file))
        .build()
        .map_err(|e| panic!("Failed to build config source: {e}"))
        .and_then(|c| c.try_deserialize())
        .unwrap_or_else(|e| panic!("Failed to build config source: {e}"));

    run_daemon(config, node_id).await;
}

#[derive(Default)]
struct DaemonState {
    server_tx: Option<UnboundedSender<DaemonToServer>>,
    peer_txs: HashMap<NodeId, UnboundedSender<ClusterMessage>>,
    client_txs: HashMap<ClientId, UnboundedSender<ServerMessage>>,
    next_client_id: ClientId,
}

pub async fn run_daemon(config: ClusterConfig, node_id: NodeId) {
    let state = Arc::new(Mutex::new(DaemonState::default()));

    let listen_port = config
        .node_addrs
        .iter()
        .find(|(id, _)| *id == node_id)
        .expect("Own address not found")
        .1
        .split(':')
        .next_back()
        .expect("Address must contain a port")
        .parse::<u16>()
        .expect("Invalid port number");

    let listen_address = SocketAddr::new(IpAddr::V4(Ipv4Addr::UNSPECIFIED), listen_port);

    let peer_addresses: Vec<(NodeId, SocketAddr)> = config
        .node_addrs
        .iter()
        .map(|(id, addr)| (*id, resolve_addr_with_retry(addr, 15)))
        .filter(|(id, _)| *id != node_id)
        .collect();

    // Spawn IPC (Unix Socket) Listener for the local Server container
    let state_clone = state.clone();
    tokio::spawn(async move {
        // Clean up old socket if it exists
        let _ = std::fs::remove_file(IPC_SOCKET_PATH);
        let listener = UnixListener::bind(IPC_SOCKET_PATH).expect("Failed to bind to IPC socket");
        info!("Listening for local server on UDS: {}", IPC_SOCKET_PATH);

        loop {
            match listener.accept().await {
                Ok((stream, _)) => {
                    info!("Local server connected via IPC.");
                    handle_ipc_connection(stream, state_clone.clone());
                }
                Err(e) => error!("IPC accept error: {}", e),
            }
        }
    });

    // Spawn TCP Listener for external Peers & Clients
    let state_clone = state.clone();
    tokio::spawn(async move {
        let listener = TcpListener::bind(listen_address).await.unwrap();
        info!("Listening for peers and clients on TCP: {}", listen_address);

        loop {
            match listener.accept().await {
                Ok((tcp_stream, socket_addr)) => {
                    info!("New TCP connection from {socket_addr}");
                    tcp_stream.set_nodelay(true).unwrap();
                    handle_incoming_tcp(tcp_stream, state_clone.clone());
                }
                Err(e) => error!("TCP accept error: {:?}", e),
            }
        }
    });

    // Connect to out-going Peers
    let peers_to_connect_to = peer_addresses
        .into_iter()
        .filter(|(peer_id, _)| *peer_id < node_id);
    for (peer_id, peer_address) in peers_to_connect_to {
        let state_clone = state.clone();
        tokio::spawn(async move {
            let mut reconnect_interval = tokio::time::interval(Duration::from_secs(1));
            loop {
                reconnect_interval.tick().await;
                match tokio::net::TcpStream::connect(peer_address).await {
                    Ok(connection) => {
                        info!("Connected out to peer node {}", peer_id);
                        connection.set_nodelay(true).unwrap();

                        let mut registration = frame_registration_connection(connection);
                        if registration
                            .send(RegistrationMessage::NodeRegister(node_id))
                            .await
                            .is_ok()
                        {
                            let stream = registration.into_inner().into_inner();
                            spawn_peer_tasks(peer_id, stream, state_clone.clone());
                            break; // Done connecting, exit the retry loop
                        }
                    }
                    Err(err) => {
                        warn!(
                            "Failed to connect to peer node {peer_id} at {peer_address}: {err}. Retrying..."
                        );
                    }
                }
            }
        });
    }

    // Keep daemon alive indefinitely
    std::future::pending::<()>().await;
}

// Connection Handlers
fn handle_ipc_connection(stream: tokio::net::UnixStream, state: Arc<Mutex<DaemonState>>) {
    let (mut ipc_rx, mut ipc_tx) = frame_ipc_daemon_side(stream);

    // Create writer channel for the Server
    let (server_tx, mut server_rx) = mpsc::unbounded_channel::<DaemonToServer>();
    {
        let mut st = state.lock().unwrap();
        st.server_tx = Some(server_tx);
    }

    // Reader Task (Server -> Daemon -> Out to Network)
    let state_reader = state.clone();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = ipc_rx.next().await {
            let st = state_reader.lock().unwrap();
            match msg {
                ServerToDaemon::Cluster(node_id, cluster_msg) => {
                    if let Some(tx) = st.peer_txs.get(&node_id) {
                        let _ = tx.send(cluster_msg);
                    }
                }
                ServerToDaemon::Client(client_id, server_msg) => {
                    if let Some(tx) = st.client_txs.get(&client_id) {
                        let _ = tx.send(server_msg);
                    }
                }
            }
        }
        info!("Local server disconnected.");
        let mut st = state_reader.lock().unwrap();
        st.server_tx = None;
    });

    // Writer Task (Network -> Daemon -> In to Server)
    tokio::spawn(async move {
        while let Some(msg) = server_rx.recv().await {
            if ipc_tx.send(msg).await.is_err() {
                break;
            }
        }
    });
}

fn handle_incoming_tcp(connection: TcpStream, state: Arc<Mutex<DaemonState>>) {
    tokio::spawn(async move {
        let mut registration = frame_registration_connection(connection);

        match registration.next().await {
            Some(Ok(RegistrationMessage::NodeRegister(node_id))) => {
                info!("Registered incoming peer: {}", node_id);
                let stream = registration.into_inner().into_inner();
                spawn_peer_tasks(node_id, stream, state);
            }
            Some(Ok(RegistrationMessage::ClientRegister)) => {
                let stream = registration.into_inner().into_inner();
                spawn_client_tasks(stream, state);
            }
            _ => warn!("Invalid registration handshake."),
        }
    });
}

fn spawn_peer_tasks(peer_id: NodeId, stream: TcpStream, state: Arc<Mutex<DaemonState>>) {
    let (mut tcp_rx, mut tcp_tx) = frame_cluster_connection(stream);
    let (peer_tx, mut peer_rx) = mpsc::unbounded_channel::<ClusterMessage>();

    state.lock().unwrap().peer_txs.insert(peer_id, peer_tx);

    // Reader Task (Peer -> Daemon -> Server)
    let state_reader = state.clone();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = tcp_rx.next().await {
            let st = state_reader.lock().unwrap();
            if let Some(tx) = &st.server_tx {
                let _ = tx.send(DaemonToServer::Cluster(peer_id, msg));
            }
        }
        info!("Peer {} disconnected.", peer_id);
        state_reader.lock().unwrap().peer_txs.remove(&peer_id);
    });

    // Writer Task (Server -> Daemon -> Peer)
    tokio::spawn(async move {
        while let Some(msg) = peer_rx.recv().await {
            if tcp_tx.send(msg).await.is_err() {
                break;
            }
        }
    });
}

fn spawn_client_tasks(stream: TcpStream, state: Arc<Mutex<DaemonState>>) {
    let client_id = {
        let mut st = state.lock().unwrap();
        st.next_client_id += 1;
        st.next_client_id
    };

    info!("Registered incoming client: {}", client_id);

    let (mut tcp_rx, mut tcp_tx) = frame_servers_connection(stream);
    let (client_tx, mut client_rx) = mpsc::unbounded_channel::<ServerMessage>();

    state
        .lock()
        .unwrap()
        .client_txs
        .insert(client_id, client_tx);

    // Reader Task (Client -> Daemon -> Server)
    let state_reader = state.clone();
    tokio::spawn(async move {
        while let Some(Ok(msg)) = tcp_rx.next().await {
            let st = state_reader.lock().unwrap();
            if let Some(tx) = &st.server_tx {
                let _ = tx.send(DaemonToServer::Client(client_id, msg));
            }
        }
        info!("Client {} disconnected.", client_id);
        state_reader.lock().unwrap().client_txs.remove(&client_id);
    });

    // Writer Task (Server -> Daemon -> Client)
    tokio::spawn(async move {
        while let Some(msg) = client_rx.recv().await {
            if tcp_tx.send(msg).await.is_err() {
                break;
            }
        }
    });
}
