use omnipaxos::{macros::Entry, messages::Message as OmniPaxosMessage, utils::AcceptStatus};
use serde::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::Duration,
};
use tokio::net::{
    TcpStream, UnixStream,
    tcp::{OwnedReadHalf as TcpReadHalf, OwnedWriteHalf as TcpWriteHalf},
    unix::{OwnedReadHalf as UdsReadHalf, OwnedWriteHalf as UdsWriteHalf},
};
use tokio_serde::{Framed, formats::Bincode};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

pub const IPC_SOCKET_PATH: &str = "/tmp/omnipaxos-ipc.sock";

pub type CommandId = usize;
pub type ClientId = u64;
pub type NodeId = omnipaxos::utils::NodeId;
pub type Timestamp = i64;

#[derive(Debug, Clone, Entry, Serialize, Deserialize)]
pub struct Command {
    pub client_id: ClientId,
    pub coordinator_id: NodeId,
    pub id: CommandId,
    pub kv_cmd: KVCommand,
}

#[derive(Debug, Clone, Serialize, Deserialize)]
pub enum KVCommand {
    Put(String, String),
    Delete(String),
    Get(String),
}

impl PartialEq for Command {
    fn eq(&self, other: &Self) -> bool {
        self.id == other.id && self.coordinator_id == other.coordinator_id
    }
}
impl Eq for Command {}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum RegistrationMessage {
    NodeRegister(NodeId),
    ClientRegister,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ClusterMessage {
    OmniPaxosMessage(OmniPaxosMessage<Command>),
    LeaderStartSignal(Timestamp),
}

pub type ClientMessage = (CommandId, KVCommand);

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerMessage {
    Write(CommandId, AcceptStatus),
    Read(CommandId, Option<String>, AcceptStatus),
    StartSignal(Timestamp),
}

impl ServerMessage {
    pub fn command_id(&self) -> CommandId {
        match self {
            ServerMessage::Write(id, _) => *id,
            ServerMessage::Read(id, _, _) => *id,
            ServerMessage::StartSignal(_) => unimplemented!(),
        }
    }

    pub fn accept_status(&self) -> AcceptStatus {
        match self {
            ServerMessage::Write(_, accept_status) => *accept_status,
            ServerMessage::Read(_, _, accept_status) => *accept_status,
            ServerMessage::StartSignal(_) => unimplemented!(),
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum DaemonToServer {
    Cluster(NodeId, ClusterMessage),
    Client(ClientId, ClientMessage),
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum ServerToDaemon {
    Cluster(NodeId, ClusterMessage),
    Client(ClientId, ServerMessage),
}

pub type RegistrationConnection = Framed<
    CodecFramed<TcpStream, LengthDelimitedCodec>,
    RegistrationMessage,
    RegistrationMessage,
    Bincode<RegistrationMessage, RegistrationMessage>,
>;

pub fn frame_registration_connection(stream: TcpStream) -> RegistrationConnection {
    let length_delimited = CodecFramed::new(stream, LengthDelimitedCodec::new());
    Framed::new(length_delimited, Bincode::default())
}

pub type FromNodeConnection = Framed<
    FramedRead<TcpReadHalf, LengthDelimitedCodec>,
    ClusterMessage,
    (),
    Bincode<ClusterMessage, ()>,
>;
pub type ToNodeConnection = Framed<
    FramedWrite<TcpWriteHalf, LengthDelimitedCodec>,
    (),
    ClusterMessage,
    Bincode<(), ClusterMessage>,
>;

pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
    let (reader, writer) = stream.into_split();
    (
        FromNodeConnection::new(
            FramedRead::new(reader, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
        ToNodeConnection::new(
            FramedWrite::new(writer, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
    )
}

pub type FromServerConnection = Framed<
    FramedRead<TcpReadHalf, LengthDelimitedCodec>,
    ServerMessage,
    (),
    tokio_serde::formats::Bincode<ServerMessage, ()>,
>;

pub type ToServerConnection = Framed<
    FramedWrite<TcpWriteHalf, LengthDelimitedCodec>,
    (),
    ClientMessage,
    tokio_serde::formats::Bincode<(), ClientMessage>,
>;

pub type FromClientConnection = Framed<
    FramedRead<TcpReadHalf, LengthDelimitedCodec>,
    ClientMessage,
    (),
    tokio_serde::formats::Bincode<ClientMessage, ()>,
>;

pub type ToClientConnection = Framed<
    FramedWrite<TcpWriteHalf, LengthDelimitedCodec>,
    (),
    ServerMessage,
    tokio_serde::formats::Bincode<(), ServerMessage>,
>;

pub fn frame_clients_connection(stream: TcpStream) -> (FromServerConnection, ToServerConnection) {
    let (reader, writer) = stream.into_split();
    (
        FromServerConnection::new(
            FramedRead::new(reader, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
        ToServerConnection::new(
            FramedWrite::new(writer, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
    )
}

pub fn frame_servers_connection(stream: TcpStream) -> (FromClientConnection, ToClientConnection) {
    let (reader, writer) = stream.into_split();
    (
        FromClientConnection::new(
            FramedRead::new(reader, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
        ToClientConnection::new(
            FramedWrite::new(writer, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
    )
}

pub type IpcFromDaemon = Framed<
    FramedRead<UdsReadHalf, LengthDelimitedCodec>,
    DaemonToServer,
    (),
    Bincode<DaemonToServer, ()>,
>;
pub type IpcToServer = Framed<
    FramedWrite<UdsWriteHalf, LengthDelimitedCodec>,
    (),
    ServerToDaemon,
    Bincode<(), ServerToDaemon>,
>;

pub type IpcFromServer = Framed<
    FramedRead<UdsReadHalf, LengthDelimitedCodec>,
    ServerToDaemon,
    (),
    Bincode<ServerToDaemon, ()>,
>;
pub type IpcToDaemon = Framed<
    FramedWrite<UdsWriteHalf, LengthDelimitedCodec>,
    (),
    DaemonToServer,
    Bincode<(), DaemonToServer>,
>;

pub fn frame_ipc_server_side(stream: UnixStream) -> (IpcFromDaemon, IpcToServer) {
    let (reader, writer) = stream.into_split();
    (
        IpcFromDaemon::new(
            FramedRead::new(reader, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
        IpcToServer::new(
            FramedWrite::new(writer, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
    )
}

pub fn frame_ipc_daemon_side(stream: UnixStream) -> (IpcFromServer, IpcToDaemon) {
    let (reader, writer) = stream.into_split();
    (
        IpcFromServer::new(
            FramedRead::new(reader, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
        IpcToDaemon::new(
            FramedWrite::new(writer, LengthDelimitedCodec::new()),
            Bincode::default(),
        ),
    )
}

pub fn resolve_addr_with_retry(addr: &str, retries: usize) -> SocketAddr {
    for attempt in 0..retries {
        match addr.to_socket_addrs() {
            Ok(mut addrs) => {
                if let Some(a) = addrs.next() {
                    return a;
                }
            }
            Err(e) => {
                if attempt == retries - 1 {
                    panic!("Address {addr} is invalid after {retries} attempts: {e}");
                }
            }
        }
        thread::sleep(Duration::from_millis(200 * (attempt as u64 + 1)));
    }
    unreachable!()
}
