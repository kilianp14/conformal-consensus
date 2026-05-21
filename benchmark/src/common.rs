use omnipaxos::{macros::Entry, messages::Message as OmniPaxosMessage};
use serde::{Deserialize, Serialize};
use std::{
    net::{SocketAddr, ToSocketAddrs},
    thread,
    time::Duration,
};
use tokio::net::{
    TcpStream,
    tcp::{OwnedReadHalf, OwnedWriteHalf},
};
use tokio_serde::{Framed, formats::Bincode};
use tokio_util::codec::{Framed as CodecFramed, FramedRead, FramedWrite, LengthDelimitedCodec};

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
    Write(CommandId),
    Read(CommandId, Option<String>),
    StartSignal(Timestamp),
}

impl ServerMessage {
    pub fn command_id(&self) -> CommandId {
        match self {
            ServerMessage::Write(id) => *id,
            ServerMessage::Read(id, _) => *id,
            ServerMessage::StartSignal(_) => unimplemented!(),
        }
    }
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
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ClusterMessage,
    (),
    Bincode<ClusterMessage, ()>,
>;
pub type ToNodeConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ClusterMessage,
    Bincode<(), ClusterMessage>,
>;

pub fn frame_cluster_connection(stream: TcpStream) -> (FromNodeConnection, ToNodeConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromNodeConnection::new(stream, Bincode::default()),
        ToNodeConnection::new(sink, Bincode::default()),
    )
}

pub type FromServerConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ServerMessage,
    (),
    Bincode<ServerMessage, ()>,
>;

pub type ToServerConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ClientMessage,
    Bincode<(), ClientMessage>,
>;

pub type FromClientConnection = Framed<
    FramedRead<OwnedReadHalf, LengthDelimitedCodec>,
    ClientMessage,
    (),
    Bincode<ClientMessage, ()>,
>;

pub type ToClientConnection = Framed<
    FramedWrite<OwnedWriteHalf, LengthDelimitedCodec>,
    (),
    ServerMessage,
    Bincode<(), ServerMessage>,
>;

pub fn frame_clients_connection(stream: TcpStream) -> (FromServerConnection, ToServerConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromServerConnection::new(stream, Bincode::default()),
        ToServerConnection::new(sink, Bincode::default()),
    )
}

pub fn frame_servers_connection(stream: TcpStream) -> (FromClientConnection, ToClientConnection) {
    let (reader, writer) = stream.into_split();
    let stream = FramedRead::new(reader, LengthDelimitedCodec::new());
    let sink = FramedWrite::new(writer, LengthDelimitedCodec::new());
    (
        FromClientConnection::new(stream, Bincode::default()),
        ToClientConnection::new(sink, Bincode::default()),
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
