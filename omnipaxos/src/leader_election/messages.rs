use crate::utils::{Ballot, NodeId};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

/// An enum for all the different BLE message types.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum HeartbeatMsg {
    Request(HeartbeatRequest),
    Reply(HeartbeatReply),
}

/// Requests a reply from all the other servers.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HeartbeatRequest {
    /// Number of the current round.
    pub round: u32,
}

/// Replies
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct HeartbeatReply {
    /// Number of the current heartbeat round.
    pub round: u32,
    /// Ballot of replying server.
    pub ballot: Ballot,
    /// Leader this server is following
    pub leader: Ballot,
    /// Whether the replying server sees a need for a new leader
    pub happy: bool,
}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct BLEMessage {
    /// Sender of `msg`.
    pub from: NodeId,
    /// Receiver of `msg`.
    pub to: NodeId,
    /// The message content.
    pub msg: HeartbeatMsg,
}
