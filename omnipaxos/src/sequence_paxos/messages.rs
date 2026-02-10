use crate::{
    storage::{Entry, StopSign},
    utils::{Ballot, LogSync, NodeId, SequenceNumber},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::fmt::Debug;

/// Message sent by a follower on crash-recovery or dropped messages to request its leader to re-prepare them.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PrepareReq {
    /// The current round.
    pub n: Ballot,
}

/// Prepare message sent by a newly-elected leader to initiate the Prepare phase.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Prepare {
    /// The current round.
    pub n: Ballot,
    /// The decided index of this leader.
    pub decided_idx: usize,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The log length of this leader.
    pub accepted_idx: usize,
}

/// Promise message sent by a follower in response to a [`Prepare`] sent by the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Promise<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// The latest round in which an entry was accepted.
    pub n_accepted: Ballot,
    /// The decided index of this follower.
    pub decided_idx: usize,
    /// The log length of this follower.
    pub accepted_idx: usize,
    /// The log update which the leader applies to its log in order to sync
    /// with this follower (if the follower is more up-to-date).
    pub log_sync: Option<LogSync<T>>,
}

/// AcceptSync message sent by the leader to synchronize the logs of all replicas in the prepare phase.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AcceptSync<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// The decided index
    pub decided_idx: usize,
    /// The log update which the follower applies to its log in order to sync
    /// with the leader.
    pub log_sync: LogSync<T>,
    #[cfg(feature = "unicache")]
    /// The UniCache of the leader
    pub unicache: T::UniCache,
}

/// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SlowAccept<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// The decided index.
    pub decided_idx: usize,
    #[cfg(not(feature = "unicache"))]
    /// Entries to be replicated.
    pub entries: Vec<T>,
    #[cfg(feature = "unicache")]
    /// Entries to be replicated.
    pub entries: Vec<T::EncodeResult>,
}

/// Message with entries to be replicated and the latest decided index sent by the leader in the accept phase.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FastAccept<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    #[cfg(not(feature = "unicache"))]
    /// Entry to be replicated.
    pub entry: T,
    #[cfg(feature = "unicache")]
    /// Entries to be replicated.
    pub entry: T::EncodeResult,
}

/// Message sent by follower to leader when entries has been accepted.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Accepted {
    /// The current round.
    pub n: Ballot,
    /// The accepted index.
    pub accepted_idx: usize,
}

/// Message sent by leader to followers to decide up to a certain index in the log.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Decide {
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// The decided index.
    pub decided_idx: usize,
}

/// Message sent by leader to followers to accept a StopSign
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AcceptStopSign {
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// The decided index.
    pub ss: StopSign,
}

/// Message sent by follower to leader when accepting an entry is rejected.
/// This happens when the follower is promised to a greater leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NotAccepted {
    /// The follower's current ballot
    pub n: Ballot,
}

/// Compaction Request
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Compaction {
    Trim(usize),
    Snapshot(Option<usize>),
}

/// An enum for all the different message types.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PaxosMsg<T>
where
    T: Entry,
{
    /// Request a [`Prepare`] to be sent from the leader. Used for fail-recovery.
    PrepareReq(PrepareReq),
    #[allow(missing_docs)]
    Prepare(Prepare),
    Promise(Promise<T>),
    AcceptSync(AcceptSync<T>),
    SlowAccept(SlowAccept<T>),
    FastAccept(FastAccept<T>),
    Accepted(Accepted),
    NotAccepted(NotAccepted),
    Decide(Decide),
    /// Forward client proposals to the leader.
    ProposalForward(Vec<T>),
    Compaction(Compaction),
    AcceptStopSign(AcceptStopSign),
    ForwardStopSign(StopSign),
}

/// A struct for a Paxos message that also includes sender and receiver.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PaxosMessage<T>
where
    T: Entry,
{
    /// Sender of `msg`.
    pub from: NodeId,
    /// Receiver of `msg`.
    pub to: NodeId,
    /// The message content.
    pub msg: PaxosMsg<T>,
}
