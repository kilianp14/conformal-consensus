use crate::{
    sequence_paxos::utils::{LogSync, SlotId},
    utils::{Ballot, Entry, EntryId, LogEntry, NodeId, SequenceNumber},
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
    pub decided_idx: SlotId,
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
    /// The decided index of this leader.
    pub decided_idx: SlotId,
    /// For log syncing
    pub log_sync: Option<LogSync<T>>,
}

/// AcceptSync message sent by the leader to add missing decided entries to the logs of all replicas in the prepare phase.
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
    /// The missing decided entries of the follower
    pub missing_decided: Vec<T>,
}

/// Message with entry to be replicated sent by the leader in accept phase of OmniPaxos or
/// in FastPaxos indicating a Slow path.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Accept<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// Entry to be replicated.
    pub entry: (EntryId, T),
    /// The index to place the entry.
    pub slot_idx: SlotId,
}

/// Message sent by follower to leader when entry has been accepted in OmniPaxos or in a slow round
/// in FastPaxos.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Accepted {
    /// The current round.
    pub n: Ballot,
    /// The index where the entry was placed.
    pub slot_idx: SlotId,
}

/// Message with entry proposed to be replicated in FastPaxos broadcasted to all nodes.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FpPropose<T>
where
    T: Entry,
{
    /// Entry to be replicated.
    pub entry: (EntryId, T),
    /// The index to place the entry.
    pub slot_idx: SlotId,
}

/// Message sent by a follower to leader in FastPaxos when entry has been accepted in fast round
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct FpFastAccepted<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// Entry to be replicated.
    pub entry: (EntryId, T),
    /// The index to place the entry.
    pub slot_idx: SlotId,
}

/// Message sent by leader to followers to decide up to a certain index in the log.
#[derive(Copy, Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Decide<T>
where
    T: Entry,
{
    /// The current round.
    pub n: Ballot,
    /// The sequence number of this message in the leader-to-follower accept sequence
    pub seq_num: SequenceNumber,
    /// Entry to be decided.
    pub entry: T,
    /// The index to place the decided entry.
    pub decided_idx: SlotId,
}

/// Message sent by follower to leader when accepting an entry is rejected.
/// This happens when the follower is promised to a greater leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct NotAccepted {
    /// The follower's current ballot
    pub n: Ballot,
}

/// An enum for all the different message types.
#[allow(missing_docs)]
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PaxosMsg<T>
where
    T: Entry,
{
    // Log reconciliation
    PrepareReq(PrepareReq),
    #[allow(missing_docs)]
    Prepare(Prepare),
    Promise(Promise<T>),
    AcceptSync(AcceptSync<T>),

    // OmniPaxos Messages
    OpProposalForward(EntryId, T),
    OpAccept(Accept<T>),
    OpAccepted(Accepted),

    // FastPaxos Messages
    FpPropose(FpPropose<T>),
    FpFastAccepted(FpFastAccepted<T>),
    FpSlowAccept(Accept<T>),
    FpSlowAccepted(Accepted),

    // Shared Messages
    NotAccepted(NotAccepted),
    Decide(Decide<T>),
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
