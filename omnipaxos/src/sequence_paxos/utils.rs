use crate::utils::{Ballot, Entry, NodeId};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::cmp::Ordering;

/// Promise without the log update
#[derive(Debug, Clone, Default)]
pub(crate) struct PromiseMetaData {
    pub n_accepted: Ballot,
    pub accepted_idx: usize,
    pub decided_idx: usize,
    pub pid: NodeId,
}

impl PartialOrd for PromiseMetaData {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        let ordering = if self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
        {
            Ordering::Equal
        } else if self.n_accepted > other.n_accepted
            || (self.n_accepted == other.n_accepted && self.accepted_idx > other.accepted_idx)
        {
            Ordering::Greater
        } else {
            Ordering::Less
        };
        Some(ordering)
    }
}

impl PartialEq for PromiseMetaData {
    fn eq(&self, other: &Self) -> bool {
        self.n_accepted == other.n_accepted
            && self.accepted_idx == other.accepted_idx
            && self.pid == other.pid
    }
}

pub type DataId = (NodeId, u64);

#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogData<T: Entry> {
    pub id: DataId,
    pub entry: T,
}

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The log suffix.
    pub suffix: Vec<LogData<T>>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: usize,
}

/*
pub(crate) type SlotIdx = usize;

#[derive(Copy, Clone, Debug, Ord, PartialEq, Eq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
struct Proposal {
    pub n: Ballot,
    pub version: usize,
    pub data_id: DataId,
}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.n, self.version).cmp(&(other.n, other.version)))
    }
}

#[derive(Clone, Debug, PartialEq)]
enum ProposalResult {
    // A majority has not voted yet
    NotEnoughVotes,
    // A quorum has voted, but vote is not uniform
    SlowPath(Proposal),
    // A quorum has voted uniformly, but a fast quorum has not been achieved yet
    // If fast quorum takes too long, a slow path could be initiated
    Pending,
    // A fast quorum has voted uniformly
    FastPath(Proposal),
}

#[derive(Debug, Clone)]
struct Proposals(HashMap<NodeId, Proposal>);

impl Proposals {
    pub(crate) fn new(num_nodes: usize) -> Self {
        Proposals(HashMap::with_capacity(num_nodes))
    }

    pub(crate) fn add_proposal(&mut self, p: Proposal, from: NodeId) {
        self.0.insert(from, p);
    }

    pub fn check_result<T: Entry>(&self, quorum: usize, super_quorum: usize) -> ProposalResult {
        let num_votes = self.0.len();

        if num_votes < quorum {
            return ProposalResult::NotEnoughVotes;
        }

        let mut counts: HashMap<Proposal, usize> = HashMap::new();
        for proposal in self.0.values() {
            *counts.entry(*proposal).or_insert(0) += 1;
        }

        // Find the proposal with the most votes
        // If there's a tie, the Ord implementation of Proposal acts as a tie-breaker
        let (most_common_proposal, &max_count) = counts
            .iter()
            .max_by(|(p1, count1), (p2, count2)| count1.cmp(count2).then_with(|| p1.cmp(p2)))
            .unwrap();

        // Non-uniform votes -> Slow path
        if max_count < num_votes {
            return ProposalResult::SlowPath(*most_common_proposal);
        }

        // Super-quorum -> Fast Path with the uniformly voted proposal
        if num_votes >= super_quorum {
            return ProposalResult::FastPath(*most_common_proposal);
        }

        // Super-quorum can still be reached
        ProposalResult::Pending
    }
}

#[derive(Debug, Clone)]
pub(crate) struct Data<T: Entry> {
    pub(crate) data: Option<T>,
    pub(crate) status: DataStatus,
}

#[derive(Debug, Clone)]
pub(crate) enum DataStatus {
    Acked,
    ReplicateAcks(PossibleFastSlots),
    SlowPathWithSlot(SlotIdx),
    DecidedWithSlot(SlotIdx),
    Completed,
}

#[derive(Debug, Clone)]
pub struct ReplicatedData<T: Entry>(HashMap<DataId, Data<T>>);

impl<T: Entry> ReplicatedData<T> {
    pub fn get(&self, data_id: &DataId) -> Option<&Data<T>> {
        self.0.get(data_id)
    }

    pub fn with_capacity(capacity: usize) -> Self {
        ReplicatedData(HashMap::with_capacity(capacity))
    }

    pub fn complete_and_take_decided_data(&mut self, data_id: &DataId) -> Option<T> {
        match self.0.get_mut(data_id) {
            Some(Data { data, status }) => match status {
                DataStatus::DecidedWithSlot(_) => {
                    let d = std::mem::take(data).expect("Data not found");
                    *status = DataStatus::Completed;
                    return Some(d);
                }
                _ => {}
            },
            _ => {}
        }
        None
    }

    pub fn get_mut(&mut self, data_id: &DataId) -> Option<&mut Data<T>> {
        self.0.get_mut(data_id)
    }

    pub fn contains_key(&self, data_id: &DataId) -> bool {
        self.0.contains_key(data_id)
    }

    pub fn set_decided_slot(&mut self, data_id: &DataId, slot_idx: usize) {
        let status = DataStatus::DecidedWithSlot(slot_idx);
        match self.0.get_mut(data_id) {
            Some(x) => {
                x.status = status;
            }
            None => {
                self.0.insert(*data_id, Data { data: None, status });
            }
        }
    }

    pub fn insert(&mut self, data_id: DataId, data: Data<T>) {
        self.0.insert(data_id, data);
    }
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Slots {
    pub slots: HashMap<SlotIdx, SlotStatus>,
}

impl Slots {
    pub fn new() -> Slots {
        Slots {
            slots: HashMap::with_capacity(100000),
        }
    }
    pub fn clear(&mut self) {
        self.slots.clear();
    }

    pub fn insert(&mut self, idx: SlotIdx, status: SlotStatus) {
        self.slots.insert(idx, status);
    }

    pub fn get(&self, idx: &SlotIdx) -> Option<&SlotStatus> {
        self.slots.get(idx)
    }

    pub fn remove(&mut self, idx: &SlotIdx) -> Option<SlotStatus> {
        self.slots.remove(idx)
    }

    pub fn get_mut(&mut self, idx: &SlotIdx) -> Option<&mut SlotStatus> {
        self.slots.get_mut(idx)
    }

    pub fn get_max_decided_slot(&self) -> SlotIdx {
        self.slots
            .iter()
            .filter_map(|(slot_idx, x)| match x {
                SlotStatus::Decided(_) => Some(*slot_idx),
                _ => None,
            })
            .max()
            .unwrap_or(0)
    }

    pub fn get_pending_slots(s: Self) -> Vec<PendingSlot> {
        s.slots
            .iter()
            .filter_map(|(idx, s)| match s {
                SlotStatus::SlowAcks(p, _) => Some(PendingSlot {
                    idx: *idx,
                    proposal: *p,
                    decided: false,
                }),
                SlotStatus::FastVotes(ps) => {
                    let p = ps.0.iter().max().unwrap();
                    Some(PendingSlot {
                        idx: *idx,
                        proposal: *p,
                        decided: false,
                    })
                }
                SlotStatus::Voted(p) => Some(PendingSlot {
                    idx: *idx,
                    proposal: *p,
                    decided: false,
                }),
                SlotStatus::Decided(data_id) => {
                    let p = Proposal {
                        data_id: *data_id,
                        n: Ballot::default(),
                        version: 0,
                    };
                    Some(PendingSlot {
                        idx: *idx,
                        proposal: p,
                        decided: true,
                    })
                }
                SlotStatus::Completed(_) => {
                    unimplemented!("Completed slots should be appended to the log")
                }
                SlotStatus::Recovery(_) => {
                    unimplemented!("Recovery should only be used locally during prepare phase")
                }
            })
            .collect()
    }
}

#[derive(Debug, Clone)]
pub enum SlotStatus {
    Voted(Proposal),
    FastVotes(Proposals),
    SlowAcks(Proposal, usize), // slow path
    Decided(DataId),
    Completed(DataId),
    Recovery(Proposals),
}

#[derive(Copy, Clone, Debug, Ord, Eq, PartialOrd, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PendingSlot {
    pub idx: SlotIdx,
    pub proposal: Proposal,
    pub decided: bool,
}

#[derive(Debug, Clone)]
pub struct PossibleFastSlots(HashMap<SlotIdx, usize>);

impl PossibleFastSlots {
    pub fn new() -> Self {
        Self(HashMap::with_capacity(100))
    }

    pub fn add_slot(&mut self, idx: SlotIdx) {
        match self.0.get_mut(&idx) {
            Some(count) => *count += 1,
            None => {
                self.0.insert(idx, 1);
            }
        }
    }

    pub fn eligible_for_slowpath(
        &self,
        quorum: usize,
        super_quorum: usize,
        all_slots: &Slots,
    ) -> bool {
        let total: usize = self.0.values().sum();
        let num_slots = self.0.len();
        if total < super_quorum {
            return false;
        }
        if num_slots <= 2 {
            for (slot_idx, count) in &self.0 {
                if count == &quorum {
                    // one slot has a quorum of the same data, so it might still take the fast path
                    if let Some(SlotStatus::FastVotes(_)) = all_slots.get(&slot_idx) {
                        return false;
                    }
                }
            }
        }
        true
    }

    pub fn get_num_votes(&self) -> usize {
        self.0.values().sum()
    }
}

#[derive(Debug, Clone)]
pub struct SlotEntries<T> {
    pub(crate) gaps: Vec<SlotIdx>,
    pub(crate) completed_entries: Vec<T>,
    pub(crate) completed_idx: SlotIdx,
}
*/
