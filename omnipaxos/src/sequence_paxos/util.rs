use crate::{
    storage::Entry,
    utils::{Ballot, NodeId},
};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::{
    cmp::Ordering,
    collections::{HashMap, HashSet},
};

type DataId = (NodeId, usize);
type SlotIdx = usize;

#[derive(Debug, Clone)]
struct Data<T: Entry> {
    pub(crate) data: Option<T>,
    pub(crate) status: DataStatus,
}

#[derive(Debug, Clone)]
enum DataStatus {
    Acked,
    ReplicateAcks(PossibleFastSlots),
    SlowPathWithSlot(SlotIdx),
    DecidedWithSlot(SlotIdx),
    Completed,
}

#[derive(Debug, Clone)]
struct ReplicatedData<T: Entry>(HashMap<DataId, Data<T>>);

impl<T: Entry> ReplicatedData<T> {
    fn get(&self, data_id: &DataId) -> Option<&Data<T>> {
        self.0.get(data_id)
    }

    fn with_capacity(capacity: usize) -> Self {
        ReplicatedData(HashMap::with_capacity(capacity))
    }

    fn complete_and_take_decided_data(&mut self, data_id: &DataId) -> Option<T> {
        match self.0.get_mut(data_id) {
            Some(Data { data, status }) => match status {
                DataStatus::DecidedWithSlot(slot) => {
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

    fn get_mut(&mut self, data_id: &DataId) -> Option<&mut Data<T>> {
        self.0.get_mut(data_id)
    }

    fn contains_key(&self, data_id: &DataId) -> bool {
        self.0.contains_key(data_id)
    }

    fn set_decided_slot(&mut self, data_id: &DataId, slot_idx: usize) {
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

    fn insert(&mut self, data_id: DataId, data: Data<T>) {
        self.0.insert(data_id, data);
    }

    /*
    // TODO need some GC?
    pub fn remove(&mut self, data_id: &DataId) -> Option<Data<T>> {
        self.0.remove(data_id)
    }
    */
}

#[derive(Debug, Clone, Default)]
pub(crate) struct Slots {
    pub slots: HashMap<SlotIdx, SlotStatus>,
}

impl Slots {
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

#[derive(Copy, Clone, Debug, Ord, PartialEq, Eq, Default, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Proposal {
    n: Ballot,
    version: usize,
    data_id: DataId,
}

impl PartialOrd for Proposal {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some((self.n, self.version).cmp(&(other.n, other.version)))
    }
}

#[derive(Debug, Clone)]
pub struct PossibleFastSlots(HashMap<SlotIdx, usize>);

impl PossibleFastSlots {
    pub fn new() -> Self {
        Self(HashMap::new())
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
        if total >= super_quorum {
            for (slot_idx, count) in &self.0 {
                if count == &quorum && num_slots <= 2 {
                    // one slot has a quorum of the same data, so it might still take the fast path
                    if let Some(SlotStatus::FastVotes(_)) = all_slots.get(&slot_idx) {
                        return false;
                    }
                }
            }
            true
        } else {
            false
        }
    }

    pub fn get_num_votes(&self) -> usize {
        self.0.values().sum()
    }

    pub fn testvote_would_be_fast(&self, super_quorum_size: usize) -> bool {
        if self.0.len() == 1 {
            let total: usize = self.0.values().sum();
            total == super_quorum_size
        } else {
            false
        }
    }
}

#[derive(Debug, Clone)]
pub struct Proposals(pub HashMap<NodeId, Proposal>);

impl Proposals {
    pub fn new() -> Self {
        Self([Proposal::default(); NUM_NODES])
    }

    pub fn initialize_with(p: Proposal, pid: NodeId) -> Self {
        let mut proposals = Self::new();
        proposals.add_proposal(p, pid);
        proposals
    }

    pub fn add_proposal(&mut self, p: Proposal, from: NodeId) {
        self.0[pid_to_idx(from)] = p;
    }

    pub fn check_result<T: Entry>(&self, quorum: usize, super_quorum: usize) -> ProposalResult {
        let mut num_votes = 0;
        let mut real_votes = HashSet::with_capacity(NUM_NODES);
        for p in &self.0 {
            if p != &Proposal::default() {
                num_votes += 1;
                real_votes.insert(*p);
            }
        }
        if num_votes < quorum {
            return ProposalResult::Pending;
        }
        let uniform = real_votes.len() == 1;
        if num_votes < quorum {
            return ProposalResult::Pending;
        } else if num_votes == quorum {
            if uniform {
                return ProposalResult::Pending;
            } else {
                return ProposalResult::SlowPath(real_votes);
            }
        } else if num_votes == super_quorum {
            if uniform {
                return ProposalResult::FastPath(*real_votes.iter().next().unwrap());
            } else {
                return ProposalResult::SlowPath(real_votes);
            }
        } else {
            return ProposalResult::SlowPath(real_votes);
        }
        // ProposalResult::Pending
    }

    pub fn get_recovery_result(&self) -> DataId {
        let mut max_proposal = self.0.first().unwrap();
        let mut max_proposal_count = self.0.iter().filter(|x| x == &max_proposal).count();
        for p in &self.0 {
            if (p.n, p.version) >= (max_proposal.n, max_proposal.version) {
                let count = self.0.iter().filter(|x| x == &p).count();
                if count > max_proposal_count {
                    max_proposal = p;
                    max_proposal_count = count;
                }
            }
        }
        max_proposal.data_id
    }

    pub fn get_num_voted(&self) -> usize {
        self.0.iter().filter(|p| p != &&Proposal::default()).count()
    }
}

#[derive(Debug, Clone)]
pub struct SlotEntries<T> {
    pub(crate) gaps: Vec<SlotIdx>,
    pub(crate) completed_entries: Vec<T>,
    pub(crate) completed_idx: SlotIdx,
}

#[derive(Clone, Debug, PartialEq)]
pub enum ProposalResult {
    Pending,
    FastPath(Proposal),
    SlowPath(HashSet<Proposal>),
}

pub fn pid_to_idx(pid: NodeId) -> usize {
    pid as usize - 1
}

pub fn idx_to_pid(idx: usize) -> NodeId {
    idx as NodeId + 1
}
