use crate::utils::{Ballot, Entry, EntryId, LogEntry, NodeId, SequenceNumber, SlotStatus};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::{BTreeMap, HashMap};

pub type SlotId = usize;

/// Struct used to help another server synchronize their log with the current state of our own log.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct LogSync<T>
where
    T: Entry,
{
    /// The log suffix.
    pub suffix: Vec<LogEntry<T>>,
    /// The index of the log where the entries from `suffix` should be applied at (also the compacted idx of `decided_snapshot` if it exists).
    pub sync_idx: SlotId,
}

#[derive(Default, Debug, Clone)]
/// The promise state of a node.
pub(crate) enum PromiseState {
    /// Not promised to any leader
    #[default]
    NotPromised,
    /// Promised to my ballot with accepted round and decided_idx
    Promised(Ballot, SlotId),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Clone, Debug)]
enum ProposalResult<T>
where
    T: Entry,
{
    // A quorum has not voted yet
    // or quorum has voted uniformly with fast accepts, but a fast quorum has not been achieved yet
    Pending,
    // A quorum has voted, but vote is not uniform
    SlowPath(EntryId, T),
    // A quorum has voted uniformly using OmniPaxos accept
    // or a fast quorum has voted uniformly
    Decided(T),
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub(crate) n_leader: Ballot,
    // Promises from followers
    promises_meta: HashMap<NodeId, PromiseState>,
    // Log syncs from followers
    log_syncs: HashMap<NodeId, LogSync<T>>,
    // The sequence number of accepts for each follower
    follower_seq_nums: HashMap<NodeId, SequenceNumber>,
    // Mapping between entry ids and entries
    entries: HashMap<EntryId, T>,
    // Stores proposals by slot and node
    accept_meta: HashMap<SlotId, HashMap<NodeId, (EntryId, SlotStatus)>>,
    // Majority quorum size
    pub(crate) quorum_size: usize,
    // Fast quorum size
    pub(crate) super_quorum_size: usize,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub(crate) fn with(
        n_leader: Ballot,
        n_nodes: usize,
        quorum_size: usize,
        super_quorum_size: usize,
    ) -> Self {
        Self {
            n_leader,
            promises_meta: HashMap::with_capacity(n_nodes),
            log_syncs: HashMap::with_capacity(n_nodes),
            accept_meta: HashMap::new(),
            entries: HashMap::new(),
            follower_seq_nums: HashMap::with_capacity(n_nodes),
            quorum_size,
            super_quorum_size,
        }
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub(crate) fn increment_seq_num_session(&mut self, pid: NodeId) {
        let seq = self.follower_seq_nums.entry(pid).or_default();
        seq.session += 1;
        seq.counter = 0;
    }

    pub(crate) fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let seq = self.follower_seq_nums.entry(pid).or_default();
        seq.counter += 1;
        *seq
    }

    pub(crate) fn get_promised_count(&self) -> usize {
        self.promises_meta
            .values()
            .filter(|m| matches!(m, PromiseState::Promised(_, _)))
            .count()
    }

    pub(crate) fn set_promise(
        &mut self,
        from: NodeId,
        accepted_round: Ballot,
        decided_idx: SlotId,
        log_sync: LogSync<T>,
    ) -> bool {
        self.promises_meta
            .insert(from, PromiseState::Promised(accepted_round, decided_idx));
        // all of the followers log-syncs start directly after the leaders decided_idx
        self.log_syncs.insert(from, log_sync);
        let num_promised = self.get_promised_count();
        num_promised >= self.quorum_size
    }

    pub(crate) fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta.insert(pid, PromiseState::NotPromised);
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub(crate) fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta.insert(pid, PromiseState::PromisedHigher);
    }

    /// Returns the log sync to be applied after the leader's decided_idx, and the new decided_idx
    /// Assumes that a majority has promised to the leader
    /// and that all log syncs sent by the followers start directly from said decided_idx
    /// slot status marks what to put in the status of the accepted but not yet decided values
    pub(crate) fn take_my_log_sync(
        &mut self,
        decided_idx: SlotId,
        status: SlotStatus,
    ) -> (LogSync<T>, SlotId) {
        // Only log syncs of nodes that have the maximum accepted round have to be considered
        let max_accepted_ballot = self
            .promises_meta
            .values()
            .filter_map(|state| {
                if let PromiseState::Promised(ballot, _) = state {
                    Some(ballot)
                } else {
                    None
                }
            })
            .max()
            .expect("No promised follower. Cannot take log sync");
        let nodes_with_max_accepted_ballot: Vec<NodeId> = self
            .promises_meta
            .iter()
            .filter_map(|(id, state)| {
                if let PromiseState::Promised(ballot, _) = state {
                    if ballot == max_accepted_ballot {
                        return Some(*id);
                    }
                }
                None
            })
            .collect();
        // Suffixes of nodes with maximum accepted ballot
        let mut relevant_log_syncs: Vec<Vec<LogEntry<T>>> = nodes_with_max_accepted_ballot
            .iter()
            .map(|id| {
                let LogSync { sync_idx, suffix } = self
                    .log_syncs
                    .remove(id)
                    .expect("Promised node without a log sync");
                if sync_idx != decided_idx {
                    panic!("Follower log sync does not start at the correct index");
                }
                suffix
            })
            .collect();

        let mut final_suffix = Vec::new();
        let mut current_decided_idx = decided_idx;

        // Reverse suffixes to use more efficient pop()
        for suffix in &mut relevant_log_syncs {
            suffix.reverse();
        }

        loop {
            let mut entries_at_slot = Vec::new();
            for suffix in &mut relevant_log_syncs {
                if let Some(entry) = suffix.pop() {
                    entries_at_slot.push(entry);
                }
            }

            // If no nodes have an entry for this slot, we are done
            if entries_at_slot.is_empty() {
                break;
            }

            let mut resolved_entry = Self::find_correct_log_sync_value_for_slot(entries_at_slot);

            match &mut resolved_entry {
                LogEntry::Decided(_) => {
                    current_decided_idx += 1;
                }
                LogEntry::Undecided(_, _, entry_status) => {
                    // Replace Undecided status with the target
                    *entry_status = status.clone();
                }
                LogEntry::Empty => {}
            }
            final_suffix.push(resolved_entry);
        }
        (
            LogSync {
                sync_idx: decided_idx,
                suffix: final_suffix,
            },
            current_decided_idx,
        )
    }

    fn find_correct_log_sync_value_for_slot(mut values: Vec<LogEntry<T>>) -> LogEntry<T> {
        // 1. If there is a decided entry, return it
        if let Some(pos) = values
            .iter()
            .position(|e| matches!(e, LogEntry::Decided(_)))
        {
            return values.remove(pos);
        }

        // 2. Count frequencies of Undecided entries
        let mut counts = HashMap::new();
        for entry in &values {
            if let LogEntry::Undecided(id, _, _) = entry {
                *counts.entry(*id).or_insert(0) += 1;
            }
        }

        // 3. Find the EntryId with the highest count
        // max_by_key will pick one arbitrarily if there's a tie.
        if let Some((&max_id, _)) = counts.iter().max_by_key(|&(_, count)| count) {
            let pos = values
                .iter()
                .position(|e| matches!(e, LogEntry::Undecided(id, _, _) if id == &max_id))
                .expect("Winning ID must exist in the original list");

            return values.remove(pos);
        }

        // 4. If no Decided or Undecided entries exist, it's Empty
        LogEntry::Empty
    }

    pub(crate) fn add_proposal(
        &mut self,
        decided_idx: SlotId,
        pid: NodeId,
        slot_idx: SlotId,
        entry: (EntryId, T),
        slot_status: SlotStatus,
    ) {
        if decided_idx < slot_idx {
            self.entries.insert(entry.0, entry.1);
            self.accept_meta
                .entry(slot_idx)
                .or_insert_with(HashMap::new)
                .insert(pid, (entry.0, slot_status));

            let proposal_result = self.compute_propose_result(slot_idx);
            self.proposal_results.insert(key, value)
        }
    }

    fn compute_propose_result(&self, slot_idx: SlotId) -> ProposalResult<T> {
        let proposals = match self.accept_meta.get(&slot_idx) {
            Some(p) => p,
            None => return ProposalResult::NotEnoughVotes,
        };
        let total_votes_in_slot = proposals.len();
        if total_votes_in_slot < self.quorum_size {
            return ProposalResult::NotEnoughVotes;
        }

        let mut vote_counts: HashMap<EntryId, (usize, usize, usize, usize)> = HashMap::new();
        for (id, status) in proposals.values() {
            let (op, fast, slow, total) = vote_counts.entry(*id).or_insert((0, 0, 0, 0));
            *total += 1;
            match status {
                SlotStatus::OpAccepted => *op += 1,
                SlotStatus::FpFastAccepted => *fast += 1,
                SlotStatus::FpSlowAccepted => *slow += 1,
            }
        }
        // Identify the entry with the most votes to check for quorum/decisions
        let winner = vote_counts
            .iter()
            .max_by_key(|(_, (_, _, _, total))| *total)
            .map(|(id, counts)| (*id, *counts));

        if let Some((entry_id, (op, _fast, slow, total_for_entry))) = winner {
            let entry = self
                .entries
                .get(&entry_id)
                .expect("Entry must exist at this point")
                .clone();
            if total_for_entry >= self.super_quorum_size || op + slow >= self.quorum_size {
                return ProposalResult::Decided(entry);
            }
            // Collision happened right now
            if total_for_entry != total_votes_in_slot {
                return ProposalResult::SlowPath(entry_id, entry);
            }
            return ProposalResult::Pending;
        } else {
            panic!("Votes but no winner should not be possible");
        }
    }

    pub(crate) fn clean_proposals(&mut self, slot_idx: SlotId) {
        self.accept_meta.remove(&slot_idx);
    }

    pub(crate) fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(&pid) {
            Some(PromiseState::Promised(metadata)) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub(crate) fn get_promised_followers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .filter_map(|(&id, promise_state)| match promise_state {
                PromiseState::Promised(_) if id != self.n_leader.pid => Some(id),
                _ => None,
            })
            .collect()
    }

    /// The pids of peers which have not promised to my ballot
    pub(crate) fn get_preparable_peers(&self, peers: &[NodeId]) -> Vec<NodeId> {
        peers
            .iter()
            .filter_map(|&pid| match self.promises_meta.get(&pid) {
                Some(PromiseState::NotPromised) | None => Some(pid),
                _ => None,
            })
            .collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparable_peers_test() {
        type Value = ();

        impl Entry for Value {}

        let nodes = vec![6, 7, 8];
        let leader_state = LeaderState::<Value>::with(Ballot::with(1, 1, 8), 3, 2, 3);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let leader_state = LeaderState::<Value>::with(Ballot::with(1, 1, 100), 3, 3, 4);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }
}
