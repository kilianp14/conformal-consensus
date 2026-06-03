use crate::utils::{AcceptStatus, Ballot, Entry, LogEntry, NodeId, Quorum, SequenceNumber};
#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
use std::collections::HashMap;

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
enum SlotResult<T> {
    // A quorum has not voted yet
    // or quorum has voted uniformly with fast accepts, but a fast quorum has not been achieved yet
    Pending,
    // A quorum has voted, but vote is not uniform
    SlowPath(T),
    // A majority quorum has voted uniformly using OpAccepted
    // a fast quorum has voted uniformly using FpFastAccepted
    // or a majority has voted uniformly using FpSlowAccepted.
    // Way of acceptance is in the accept status
    Decided(T, AcceptStatus),
}

#[derive(Debug, Clone)]
pub(crate) enum LeaderAction<T> {
    /// No significant state change occurred.
    None,
    /// The slot has transitioned to a Slow Path; the leader must re-propose.
    ProcessSlowPath(SlotId, T),
    /// One or more slots have been finalized using an acceptance method. Contains new decisions and new decided_idx
    Decided(Vec<(SlotId, T, AcceptStatus)>, SlotId),
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
    // Stores proposals by slot and node
    accept_meta: HashMap<SlotId, HashMap<NodeId, (T, AcceptStatus)>>,
    /// Tracks the current state of each slot to detect transitions
    slot_results: HashMap<SlotId, SlotResult<T>>,
    // Quorums
    pub(crate) quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub(crate) fn with(n_leader: Ballot, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: HashMap::with_capacity(quorum.total_nodes),
            log_syncs: HashMap::with_capacity(quorum.total_nodes),
            follower_seq_nums: HashMap::with_capacity(quorum.total_nodes),
            accept_meta: HashMap::new(),
            slot_results: HashMap::new(),
            quorum,
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
        self.quorum.is_majority_quorum(num_promised)
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
    /// TODO: Log syncing is NOT SAFE atm. NEEDS CHANGE FOR PROPER APPLICAION
    pub(crate) fn take_my_log_sync(
        &mut self,
        decided_idx: SlotId,
        status: AcceptStatus,
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
                LogEntry::Decided(_, _) => {
                    current_decided_idx += 1;
                }
                LogEntry::Undecided(_, entry_status) => {
                    // Replace Undecided status with the target
                    *entry_status = status;
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
        // If there is a decided entry, return it
        if let Some(pos) = values
            .iter()
            .position(|e| matches!(e, LogEntry::Decided(_, _)))
        {
            return values.remove(pos);
        }

        // Count frequencies of Undecided entries
        let mut counts: Vec<(&T, usize)> = Vec::new();
        for entry in &values {
            if let LogEntry::Undecided(value, _) = entry {
                if let Some(existing) = counts.iter_mut().find(|(val, _)| *val == value) {
                    existing.1 += 1;
                } else {
                    counts.push((value, 1));
                }
            }
        }

        // Find the EntryId with the highest count
        // max_by_key will pick one arbitrarily if there's a tie.
        if let Some((max_entry, _)) = counts.into_iter().max_by_key(|&(_, count)| count) {
            let pos = values
                .iter()
                .position(|e| matches!(e, LogEntry::Undecided(entry, _) if entry == max_entry))
                .expect("Winning ID must exist in the original list");

            return values.remove(pos);
        }

        // If no Decided or Undecided entries exist, it's Empty
        LogEntry::Empty
    }

    pub(crate) fn add_proposal(
        &mut self,
        decided_idx: SlotId,
        pid: NodeId,
        slot_idx: SlotId,
        entry: T,
        accept_status: AcceptStatus,
    ) -> LeaderAction<T> {
        // Ignore proposals for slots that are already locally decided
        if slot_idx < decided_idx {
            return LeaderAction::None;
        }
        self.accept_meta
            .entry(slot_idx)
            .or_default()
            .insert(pid, (entry, accept_status));

        // Check if slot is pending to avoid initiaing slow paths more than once
        let was_pending = matches!(
            self.slot_results.get(&slot_idx),
            None | Some(SlotResult::Pending)
        );

        // Compute slot result
        let slot_proposals = self
            .accept_meta
            .get(&slot_idx)
            .expect("There should never be no proposals");
        let slot_result = compute_propose_result(slot_proposals, &self.quorum);

        // State Logic
        match slot_result {
            // Only trigger the "Decided" action if we just decided at the current decided_idx
            SlotResult::Decided(_, _) if slot_idx == decided_idx => {
                self.slot_results.insert(slot_idx, slot_result);
                let mut newly_decided = Vec::new();
                let mut current_idx = slot_idx;

                // Drain contiguous decided slots
                while let Some(SlotResult::Decided(_, _)) = self.slot_results.get(&current_idx) {
                    // Now that we know it's a Decided variant, remove it to take ownership
                    if let Some(SlotResult::Decided(entry, status)) =
                        self.slot_results.remove(&current_idx)
                    {
                        newly_decided.push((current_idx, entry, status));
                        self.accept_meta.remove(&current_idx);
                        current_idx += 1;
                    }
                }
                LeaderAction::Decided(newly_decided, current_idx)
            }
            // Only trigger slow path action if slot was previously pending
            SlotResult::SlowPath(entry) if was_pending => {
                self.slot_results
                    .insert(slot_idx, SlotResult::SlowPath(entry.clone()));
                LeaderAction::ProcessSlowPath(slot_idx, entry.clone())
            }
            _ => {
                self.slot_results.insert(slot_idx, slot_result);
                LeaderAction::None
            }
        }
    }

    pub(crate) fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(&pid) {
            Some(PromiseState::Promised(_, decided_idx)) => Some(*decided_idx),
            _ => None,
        }
    }

    pub(crate) fn get_promised_followers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .filter_map(|(&id, promise_state)| match promise_state {
                PromiseState::Promised(_, _) if id != self.n_leader.pid => Some(id),
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

fn compute_propose_result<T>(
    proposals: &HashMap<NodeId, (T, AcceptStatus)>,
    quorum: &Quorum,
) -> SlotResult<T>
where
    T: Entry,
{
    // Not enough votes
    let total_votes_in_slot = proposals.len();
    if !quorum.is_majority_quorum(total_votes_in_slot) {
        return SlotResult::Pending;
    }

    // Count number of votes for every entry
    #[allow(clippy::type_complexity)]
    let mut vote_counts: Vec<(&T, (usize, usize, usize, usize))> =
        Vec::with_capacity(quorum.total_nodes);

    for (entry, status) in proposals.values() {
        let counts = if let Some(existing) = vote_counts.iter_mut().find(|(val, _)| *val == entry) {
            &mut existing.1
        } else {
            vote_counts.push((entry, (0, 0, 0, 0)));
            &mut vote_counts.last_mut().unwrap().1
        };

        counts.3 += 1; // Increment total
        match status {
            AcceptStatus::OpAccepted => counts.0 += 1,
            AcceptStatus::FpFastAccepted => counts.1 += 1,
            AcceptStatus::FpSlowAccepted => counts.2 += 1,
            AcceptStatus::TestAccepted => {
                panic!("Test Entry should not be in leader decision process")
            }
        }
    }

    // Identify the entry with the most votes
    let winner = vote_counts
        .iter()
        .max_by_key(|(_, (_, _, _, total))| *total)
        .map(|(id, counts)| ((*id).clone(), *counts));

    // Check if we can make a decision
    if let Some((entry, (op, fast, slow, _))) = winner.clone() {
        if quorum.is_majority_quorum(op) {
            return SlotResult::Decided(entry, AcceptStatus::OpAccepted);
        }
        if quorum.is_fast_quorum(fast) {
            return SlotResult::Decided(entry, AcceptStatus::FpFastAccepted);
        }
        if quorum.is_majority_quorum(slow) {
            return SlotResult::Decided(entry, AcceptStatus::FpSlowAccepted);
        }
    }

    if !quorum.is_fast_quorum(total_votes_in_slot) {
        return SlotResult::Pending;
    }

    let remaining_votes = quorum.total_nodes - total_votes_in_slot;
    for (e, (op, fast, slow, _)) in vote_counts {
        // Calculate if a decision is still possible
        if quorum.is_fast_quorum(remaining_votes + fast)
            || quorum.is_majority_quorum(remaining_votes + op)
            || slow > 0
        {
            // It is still possible to reach a decision if the
            // remaining nodes vote for this entry_id.
            // We could also initiate a slow path from this point since only one value
            // is possible once more than a fast quorum has voted
            return SlotResult::SlowPath(e.to_owned());
        }
    }

    // Collision definitely happened, continue slow path with value that has the most votes
    SlotResult::SlowPath(
        winner
            .expect("no winner should not be possible at this point")
            .0,
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn preparable_peers_test() {
        type Value = ();

        impl Entry for Value {}

        let nodes = vec![6, 7, 8];
        let leader_state = LeaderState::<Value>::with(Ballot::with(1, 1, 8), Quorum::with(3));
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let leader_state = LeaderState::<Value>::with(Ballot::with(1, 1, 100), Quorum::with(5));
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }
}
