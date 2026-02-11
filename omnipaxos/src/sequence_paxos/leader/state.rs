use crate::{
    sequence_paxos::Promise,
    storage::Entry,
    utils::{Ballot, LogSync, NodeId, PromiseMetaData, Quorum, SequenceNumber},
};

#[derive(Debug, Clone)]
/// The promise state of a node.
enum PromiseState {
    /// Not promised to any leader
    NotPromised,
    /// Promised to my ballot
    Promised(PromiseMetaData),
    /// Promised to a leader who's ballot is greater than mine
    PromisedHigher,
}

#[derive(Debug, Clone)]
pub(crate) struct LeaderState<T>
where
    T: Entry,
{
    pub(crate) n_leader: Ballot,
    promises_meta: Vec<PromiseState>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    follower_seq_nums: Vec<SequenceNumber>,
    pub(crate) accepted_indexes: Vec<usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    latest_accept_meta: Vec<Option<(Ballot, usize)>>, //  index in outgoing
    pub(crate) max_pid: usize,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub(crate) quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub(crate) fn with(n_leader: Ballot, max_pid: usize, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: vec![PromiseState::NotPromised; max_pid],
            follower_seq_nums: vec![SequenceNumber::default(); max_pid],
            accepted_indexes: vec![0; max_pid],
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
            latest_accept_meta: vec![None; max_pid],
            max_pid,
            quorum,
        }
    }

    fn pid_to_idx(pid: NodeId) -> usize {
        (pid - 1) as usize
    }

    // Resets `pid`'s accept sequence to indicate they are in the next session of accepts
    pub(crate) fn increment_seq_num_session(&mut self, pid: NodeId) {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].session += 1;
        self.follower_seq_nums[idx].counter = 0;
    }

    pub(crate) fn next_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        let idx = Self::pid_to_idx(pid);
        self.follower_seq_nums[idx].counter += 1;
        self.follower_seq_nums[idx]
    }

    pub(crate) fn get_seq_num(&mut self, pid: NodeId) -> SequenceNumber {
        self.follower_seq_nums[Self::pid_to_idx(pid)]
    }

    pub(crate) fn set_promise(
        &mut self,
        prom: Promise<T>,
        from: NodeId,
        check_max_prom: bool,
    ) -> bool {
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid: from,
        };
        if check_max_prom && promise_meta > self.max_promise_meta {
            self.max_promise_meta = promise_meta.clone();
            self.max_promise_sync = prom.log_sync;
        }
        self.promises_meta[Self::pid_to_idx(from)] = PromiseState::Promised(promise_meta);
        let num_promised = self
            .promises_meta
            .iter()
            .filter(|p| matches!(p, PromiseState::Promised(_)))
            .count();
        self.quorum.is_prepare_quorum(num_promised)
    }

    pub(crate) fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::NotPromised;
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub(crate) fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta[Self::pid_to_idx(pid)] = PromiseState::PromisedHigher;
    }

    pub(crate) fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub(crate) fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub(crate) fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .iter()
            .filter_map(|p| match p {
                PromiseState::Promised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub(crate) fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        match &self.promises_meta[Self::pid_to_idx(pid)] {
            PromiseState::Promised(metadata) => metadata,
            _ => panic!("No Metadata found for promised follower"),
        }
    }

    pub(crate) fn reset_latest_accept_meta(&mut self) {
        self.latest_accept_meta = vec![None; self.max_pid];
    }

    pub(crate) fn get_promised_followers(&self) -> Vec<NodeId> {
        self.promises_meta
            .iter()
            .enumerate()
            .filter_map(|(idx, x)| match x {
                PromiseState::Promised(_) if idx != Self::pid_to_idx(self.n_leader.pid) => {
                    Some((idx + 1) as NodeId)
                }
                _ => None,
            })
            .collect()
    }

    /// The pids of peers which have not promised a higher ballot than mine.
    pub(crate) fn get_preparable_peers(&self, peers: &[NodeId]) -> Vec<NodeId> {
        peers
            .iter()
            .filter_map(|pid| {
                let idx = Self::pid_to_idx(*pid);
                match self.promises_meta.get(idx).unwrap() {
                    PromiseState::NotPromised => Some(*pid),
                    _ => None,
                }
            })
            .collect()
    }

    pub(crate) fn set_latest_accept_meta(&mut self, pid: NodeId, idx: Option<usize>) {
        let meta = idx.map(|x| (self.n_leader, x));
        self.latest_accept_meta[Self::pid_to_idx(pid)] = meta;
    }

    pub(crate) fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        self.accepted_indexes[Self::pid_to_idx(pid)] = idx;
    }

    pub(crate) fn get_latest_accept_meta(&self, pid: NodeId) -> Option<(Ballot, usize)> {
        self.latest_accept_meta
            .get(Self::pid_to_idx(pid))
            .unwrap()
            .as_ref()
            .copied()
    }

    pub(crate) fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(Self::pid_to_idx(pid)).unwrap() {
            PromiseState::Promised(metadata) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub(crate) fn is_chosen(&self, idx: usize) -> bool {
        let num_accepted = self
            .accepted_indexes
            .iter()
            .filter(|la| **la >= idx)
            .count();
        self.quorum.is_accept_quorum(num_accepted)
    }
}

#[cfg(test)]
mod tests {
    use super::*; // Import functions and types from this module
    #[test]
    fn preparable_peers_test() {
        type Value = ();

        impl Entry for Value {}

        let nodes = vec![6, 7, 8];
        let quorum = Quorum::Majority(2);
        let max_pid = 8;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let quorum = Quorum::Majority(3);
        let max_pid = 100;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }
}
