use std::collections::HashMap;

use crate::{
    sequence_paxos::{
        messages::*,
        utils::{LogData, LogSync, PromiseMetaData},
        Promise, SequencePaxos,
    },
    utils::{Ballot, Entry, NodeId, Phase, Quorum, Role, SequenceNumber},
};
#[cfg(feature = "logging")]
use slog::info;

impl<T> SequencePaxos<T>
where
    T: Entry,
{
    /// Handle a new leader. Should be called when the leader election has elected a new leader with the ballot `n`
    /*** Leader ***/
    pub(crate) fn handle_leader(&mut self, n: Ballot) {
        if n <= self.internal_storage.get_promise() {
            return;
        }
        #[cfg(feature = "logging")]
        info!(self.logger, "Newly elected leader: {:?}", n);
        if self.pid == n.pid {
            self.leader_state =
                LeaderState::with(n, self.peers.len() + 1, self.leader_state.quorum);
            /* insert my promise */
            self.internal_storage.set_promise(n);
            let na = self.internal_storage.get_accepted_round();
            let decided_idx = self.internal_storage.get_decided_idx();
            let accepted_idx = self.internal_storage.get_accepted_idx();
            let my_promise = Promise {
                n,
                n_accepted: na,
                decided_idx,
                accepted_idx,
                log_sync: None,
            };
            self.leader_state.set_promise(my_promise, self.pid, true);
            self.state = (Role::Leader, Phase::Prepare);
            /* send prepare */
            let prep = Prepare {
                n,
                decided_idx,
                n_accepted: na,
                accepted_idx,
            };
            self.send_to_all_peers(PaxosMsg::Prepare(prep));
        } else {
            self.state.0 = Role::Follower;
        }
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.send_prepare(from);
        }
    }

    fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
            n_accepted: self.internal_storage.get_accepted_round(),
            accepted_idx: self.internal_storage.get_accepted_idx(),
        };
        self.send_msg_to(to, PaxosMsg::Prepare(prep));
    }

    pub(crate) fn handle_forwarded_proposal(&mut self, entry: LogData<T>) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.accept_entry_leader(entry),
            _ => self.forward_proposal(entry),
        }
    }

    pub(crate) fn accept_entry_leader(&mut self, entry: LogData<T>) {
        let accepted_idx = self.internal_storage.append_entry(entry.clone());
        self.leader_state.set_accepted_idx(self.pid, accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            let acc = SlowAccept {
                n: self.leader_state.n_leader,
                seq_num: self.leader_state.next_seq_num(pid),
                entry: entry.clone(),
            };
            self.send_msg_to(pid, PaxosMsg::SlowAccept(acc));
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let current_n = self.leader_state.n_leader;
        let PromiseMetaData {
            n_accepted: prev_round_max_promise_n,
            accepted_idx: prev_round_max_accepted_idx,
            ..
        } = &self.leader_state.get_max_promise_meta();
        let PromiseMetaData {
            n_accepted: followers_promise_n,
            accepted_idx: followers_accepted_idx,
            pid,
            ..
        } = self.leader_state.get_promise_meta(to);
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(*pid)
            .expect("Received PromiseMetaData but not found in ld");
        // Follower can have valid accepted entries depending on which leader they were previously following
        let followers_valid_entries_idx = if *followers_promise_n == current_n {
            *followers_accepted_idx
        } else if *followers_promise_n == *prev_round_max_promise_n {
            *prev_round_max_accepted_idx.min(followers_accepted_idx)
        } else {
            followers_decided_idx
        };
        let log_sync = self.create_log_sync(followers_valid_entries_idx);
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: current_n,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.internal_storage.get_decided_idx(),
            log_sync,
        };
        self.send_msg_to(to, PaxosMsg::AcceptSync(acc_sync));
    }

    fn handle_majority_promises(&mut self) {
        let decided_idx = self.leader_state.get_max_decided_idx();
        self.internal_storage.set_decided_idx(decided_idx);
        self.internal_storage
            .set_accepted_round(self.leader_state.n_leader);
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let mut new_accepted_idx = match max_promise_sync {
            Some(LogSync { suffix, sync_idx }) => {
                self.internal_storage.append_suffix(suffix, sync_idx)
            }
            None => self.internal_storage.get_accepted_idx(),
        };
        if !self.buffered_proposals.is_empty() {
            let entries = std::mem::take(&mut self.buffered_proposals);
            new_accepted_idx = self.internal_storage.append_entries(entries);
        }
        self.state = (Role::Leader, Phase::Accept);
        self.leader_state
            .set_accepted_idx(self.pid, new_accepted_idx);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(prom, from, true);
            if received_majority {
                self.handle_majority_promises();
            }
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            info!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(prom, from, false);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted, from: NodeId) {
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            self.leader_state
                .set_accepted_idx(from, accepted.accepted_idx);
            if accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(accepted.accepted_idx)
            {
                let decided_idx = accepted.accepted_idx;
                self.internal_storage.set_decided_idx(decided_idx);
                for pid in self.leader_state.get_promised_followers() {
                    let d = Decide {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        decided_idx,
                    };
                    self.send_msg_to(pid, PaxosMsg::Decide(d));
                }
            }
        }
        #[cfg(feature = "logging")]
        info!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
        );
    }

    pub(crate) fn handle_notaccepted(&mut self, not_acc: NotAccepted, from: NodeId) {
        if self.state.0 == Role::Leader && self.leader_state.n_leader < not_acc.n {
            self.leader_state.lost_promise(from);
        }
    }

    pub(crate) fn resend_messages_leader(&mut self) {
        match self.state.1 {
            Phase::Prepare | Phase::Accept => {
                let preparable_peers = self.leader_state.get_preparable_peers(&self.peers);
                for peer in preparable_peers {
                    self.send_prepare(peer);
                }
            }
            _ => (),
        }
    }
}

#[derive(Default, Debug, Clone)]
/// The promise state of a node.
enum PromiseState {
    /// Not promised to any leader
    #[default]
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
    promises_meta: HashMap<NodeId, PromiseState>,
    // the sequence number of accepts for each follower where AcceptSync has sequence number = 1
    follower_seq_nums: HashMap<NodeId, SequenceNumber>,
    accepted_indexes: HashMap<NodeId, usize>,
    max_promise_meta: PromiseMetaData,
    max_promise_sync: Option<LogSync<T>>,
    // The number of promises needed in the prepare phase to become synced and
    // the number of accepteds needed in the accept phase to decide an entry.
    pub(crate) quorum: Quorum,
}

impl<T> LeaderState<T>
where
    T: Entry,
{
    pub(crate) fn with(n_leader: Ballot, n_nodes: usize, quorum: Quorum) -> Self {
        Self {
            n_leader,
            promises_meta: HashMap::with_capacity(n_nodes),
            follower_seq_nums: HashMap::with_capacity(n_nodes),
            accepted_indexes: HashMap::with_capacity(n_nodes),
            max_promise_meta: PromiseMetaData::default(),
            max_promise_sync: None,
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
        self.promises_meta
            .insert(from, PromiseState::Promised(promise_meta));
        let num_promised = self.promises_meta.values().count();

        self.quorum.is_prepare_quorum(num_promised)
    }

    pub(crate) fn reset_promise(&mut self, pid: NodeId) {
        self.promises_meta.insert(pid, PromiseState::NotPromised);
    }

    /// Node `pid` seen with ballot greater than my ballot
    pub(crate) fn lost_promise(&mut self, pid: NodeId) {
        self.promises_meta.insert(pid, PromiseState::PromisedHigher);
    }

    pub(crate) fn take_max_promise_sync(&mut self) -> Option<LogSync<T>> {
        std::mem::take(&mut self.max_promise_sync)
    }

    pub(crate) fn get_max_promise_meta(&self) -> &PromiseMetaData {
        &self.max_promise_meta
    }

    pub(crate) fn get_max_decided_idx(&self) -> usize {
        self.promises_meta
            .values()
            .filter_map(|p| match p {
                PromiseState::Promised(m) => Some(m.decided_idx),
                _ => None,
            })
            .max()
            .unwrap_or_default()
    }

    pub(crate) fn get_promise_meta(&self, pid: NodeId) -> &PromiseMetaData {
        match self.promises_meta.get(&pid) {
            Some(PromiseState::Promised(metadata)) => metadata,
            _ => panic!("No Metadata found for promised follower"),
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

    pub(crate) fn set_accepted_idx(&mut self, pid: NodeId, idx: usize) {
        self.accepted_indexes.insert(pid, idx);
    }

    pub(crate) fn get_decided_idx(&self, pid: NodeId) -> Option<usize> {
        match self.promises_meta.get(&pid) {
            Some(PromiseState::Promised(metadata)) => Some(metadata.decided_idx),
            _ => None,
        }
    }

    pub(crate) fn is_chosen(&self, idx: usize) -> bool {
        let num_accepted = self
            .accepted_indexes
            .values()
            .filter(|&&la| la >= idx)
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
            LeaderState::<Value>::with(Ballot::with(1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);

        let nodes = vec![7, 1, 100, 4, 6];
        let quorum = Quorum::Majority(3);
        let max_pid = 100;
        let leader_state =
            LeaderState::<Value>::with(Ballot::with(1, 1, max_pid), max_pid as usize, quorum);
        let prep_peers = leader_state.get_preparable_peers(&nodes);
        assert_eq!(prep_peers, nodes);
    }
}
