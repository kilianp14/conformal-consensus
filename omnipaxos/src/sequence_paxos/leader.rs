use crate::{
    sequence_paxos::{
        messages::*,
        utils::{LeaderState, LogSync},
        Promise, SequencePaxos,
    },
    utils::{Ballot, Entry, EntryId, LogEntry, NodeId, Phase, Role, SlotStatus},
};
#[cfg(feature = "logging")]
use slog::{debug, info};

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
            let decided_idx = self.internal_storage.get_decided_idx();
            self.leader_state.set_promise(self.pid, decided_idx);
            self.state = (Role::Leader, Phase::Prepare);
            /* send prepare */
            let prep = Prepare { n, decided_idx };
            self.send_to_all_peers(PaxosMsg::Prepare(prep));
        } else {
            self.state.0 = Role::Follower;
        }
    }

    pub(crate) fn handle_preparereq(&mut self, prepreq: PrepareReq, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(self.logger, "Incoming message PrepareReq from {}", from);
        if self.state.0 == Role::Leader && prepreq.n <= self.leader_state.n_leader {
            self.leader_state.reset_promise(from);
            self.send_prepare(from);
        }
    }

    fn send_prepare(&mut self, to: NodeId) {
        let prep = Prepare {
            n: self.leader_state.n_leader,
            decided_idx: self.internal_storage.get_decided_idx(),
        };
        self.send_msg_to(to, PaxosMsg::Prepare(prep));
    }

    pub(crate) fn handle_promise_prepare(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        debug!(
            self.logger,
            "Handling promise from {} in Prepare phase", from
        );
        if let Some(LogSync { suffix, sync_idx }) = prom.log_sync {
            let index = sync_idx;
            let current_decided_idx = self.internal_storage.get_decided_idx();
            for entry in suffix {
                if index > current_decided_idx {
                    match entry {
                        LogEntry::Decided(value) => {
                            self.internal_storage.insert_at_index(index, entry);
                            self.internal_storage.set_decided_idx(index);
                        }
                        LogEntry::Empty => (),
                        LogEntry::Undecided(entry_id, entry, slot_status) => {
                            self.leader_state.add_proposal(from, index, (entry_id, entry), slot_status);
                        }
                    }
                }
                index += 1;
            }
        }
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(from, prom.decided_idx);
            if received_majority {
                for 
                for pid in self.leader_state.get_promised_followers() {
                    self.send_accsync(pid);
                }
            }
        }
    }

    fn handle_majority_promises(&mut self) {
        let max_promise_sync = self.leader_state.take_max_promise_sync();
        let mut new_accepted_idx = match max_promise_sync {
            Some(LogSync { suffix, sync_idx }) => {
                self.internal_storage.append_suffix(suffix, sync_idx)
            }
            None => self.internal_storage.get_accepted_idx(),
        };
        if !self.buffered_proposals.is_empty() {
            let entries = std::mem::take(&mut self.buffered_proposals);
            for entry in entries {
                self.internal_storage.insert_entry(entry);
            }
            new_accepted_idx = self.internal_storage.get_accepted_idx();
        }
        self.state = (Role::Leader, Phase::Accept);
        for pid in self.leader_state.get_promised_followers() {
            self.send_accsync(pid);
        }
    }

    pub(crate) fn handle_promise_accept(&mut self, prom: Promise<T>, from: NodeId) {
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming message Promise Accept from {}", r, p, from
            );
        }
        let promise_meta = PromiseMetaData {
            n_accepted: prom.n_accepted,
            accepted_idx: prom.accepted_idx,
            decided_idx: prom.decided_idx,
            pid: from,
        };
        if prom.n == self.leader_state.n_leader {
            self.leader_state.set_promise(promise_meta, prom.log_sync);
            self.send_accsync(from);
        }
    }

    pub(crate) fn handle_op_forwarded_proposal(&mut self, entry: (EntryId, T)) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => self.op_accept_entry_leader(entry),
            _ => self.forward_proposal(entry),
        }
    }

    pub(crate) fn op_accept_entry_leader(&mut self, entry: (EntryId, T)) {
        let slot_idx =
            self.internal_storage
                .add_entry(LogEntry::Undecided(SlotStatus::OpAccepted(
                    entry.0,
                    entry.1.clone(),
                )));
        for pid in self.leader_state.get_promised_followers() {
            let acc = Accept {
                n: self.leader_state.n_leader,
                seq_num: self.leader_state.next_seq_num(pid),
                entry: entry.clone(),
                slot_idx,
            };
            self.send_msg_to(pid, PaxosMsg::OpAccept(acc));
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

    pub(crate) fn handle_op_accepted(&mut self, op_accepted: Accepted, from: NodeId) {
        if op_accepted.n == self.leader_state.n_leader
            && self.state == (Role::Leader, Phase::Accept)
        {
            self.leader_state
                .set_accepted_idx(from, op_accepted.accepted_idx);
            if op_accepted.accepted_idx > self.internal_storage.get_decided_idx()
                && self.leader_state.is_chosen(op_accepted.accepted_idx)
            {
                let decided_idx = op_accepted.accepted_idx;
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
        debug!(
            self.logger,
            "Got Accepted from {}, idx: {}, chosen_idx: {}",
            from,
            op_accepted.accepted_idx,
            self.internal_storage.get_decided_idx(),
        );
    }

    pub(crate) fn handle_fp_fast_accepted(&mut self, fast_accepted: FpFastAccepted<T>) {
        // TODO
    }

    pub(crate) fn handle_fp_slow_accepted(&mut self, slow_accepted: Accepted) {
        // TODO
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
