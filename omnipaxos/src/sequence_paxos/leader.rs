use crate::{
    sequence_paxos::{
        messages::*,
        utils::{LeaderState, LogSync, PromiseMetaData},
        Promise, SequencePaxos,
    },
    utils::{Ballot, Entry, EntryId, LogEntry, Mode, NodeId, Phase, Role, SlotStatus},
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
            self.leader_state = LeaderState::with(
                n,
                self.peers.len() + 1,
                self.leader_state.quorum_size,
                self.leader_state.super_quorum_size,
            );
            /* insert my promise */
            self.internal_storage.set_promise(n);
            let decided_idx = self.internal_storage.get_decided_idx();
            let n_accepted = self.internal_storage.get_accepted_round();
            self.leader_state.set_promise(
                self.pid,
                n_accepted,
                decided_idx,
                LogSync {
                    suffix: vec![],
                    sync_idx: decided_idx,
                },
            );
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
        if prom.n == self.leader_state.n_leader {
            let received_majority = self.leader_state.set_promise(
                from,
                prom.n_accepted,
                prom.decided_idx,
                prom.log_sync,
            );
            if received_majority {
                self.state = (Role::Leader, Phase::Accept);
                let status = match self.mode {
                    Mode::OmniPaxos => SlotStatus::OpAccepted,
                    Mode::FastPaxos => SlotStatus::FpSlowAccepted,
                };
                let decided_idx = self.internal_storage.get_decided_idx();
                let (max_promise_sync, new_decided_idx) = self
                    .leader_state
                    .take_my_log_sync(decided_idx, status.clone());
                self.internal_storage
                    .append_suffix(max_promise_sync.suffix, max_promise_sync.sync_idx);
                self.internal_storage.set_decided_idx(new_decided_idx);
                for pid in self.leader_state.get_promised_followers() {
                    self.send_accsync(pid);
                }
                self.handle_buffered_proposals();
            }
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
        if prom.n == self.leader_state.n_leader {
            self.leader_state
                .set_promise(from, prom.n_accepted, prom.decided_idx, prom.log_sync);
            self.send_accsync(from);
        }
    }

    fn send_accsync(&mut self, to: NodeId) {
        let followers_decided_idx = self
            .leader_state
            .get_decided_idx(to)
            .expect("Received PromiseMetaData but not found in ld");
        let log_sync = LogSync {
            suffix: self.internal_storage.get_suffix(followers_decided_idx),
            sync_idx: followers_decided_idx,
        };
        self.leader_state.increment_seq_num_session(to);
        let acc_sync = AcceptSync {
            n: self.leader_state.n_leader,
            seq_num: self.leader_state.next_seq_num(to),
            decided_idx: self.internal_storage.get_decided_idx(),
            log_sync,
        };
        self.send_msg_to(to, PaxosMsg::AcceptSync(acc_sync));
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
