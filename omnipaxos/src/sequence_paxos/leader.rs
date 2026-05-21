#[cfg(feature = "adaptive")]
use crate::sequence_paxos::predictor::Label;
use crate::{
    sequence_paxos::{
        messages::*,
        utils::{LeaderAction, LeaderState, LogSync},
        Promise, SequencePaxos,
    },
    utils::{AcceptStatus, Ballot, Entry, LogEntry, NodeId, Phase, Role},
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
            self.leader_state = LeaderState::with(n, self.quorum);
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
                let status = AcceptStatus::OpAccepted;
                let decided_idx = self.internal_storage.get_decided_idx();
                let (max_promise_sync, new_decided_idx) =
                    self.leader_state.take_my_log_sync(decided_idx, status);
                self.internal_storage
                    .append_suffix(max_promise_sync.suffix, max_promise_sync.sync_idx);
                self.internal_storage.set_decided_idx(new_decided_idx);
                for pid in self.leader_state.get_promised_followers() {
                    self.send_accsync(pid);
                }
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

    pub(crate) fn op_accept_entry_leader(&mut self, entry: T) {
        let accept_status = AcceptStatus::OpAccepted;

        // Add to storage
        let slot_idx = self
            .internal_storage
            .add_entry(LogEntry::Undecided(entry.clone(), accept_status));

        #[cfg(not(feature = "adaptive"))]
        self.pending_proposals.insert(slot_idx, entry.clone());
        #[cfg(feature = "adaptive")]
        self.pending_proposals
            .insert(slot_idx, (entry.clone(), None));

        // Send Accept messages
        for pid in self.leader_state.get_promised_followers() {
            let acc = Accept {
                n: self.leader_state.n_leader,
                seq_num: self.leader_state.next_seq_num(pid),
                entry: entry.clone(),
                slot_idx,
                accept_status,
            };
            self.send_msg_to(pid, PaxosMsg::Accept(acc));
        }

        // Add own proposal
        let leader_action = self.leader_state.add_proposal(
            self.internal_storage.get_decided_idx(),
            self.pid,
            slot_idx,
            entry,
            accept_status,
        );
        self.handle_leader_action(leader_action);
    }

    pub(crate) fn handle_accepted(&mut self, accepted: Accepted<T>, from: NodeId) {
        // For followers (calibration)
        #[cfg(feature = "adaptive")]
        if accepted.accept_status == AcceptStatus::TestAccepted {
            if self.calibrating {
                if let Some(index) =
                    self.test_proposals
                        .iter()
                        .position(|(slot_idx, entry, _, _)| {
                            *slot_idx == accepted.slot_idx && entry == &accepted.entry
                        })
                {
                    // Increase number of test accepts
                    self.test_proposals[index].3 += 1;
                    // If fast quorum would have been reached, add features and Success to
                    // calibration data
                    if self.quorum.is_fast_quorum(self.test_proposals[index].3) {
                        let (_, _, features, _) = self.test_proposals.remove(index);
                        self.calibration_data.push((features, Label::Success));
                    }
                }
            }
            return;
        }
        if accepted.n == self.leader_state.n_leader && self.state == (Role::Leader, Phase::Accept) {
            let leader_action = self.leader_state.add_proposal(
                self.internal_storage.get_decided_idx(),
                from,
                accepted.slot_idx,
                accepted.entry,
                accepted.accept_status,
            );
            #[cfg(feature = "logging")]
            debug!(
                self.logger,
                "Got {:?} from {} for slot {:?} => LeaderAction: {:?}",
                accepted.accept_status,
                from,
                accepted.slot_idx,
                leader_action
            );
            self.handle_leader_action(leader_action);
        }
    }

    fn handle_leader_action(&mut self, action: LeaderAction<T>) {
        match action {
            LeaderAction::ProcessSlowPath(slot_idx, entry) => {
                let accept_status = AcceptStatus::FpSlowAccepted;
                for pid in self.leader_state.get_promised_followers() {
                    let a = Accept {
                        n: self.leader_state.n_leader,
                        seq_num: self.leader_state.next_seq_num(pid),
                        entry: entry.clone(),
                        slot_idx,
                        accept_status,
                    };
                    self.send_msg_to(pid, PaxosMsg::Accept(a));
                }
                self.internal_storage
                    .insert_at_index(slot_idx, LogEntry::Undecided(entry, accept_status));
            }
            LeaderAction::Decided(new_decided_entries, new_decided_index) => {
                for (slot_idx, entry, accept_status) in new_decided_entries {
                    for pid in self.leader_state.get_promised_followers() {
                        let d = Decide {
                            n: self.leader_state.n_leader,
                            seq_num: self.leader_state.next_seq_num(pid),
                            entry: entry.clone(),
                            accept_status,
                            slot_idx,
                        };
                        self.send_msg_to(pid, PaxosMsg::Decide(d));
                    }
                    if let Some(own_proposed_entry_at_slot) =
                        self.pending_proposals.remove(&slot_idx)
                    {
                        #[cfg(not(feature = "adaptive"))]
                        let own_prop_entry = own_proposed_entry_at_slot;
                        #[cfg(feature = "adaptive")]
                        let own_prop_entry = own_proposed_entry_at_slot.0;
                        if self.retrying && own_prop_entry != entry {
                            // The slot was taken by another entry; retry our proposal
                            self.append(own_prop_entry);
                        }
                    }
                    self.internal_storage
                        .insert_at_index(slot_idx, LogEntry::Decided(entry, accept_status));
                }
                self.internal_storage.set_decided_idx(new_decided_index);
            }
            LeaderAction::None => {}
        }
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
