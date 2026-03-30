use crate::{
    sequence_paxos::{messages::*, utils::LogSync, Promise, SequencePaxos},
    utils::{
        AcceptStatus, Ballot, Entry, EntryId, LogEntry, MessageStatus, NodeId, Phase, Role,
        SequenceNumber,
    },
};
#[cfg(feature = "logging")]
use slog::{debug, info, trace, warn};

impl<T> SequencePaxos<T>
where
    T: Entry,
{
    /*** Follower ***/
    pub(crate) fn handle_prepare(&mut self, prep: Prepare, from: NodeId) {
        let old_promise = self.internal_storage.get_promise();
        if old_promise < prep.n || (old_promise == prep.n && self.state.1 == Phase::Recover) {
            self.internal_storage.set_promise(prep.n);
            self.state = (Role::Follower, Phase::Prepare);
            self.current_seq_num = SequenceNumber::default();
            let na = self.internal_storage.get_accepted_round();
            // send leader everything after his decided index
            let log_sync = LogSync {
                suffix: self.internal_storage.get_suffix(prep.decided_idx),
                sync_idx: prep.decided_idx,
            };
            let promise = Promise {
                n: prep.n,
                n_accepted: na,
                decided_idx: self.internal_storage.get_decided_idx(),
                log_sync,
            };
            self.cached_promise_message = Some(promise.clone());
            self.send_msg_to(from, PaxosMsg::Promise(promise));
            #[cfg(feature = "logging")]
            info!(self.logger, "Pid: {} promising {:?}", self.pid, prep.n);
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            #[cfg(feature = "logging")]
            {
                debug!(
                    self.logger,
                    "Pid {}. Incoming Accept Sync from {:?}: {:?}", self.pid, from, accsync
                );
            }
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            self.cached_promise_message = None;
            self.internal_storage.set_decided_idx(accsync.decided_idx);
            self.internal_storage.set_accepted_round(accsync.n);
            self.internal_storage
                .append_suffix(accsync.log_sync.suffix, accsync.log_sync.sync_idx);

            // Accept all the undecided slots after the decided index
            let mut slot_idx = accsync.decided_idx;
            for log_entry in self.internal_storage.get_suffix(slot_idx) {
                if let LogEntry::Undecided(entry_id, entry, accept_status) = log_entry {
                    let accepted = Accepted {
                        n: accsync.n,
                        entry: (entry_id, entry),
                        slot_idx,
                        accept_status,
                    };
                    self.send_msg_to(from, PaxosMsg::Accepted(accepted));
                }
                slot_idx += 1;
            }
            self.handle_buffered_proposals();
        }
    }

    pub(crate) fn fp_fast_propose(&mut self, entry: (EntryId, T)) {
        let accept_status = AcceptStatus::FpFastAccepted;
        // Add to storage
        let slot_idx = self.internal_storage.add_entry(LogEntry::Undecided(
            entry.0,
            entry.1.clone(),
            accept_status,
        ));

        // Send fast accept to all peers
        let acc = Accept {
            n: self.internal_storage.get_promise(),
            seq_num: SequenceNumber::default(), // not needed for fast path
            entry: entry.clone(),
            slot_idx,
            accept_status,
        };
        self.send_to_all_peers(PaxosMsg::Accept(acc));
        self.pending_proposals.insert(slot_idx, entry.clone());

        // Send own accepted to leader or handle if I am leader
        let accepted = Accepted {
            n: self.internal_storage.get_promise(),
            slot_idx,
            entry,
            accept_status,
        };
        match self.state.0 {
            Role::Follower => {
                self.send_msg_to(self.get_current_leader(), PaxosMsg::Accepted(accepted));
            }
            Role::Leader => {
                self.handle_accepted(accepted, self.pid);
            }
        }
    }

    pub(crate) fn handle_fast_accept(&mut self, acc: Accept<T>) {
        if self.check_valid_ballot(acc.n)
            && self.state.1 == Phase::Accept
            // Fast Accepts should never override
            && self.internal_storage.slot_is_empty(acc.slot_idx)
        {
            #[cfg(feature = "logging")]
            {
                debug!(
                    self.logger,
                    "Pid {}. Incoming Fast Accept: {:?}", self.pid, acc
                );
            }
            self.internal_storage.insert_at_index(
                acc.slot_idx,
                LogEntry::Undecided(acc.entry.0, acc.entry.1.clone(), acc.accept_status),
            );
            let accepted = Accepted {
                n: self.internal_storage.get_promise(),
                entry: acc.entry,
                slot_idx: acc.slot_idx,
                accept_status: acc.accept_status,
            };
            match self.state.0 {
                Role::Follower => {
                    self.send_msg_to(self.get_current_leader(), PaxosMsg::Accepted(accepted))
                }
                Role::Leader => self.handle_accepted(accepted, self.pid),
            }
        }
    }

    pub(crate) fn handle_slow_accept(&mut self, acc: Accept<T>) {
        if self.check_valid_ballot(acc.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(acc.seq_num, acc.n.pid) == MessageStatus::Expected
            // Fast Paxos Slow Accepts should always override, otherwise only override empty slots
            && (acc.accept_status == AcceptStatus::FpSlowAccepted
                || self.internal_storage.slot_is_empty(acc.slot_idx))
        {
            #[cfg(feature = "logging")]
            {
                debug!(
                    self.logger,
                    "Pid {}. Incoming Slow Accept: {:?}", self.pid, acc
                );
            }
            self.internal_storage.insert_at_index(
                acc.slot_idx,
                LogEntry::Undecided(acc.entry.0, acc.entry.1.clone(), acc.accept_status),
            );
            let accepted = Accepted {
                n: acc.n,
                entry: acc.entry,
                slot_idx: acc.slot_idx,
                accept_status: acc.accept_status,
            };
            self.send_msg_to(acc.n.pid, PaxosMsg::Accepted(accepted));
        }
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide<T>) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            #[cfg(feature = "logging")]
            {
                debug!(self.logger, "Pid {}. Incoming Decide: {:?}", self.pid, dec);
            }
            if let Some(own_proposed_entry_at_slot) = self.pending_proposals.remove(&dec.slot_idx) {
                if own_proposed_entry_at_slot.0 != dec.entry.0 {
                    // The slot was taken by another entry; retry our proposal
                    self.try_append(own_proposed_entry_at_slot);
                }
            }
            self.internal_storage
                .insert_at_index(dec.slot_idx, LogEntry::Decided(dec.entry.1));
            if dec.slot_idx >= self.internal_storage.get_decided_idx() {
                self.internal_storage.set_decided_idx(dec.slot_idx + 1);
            }
        }
    }

    /// Also returns whether the message's ballot was promised
    fn check_valid_ballot(&mut self, message_ballot: Ballot) -> bool {
        let my_promise = self.internal_storage.get_promise();
        match my_promise.cmp(&message_ballot) {
            std::cmp::Ordering::Equal => true,
            std::cmp::Ordering::Greater => {
                let not_acc = NotAccepted { n: my_promise };
                #[cfg(feature = "logging")]
                trace!(
                    self.logger,
                    "NotAccepted. My promise: {:?}, theirs: {:?}",
                    my_promise,
                    message_ballot
                );
                self.send_msg_to(message_ballot.pid, PaxosMsg::NotAccepted(not_acc));
                false
            }
            std::cmp::Ordering::Less => {
                #[cfg(feature = "logging")]
                warn!(
                    self.logger,
                    "Received non-prepare message from a leader I've never promised. My: {:?}, theirs: {:?}", my_promise, message_ballot
                );
                self.reconnected(message_ballot.pid);
                false
            }
        }
    }

    /// Also returns the MessageStatus of the sequence based on the incoming sequence number.
    fn handle_sequence_num(&mut self, seq_num: SequenceNumber, from: NodeId) -> MessageStatus {
        let msg_status = self.current_seq_num.check_msg_status(seq_num);
        match msg_status {
            MessageStatus::Expected => self.current_seq_num = seq_num,
            MessageStatus::DroppedPreceding => self.reconnected(from),
            MessageStatus::Outdated => (),
        };
        msg_status
    }

    pub(crate) fn resend_messages_follower(&mut self) {
        match self.state.1 {
            Phase::Prepare => {
                // Resend Promise
                match &self.cached_promise_message {
                    Some(promise) => {
                        self.send_msg_to(promise.n.pid, PaxosMsg::Promise(promise.clone()));
                    }
                    None => {
                        // Shouldn't be possible to be in prepare phase without having
                        // cached the promise sent as a response to the prepare
                        #[cfg(feature = "logging")]
                        warn!(self.logger, "In Prepare phase without a cached promise!");
                        self.state = (Role::Follower, Phase::Recover);
                        self.send_preparereq_to_all_peers();
                    }
                }
            }
            Phase::Recover => {
                // Resend PrepareReq
                self.send_preparereq_to_all_peers();
            }
            Phase::Accept => (),
            Phase::None => (),
        }
    }

    fn send_preparereq_to_all_peers(&mut self) {
        let prepreq = PrepareReq {
            n: self.internal_storage.get_promise(),
        };
        self.send_to_all_peers(PaxosMsg::PrepareReq(prepreq));
    }
}
