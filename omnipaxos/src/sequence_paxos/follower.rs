use crate::{
    sequence_paxos::{messages::*, utils::LogSync, Promise, SequencePaxos},
    utils::{
        Ballot, Entry, LogEntry, MessageStatus, NodeId, Phase, Role, SequenceNumber, SlotStatus,
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
            self.send_msg_to(from, PaxosMsg::Promise(promise));
            #[cfg(feature = "logging")]
            info!(self.logger, "Pid: {} promising {:?}", self.pid, prep.n);
        }
    }

    pub(crate) fn handle_acceptsync(&mut self, accsync: AcceptSync<T>, from: NodeId) {
        if self.check_valid_ballot(accsync.n) && self.state == (Role::Follower, Phase::Prepare) {
            #[cfg(feature = "logging")]
            {
                let (r, p) = &self.state;
                debug!(
                    self.logger,
                    "Self role {:?}, phase {:?}. Incoming Accept Sync from {:?}: {:?}",
                    r,
                    p,
                    from,
                    accsync
                );
            }
            self.state = (Role::Follower, Phase::Accept);
            self.current_seq_num = accsync.seq_num;
            self.internal_storage.set_decided_idx(accsync.decided_idx);
            self.internal_storage.set_accepted_round(accsync.n);
            self.internal_storage
                .append_suffix(accsync.log_sync.suffix, accsync.log_sync.sync_idx);

            // Accept all the undecided slots after the decided index
            let mut slot_idx = accsync.decided_idx;
            for log_entry in self.internal_storage.get_suffix(slot_idx) {
                if let LogEntry::Undecided(entry_id, entry, status) = log_entry {
                    match status {
                        SlotStatus::OpAccepted => {
                            let accepted = Accepted {
                                n: accsync.n,
                                slot_idx,
                            };
                            self.send_msg_to(from, PaxosMsg::OpAccepted(accepted))
                        }
                        SlotStatus::FpFastAccepted => {
                            let fast_accepted = FpFastAccepted {
                                n: accsync.n,
                                entry: (entry_id, entry),
                                slot_idx,
                            };
                            self.send_msg_to(from, PaxosMsg::FpFastAccepted(fast_accepted))
                        }
                        SlotStatus::FpSlowAccepted => {
                            let accepted = Accepted {
                                n: accsync.n,
                                slot_idx,
                            };
                            self.send_msg_to(from, PaxosMsg::FpSlowAccepted(accepted))
                        }
                    }
                }
                slot_idx += 1;
            }
            self.handle_buffered_proposals();
        }
    }

    fn forward_buffered_proposals(&mut self) {
        let proposals = std::mem::take(&mut self.buffered_proposals);
        for proposal in proposals {
            self.forward_proposal(proposal);
        }
    }

    pub(crate) fn handle_op_accept(&mut self, op_acc: Accept<T>) {
        if self.check_valid_ballot(op_acc.n)
            && self.state == (Role::Follower, Phase::Accept)
            && self.handle_sequence_num(op_acc.seq_num, op_acc.n.pid) == MessageStatus::Expected
        {
            #[cfg(feature = "logging")]
            {
                let (r, p) = &self.state;
                debug!(
                    self.logger,
                    "Self role {:?}, phase {:?}. Incoming Slow Accept from {:?}: {:?}",
                    r,
                    p,
                    op_acc.n.pid,
                    op_acc
                );
            }
            self.internal_storage
                .insert_at_index(op_acc.accepted_idx, op_acc.entry);
            let accepted = Accepted {
                n: op_acc.n,
                accepted_idx: op_acc.accepted_idx,
            };
            self.send_msg_to(op_acc.n.pid, PaxosMsg::OpAccepted(accepted));
        }
    }

    pub(crate) fn handle_fp_propose(&mut self, fast_proposal: FpPropose<T>) {
        // TODO
        #[cfg(feature = "logging")]
        {
            let (r, p) = &self.state;
            debug!(
                self.logger,
                "Self role {:?}, phase {:?}. Incoming fast proposal: {:?}", r, p, fast_proposal
            );
        }
        if self.state.1 == Phase::Accept {}
    }

    pub(crate) fn handle_fp_slow_accept(&mut self, slow_acc: Accept<T>) {
        // TODO
    }

    pub(crate) fn handle_decide(&mut self, dec: Decide) {
        if self.check_valid_ballot(dec.n)
            && self.state.1 == Phase::Accept
            && self.handle_sequence_num(dec.seq_num, dec.n.pid) == MessageStatus::Expected
        {
            #[cfg(feature = "logging")]
            {
                let (r, p) = &self.state;
                debug!(
                    self.logger,
                    "Self role {:?}, phase {:?}. Incoming Decide from {:?}: {:?}",
                    r,
                    p,
                    dec.n.pid,
                    dec
                );
            }
            if dec.decided_idx > self.internal_storage.get_decided_idx() {
                self.internal_storage.set_decided_idx(dec.decided_idx);
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
                // Should never happen, but to be safe send PrepareReq
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
