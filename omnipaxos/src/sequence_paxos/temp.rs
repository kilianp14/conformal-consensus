use super::{leader::LeaderState, messages::*, SequencePaxos};
use crate::{
    storage::{Entry, Storage},
    utils::{Ballot, Mode, NodeId, Phase, Role, WRITE_ERROR_MSG},
};
#[cfg(feature = "logging")]
use slog::{trace, warn};
use std::collections::{HashMap, HashSet};

impl<T, B> SequencePaxos<T, B>
where
    T: Entry,
    B: Storage<T>,
{
    pub(crate) fn replicate_and_create_slot_vote_if_needed(
        &mut self,
        data_id: DataId,
        entry: T,
        is_proposer: bool,
    ) -> Option<SlotVote> {
        match self.mode {
            Mode::FastPaxos => {
                if is_proposer {
                    let pm = PaxosMsg::Replicate(Replicate {
                        data_id,
                        data: entry.clone(),
                    });
                    self.send_to_all_peers(pm);
                }
                let sv = self.replicate_data(data_id, entry);
                sv
            }
            Mode::OmniPaxos => match self.state {
                (Role::Leader, Phase::Accept) => self.replicate_data(data_id, entry),
                (Role::Follower, Phase::Accept) => {
                    self.forward_proposals(vec![entry]);
                    None
                }
                _ => {
                    self.buffered_proposals.push(entry);
                    None
                }
            },
        }
    }

    pub(crate) fn replicate_data(&mut self, data_id: DataId, entry: T) -> Option<SlotVote> {
        match self.replicated_data.get_mut(&data_id) {
            Some(d) => {
                // data_id has been handled before
                let Data { data, status } = d;
                match status {
                    DataStatus::DecidedWithSlot(slot_idx) if data.is_none() => {
                        *data = Some(entry);
                        #[cfg(feature = "logging")]
                        trace!(self.logger, "Completed decided slot: {}", slot_idx);
                        self.slot_status
                            .insert(*slot_idx, SlotStatus::Completed(data_id));
                        self.append_completed_ents();
                    }
                    DataStatus::Completed => {
                        assert!(!data.is_none(), "Cannot be completed if data is none!");
                        // ignore
                    }
                    _ => {
                        if data.is_none() {
                            *data = Some(entry);
                        }
                    }
                }
                None
            }
            None => {
                // first time handling data
                let sv = self.create_slot_vote();
                let status = match self.state.0 {
                    Role::Leader => {
                        let psf = PossibleFastSlots::new();
                        // psf.add_slot(sv.into());
                        DataStatus::ReplicateAcks(psf)
                    }
                    Role::Follower => DataStatus::Acked,
                };
                self.replicated_data.insert(
                    data_id,
                    Data {
                        data: Some(entry),
                        status,
                    },
                );
                Some(sv)
            }
        }
    }

    fn get_next_slot_idx(&mut self) -> SlotIdx {
        let idx = self.fastpaxos_next_log_idx;
        self.fastpaxos_next_log_idx += 1;
        idx
    }

    fn get_next_test_slot_idx(&mut self) -> SlotIdx {
        let idx = self.testvoter_next_log_idx;
        self.testvoter_next_log_idx += 1;
        idx
    }

    fn create_slot_vote(&mut self) -> SlotVote {
        SlotVote::Real(self.get_next_slot_idx())
    }

    pub fn handle_replicate(&mut self, r: Replicate<T>, is_proposer: bool) {
        let sv: Option<SlotVote> =
            self.replicate_and_create_slot_vote_if_needed(r.data_id, r.data, is_proposer);
        if let Some(slot_vote) = sv {
            #[cfg(feature = "logging")]
            {
                let x: SlotIdx = slot_vote.into();
                if self.pid == LOG_NODE && x < 100 && x > 40 {
                    trace!(
                        self.logger,
                        "Node {}: replicating slot: {:?}, data: {:?}",
                        self.pid,
                        x,
                        r.data_id
                    );
                }
            }
            let n = self.get_promise();
            let proposal = Proposal {
                n,
                data_id: r.data_id,
                version: 0,
            };
            let ra = ReplicateAck {
                proposal,
                slot_vote,
                from: self.pid,
            };
            if self.pid == n.pid {
                self.handle_replicate_ack(ra);
            } else {
                self.send_msg_to(n.pid, PaxosMsg::ReplicateAck(ra));
            }
        }
    }

    fn perform_slow_path(
        &mut self,
        proposal: Proposal,
        data: Option<T>,
        slot_idx: Option<SlotIdx>,
    ) {
        let slot_idx = slot_idx.unwrap_or_else(|| self.get_next_slot_idx());
        #[cfg(feature = "logging")]
        {
            match &self.replicated_data.get(&proposal.data_id).unwrap().status {
                DataStatus::ReplicateAcks(_) => {}
                e => panic!("Expected ReplicateAcks, got: {:?}", e),
            };
            trace!(
                self.logger,
                "Slow path for slot {}: {:?}",
                slot_idx,
                proposal.data_id
            );
        }
        self.slot_status
            .insert(slot_idx, SlotStatus::SlowAcks(proposal, 1));
        let mut data_status = &mut self
            .replicated_data
            .get_mut(&proposal.data_id)
            .unwrap()
            .status;
        *data_status = DataStatus::SlowPathWithSlot(slot_idx);
        #[cfg(feature = "logging")]
        {
            if self.pid == LOG_NODE && slot_idx % 1 == 0 {
                trace!(
                    self.logger,
                    "Slow path for slot {}: {:?}",
                    slot_idx,
                    proposal.data_id
                );
            }
        }
        let ao = AcceptOrder {
            proposal,
            slot_idx,
            data,
        };
        self.buffered_data_ids.remove(&proposal.data_id);
        self.send_to_all_promised_followers(PaxosMsg::AcceptOrder(ao));
    }

    pub fn handle_replicate_ack(&mut self, ra: ReplicateAck) {
        let ReplicateAck {
            proposal,
            slot_vote,
            from,
        } = ra;
        // #[cfg(feature = "logging")]
        // trace!(self.logger, "Node {}: {:?}", self.pid, ra);
        if proposal.n != self.get_promise() {
            return;
        }
        match slot_vote {
            SlotVote::Real(slot_idx) => {
                // TODO need to make idempotent
                self.replicated_data
                    .get_mut(&proposal.data_id)
                    .map(|d| match &mut d.status {
                        DataStatus::ReplicateAcks(possible_fast_slots) => {
                            possible_fast_slots.add_slot(slot_idx);
                        }
                        _ => {}
                    });
                match self.slot_status.get_mut(&slot_idx) {
                    None => {
                        // first time handling replicate_ack for this slot
                        let slot_status = match self.mode_changer.current_mode {
                            Mode::FastPaxos => {
                                SlotStatus::FastVotes(Proposals::initialize_with(proposal, from))
                            }
                            Mode::OmniPaxos => {
                                let data = self
                                    .replicated_data
                                    .get(&proposal.data_id)
                                    .map(|d| d.data.clone().unwrap())
                                    .unwrap();
                                self.perform_slow_path(proposal, Some(data), Some(slot_idx));
                                return;
                            }
                        };
                        self.slot_status.insert(slot_idx, slot_status);
                        return;
                    }
                    Some(SlotStatus::FastVotes(votes)) => {
                        votes.add_proposal(proposal, from);
                        let pr = votes.check_result::<T>(self.quorum_size, self.super_quorum_size);
                        self.handle_votes_result(slot_idx, pr);
                    }
                    Some(SlotStatus::SlowAcks(current_proposal, acks)) => {
                        if current_proposal == &proposal {
                            *acks += 1;
                            // #[cfg(feature = "logging")]
                            // trace!(self.logger, "Node {}. SLOW SLOT {}: num_acks={:?}", self.pid, slot_idx, acks);
                            if &acks == &&self.quorum_size {
                                let ds = DecidedSlot {
                                    slot_idx,
                                    data_id: current_proposal.data_id,
                                };
                                self.handle_decidedslot(ds);
                                self.send_to_all_promised_followers(PaxosMsg::DecidedSlot(ds));
                                return;
                            }
                        } else if (current_proposal.n, current_proposal.version)
                            == (proposal.n, proposal.version)
                        {
                            // some voted in fast, some in slow meaning this was during mode switch. Treat as fast votes and it will take the slow path
                            self.slot_status.insert(
                                slot_idx,
                                SlotStatus::FastVotes(Proposals::initialize_with(proposal, from)),
                            );
                        } else {
                            let ao = AcceptOrder {
                                proposal: *current_proposal,
                                slot_idx,
                                data: None,
                            };
                            // #[cfg(feature = "logging")]
                            // trace!(self.logger, "Resending AO for slot {}: {:?}", slot_idx, ao);
                            self.send_msg_to(ra.from, PaxosMsg::AcceptOrder(ao));
                        }
                    }
                    Some(SlotStatus::Decided(_)) | Some(SlotStatus::Completed(_)) => {
                        // ignore
                        return;
                    }
                    _ => {
                        unimplemented!(
                            "Should not receive ReplicateAck during recovery or after voting"
                        )
                    }
                }
            }
        }
    }

    fn handle_votes_result(&mut self, slot_idx: usize, pr: ProposalResult) {
        match pr {
            ProposalResult::FastPath(p) => {
                if p.n == self.get_promise() {
                    self.mode_changer.increment_fast_paths();
                    let ds = DecidedSlot {
                        slot_idx,
                        data_id: p.data_id,
                    };
                    #[cfg(feature = "logging")]
                    {
                        let data = self.replicated_data.get(&p.data_id);
                        if self.pid == LOG_NODE {
                            trace!(
                                self.logger,
                                "Node {}: SLOT {} DECIDED WITH FAST PATH: {:?}",
                                self.pid,
                                slot_idx,
                                p.data_id
                            );
                        }
                    }
                    self.handle_decidedslot(ds);
                    self.send_to_all_promised_followers(PaxosMsg::DecidedSlot(ds));
                } else {
                    todo!("Highest vote was not my ballot. Clear this slot?")
                }
            }
            ProposalResult::SlowPath(proposals) => {
                let mut max_v = 0;
                let mut eligible = None;
                for p in &proposals {
                    max_v = max_v.max(p.version);
                    if eligible.is_none() {
                        if self.check_eligible_slow_value(p.data_id) {
                            eligible = Some(p.data_id);
                        } else {
                            self.buffered_data_ids.insert(p.data_id);
                        }
                    } else {
                        self.buffered_data_ids.insert(p.data_id);
                    }
                }
                let data_id = match eligible {
                    Some(data_id) => data_id,
                    None => {
                        let d = self
                            .buffered_data_ids
                            .iter()
                            .find(|data_id| self.check_eligible_slow_value(**data_id))
                            .map(|x| *x);
                        match d {
                            Some(data) => data,
                            None => {
                                return;
                            }
                        }
                    }
                };
                self.mode_changer.increment_slow_paths();
                let proposal = Proposal {
                    n: self.get_promise(),
                    data_id,
                    version: max_v + 1,
                };
                self.perform_slow_path(proposal, None, Some(slot_idx));
            }
            ProposalResult::Pending => {}
        }
    }

    pub fn handle_acceptorder(&mut self, ao: AcceptOrder<T>) {
        if ao.proposal.n != self.get_promise() {
            return;
        }
        match self.slot_status.get(&ao.slot_idx) {
            None => {
                let ra = ReplicateAck {
                    proposal: ao.proposal,
                    slot_vote: SlotVote::Real(ao.slot_idx),
                    from: self.pid,
                };
                if !self.replicated_data.contains_key(&ao.proposal.data_id) {
                    #[cfg(feature = "logging")]
                    {
                        if ao.data.is_none() {
                            warn!(
                                self.logger,
                                "Slot {}: Accepting without entry for data: {:?}",
                                ao.slot_idx,
                                ao.proposal.data_id
                            );
                        }
                    }
                    self.replicated_data.insert(
                        ao.proposal.data_id,
                        Data {
                            data: ao.data,
                            status: DataStatus::Acked,
                        },
                    );
                }
                self.send_msg_to(ao.proposal.n.pid, PaxosMsg::ReplicateAck(ra));
            }
            Some(SlotStatus::Voted(p)) if p < &ao.proposal => {
                match self.replicated_data.get_mut(&ao.proposal.data_id) {
                    Some(d) => {
                        d.status = DataStatus::Acked;
                        if ao.data.is_some() {
                            d.data = ao.data;
                        }
                    }
                    None => {
                        #[cfg(feature = "logging")]
                        {
                            if ao.data.is_none() {
                                warn!(
                                    self.logger,
                                    "Slot {}: Overwriting without entry for data: {:?}",
                                    ao.slot_idx,
                                    ao.proposal.data_id
                                );
                            }
                        }
                        self.replicated_data.insert(
                            ao.proposal.data_id,
                            Data {
                                data: ao.data,
                                status: DataStatus::Acked,
                            },
                        );
                    }
                }
                self.vote_slowpath(ao.proposal, ao.slot_idx);
            }
            Some(SlotStatus::Decided(d)) => {
                unimplemented!(
                    "Acceptorder for decided slot {}: {:?}",
                    ao.slot_idx,
                    ao.proposal.data_id
                )
            }
            _ => {
                return; // ignore
            }
        }
        self.slot_status
            .insert(ao.slot_idx, SlotStatus::Voted(ao.proposal));
    }

    fn vote_slowpath(&mut self, proposal: Proposal, slot_idx: SlotIdx) {
        self.slot_status
            .insert(slot_idx, SlotStatus::Voted(proposal));
        let ra = ReplicateAck {
            proposal,
            slot_vote: SlotVote::Real(slot_idx),
            from: self.pid,
        };
        self.send_msg_to(proposal.n.pid, PaxosMsg::ReplicateAck(ra));
    }

    pub fn handle_decidedslot(&mut self, ds: DecidedSlot) {
        let DecidedSlot { slot_idx, data_id } = ds;
        match self.slot_status.get(&slot_idx) {
            Some(SlotStatus::Decided(_)) | Some(SlotStatus::Completed(_)) => {
                // ignore
                return;
            }
            _ => {}
        }
        let (status, is_completed) = match self.replicated_data.get(&data_id) {
            Some(d) if d.data.is_some() => (SlotStatus::Completed(data_id), true),
            _ => (SlotStatus::Decided(data_id), false),
        };
        #[cfg(feature = "logging")]
        if self.pid == LOG_NODE {
            trace!(
                self.logger,
                "Node {}: decided slot {}: {:?}",
                self.pid,
                slot_idx,
                data_id
            );
        }
        self.replicated_data.set_decided_slot(&data_id, slot_idx);
        self.buffered_data_ids.remove(&data_id);
        self.slot_status.insert(slot_idx, status);
        if is_completed {
            self.append_completed_ents();
            // self.take_and_append_completed_slots();
        }
    }

    pub(crate) fn append_completed_ents(&mut self) {
        let old_decided_idx = self.get_decided_idx();
        let mut i = old_decided_idx;
        let mut completed_idx = old_decided_idx;
        let mut completed_entries = vec![];
        let mut in_completed_sequence = true;
        loop {
            match self.slot_status.get(&i) {
                Some(SlotStatus::Completed(data_id)) if in_completed_sequence => {
                    let data = self.replicated_data
                        .complete_and_take_decided_data(data_id)
                        .unwrap_or_else(|| {
                            panic!(
                                "PID {}: Expected slot {} data with id: {:?}. Old_decided_idx: {}, data_status: {:?}",
                                // "PID {}: Expected slot {} data with id: {:?}. Old_decided_idx: {}, replicated_data: {:?}",
                                self.pid, i, data_id, old_decided_idx, self.replicated_data.get(&data_id).unwrap().status
                                // self.replicated_data
                            )
                        });
                    #[cfg(feature = "logging")]
                    {
                        if self.pid == LOG_NODE && i % 10 == 0 {
                            trace!(
                                self.logger,
                                "Node {} COMPLETED slot: {} with data: {:?}",
                                self.pid,
                                i,
                                data_id
                            );
                        }
                    }
                    completed_entries.push(data);
                    completed_idx = i + 1;
                    i += 1;
                    self.buffered_data_ids.remove(&data_id);
                }
                Some(SlotStatus::FastVotes(x)) => {
                    in_completed_sequence = false;
                    let r = x.check_result::<T>(self.quorum_size, self.super_quorum_size);
                    self.handle_votes_result(i, r);
                    i += 1;
                }
                e => {
                    // #[cfg(feature = "logging")]
                    // {
                    //     if self.pid == LOG_NODE {
                    //         trace!(self.logger, "Found gap at slot {}: {:?}. IC: {}. Completed_idx: {}", i, e, in_completed_sequence, completed_idx);
                    //     }
                    // }
                    break;
                }
            }
        }
        if !completed_entries.is_empty() {
            assert!(completed_idx > old_decided_idx);
            self.internal_storage
                .append_entries_without_batching(completed_entries)
                .expect(WRITE_ERROR_MSG);
            self.internal_storage
                .set_decided_idx(completed_idx)
                .unwrap();
        }
    }

    fn check_eligible_slow_value(&self, data_id: DataId) -> bool {
        match self.replicated_data.get(&data_id) {
            Some(d) => match &d.status {
                DataStatus::ReplicateAcks(pfs) => {
                    let r = pfs.eligible_for_slowpath(
                        self.quorum_size,
                        self.super_quorum_size,
                        &self.slot_status,
                    );
                    #[cfg(feature = "logging")]
                    {
                        if self.pid == LOG_NODE && !r && data_id.1 % 1 == 0 && data_id.1 > 1200 {
                            trace!(self.logger, "Not Eligible data: {:?}, {:?}", data_id, pfs,);
                        }
                    }
                    return r;
                }
                _ => {}
            },
            None => {}
        }
        false
    }
}

impl<T: Entry> LeaderState<T> {
    /// Returns the slots that need to be appended to the log
    pub fn get_recovered_slots(&mut self) -> HashMap<usize, DataId> {
        let mut slots = HashMap::with_capacity(self.promises_meta.len());
        for (idx, ps) in self.promises_meta.iter_mut().enumerate() {
            match ps {
                PromiseState::PreparePromised(p) => {
                    let pending_slots = std::mem::take(&mut p.pending_slots);
                    for p in pending_slots {
                        if p.decided {
                            slots.insert(p.idx, SlotStatus::Decided(p.proposal.data_id));
                        } else {
                            match slots.get_mut(&p.idx) {
                                Some(SlotStatus::Recovery(ps)) => {
                                    ps.add_proposal(p.proposal, idx_to_pid(idx));
                                }
                                Some(SlotStatus::Decided(_)) => {}
                                None => {
                                    let pid = idx_to_pid(idx);
                                    let votes = Proposals::initialize_with(p.proposal, pid);
                                    slots.insert(p.idx, SlotStatus::Recovery(votes));
                                }
                                _ => {
                                    unimplemented!()
                                }
                            }
                        }
                    }
                }
                _ => {}
            }
        }
        slots
            .into_iter()
            .map(|(idx, s)| {
                let data_id: DataId = match s {
                    SlotStatus::Decided(data_id) => data_id,
                    SlotStatus::Recovery(proposals) => proposals.get_recovery_result(),
                    _ => unimplemented!(),
                };
                (idx, data_id)
            })
            .collect()
    }
}
