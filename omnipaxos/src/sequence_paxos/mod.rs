#[cfg(feature = "logging")]
use crate::utils::create_logger;
use crate::{
    sequence_paxos::{
        log::MemoryStorage,
        utils::{LeaderState, SlotId},
    },
    utils::{AcceptStatus, Ballot, Entry, EntryId, Mode, NodeId, Phase, Role, SequenceNumber},
    OmniPaxosConfig,
};
#[cfg(feature = "logging")]
use slog::{info, Logger};
use std::{collections::HashMap, fmt::Debug, vec};

mod follower;
mod leader;
mod log;
/// The different messages used by the SequencePaxos layer
pub mod messages;
//mod temp;
mod utils;

use messages::*;
//use util::ReplicatedData;

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for SequencePaxosConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();
        SequencePaxosConfig {
            pid,
            peers,
            buffer_size: config.server_config.buffer_size,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}

/// a Sequence Paxos replica. Maintains local state of the replicated log, handles incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
/// If snapshots are not desired to be used, use `()` for the type parameter `S`.
pub(crate) struct SequencePaxos<T>
where
    T: Entry,
{
    pub(crate) internal_storage: MemoryStorage<T>,
    pid: NodeId,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    // Used to differ between concurrent proposals of the same entry
    entry_id: EntryId,
    // Incoming proposals while node is still in prepare phase
    buffered_proposals: Vec<(EntryId, T)>,
    // Proposals of this node currently in transit
    pending_proposals: HashMap<SlotId, (EntryId, T)>,
    outgoing: Vec<PaxosMessage<T>>,
    leader_state: LeaderState<T>,
    cached_promise_message: Option<Promise<T>>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    // Sequence paxos operating mode (fast or default)
    pub(crate) mode: Mode,
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl<T> SequencePaxos<T>
where
    T: Entry,
{
    /*** User functions ***/
    /// Creates a Sequence Paxos replica.
    pub(crate) fn with(config: SequencePaxosConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let leader = Ballot::default();
        let outgoing = Vec::with_capacity(config.buffer_size);
        let mut paxos = SequencePaxos {
            internal_storage: MemoryStorage::new(),
            pid,
            peers,
            state: (Role::Follower, Phase::None),
            buffered_proposals: vec![],
            pending_proposals: HashMap::new(),
            entry_id: (pid, 0),
            outgoing,
            leader_state: LeaderState::<T>::with(leader, num_nodes),
            cached_promise_message: None,
            current_seq_num: SequenceNumber::default(),
            mode: Mode::OmniPaxos,
            #[cfg(feature = "logging")]
            logger: {
                if let Some(logger) = config.custom_logger {
                    logger
                } else {
                    let s = config
                        .logger_file_path
                        .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                    create_logger(s.as_str())
                }
            },
        };
        paxos.internal_storage.set_promise(leader);
        #[cfg(feature = "logging")]
        {
            info!(paxos.logger, "Paxos component pid: {} created!", pid,);
        }
        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    /// Detects if a message has been sent but not been received.
    pub(crate) fn resend_message_timeout(&mut self) {
        match self.state.0 {
            Role::Leader => self.resend_messages_leader(),
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Clears and returns the outgoing messages.
    pub(crate) fn take_outgoing_messages(&mut self) -> Vec<PaxosMessage<T>> {
        std::mem::take(&mut self.outgoing)
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) {
        let entry_id = self.next_data_id();
        self.try_append((entry_id, entry));
    }

    pub(crate) fn try_append(&mut self, entry: (EntryId, T)) {
        match self.state {
            (Role::Leader, Phase::Accept) => self.op_accept_entry_leader(entry),
            (Role::Follower, Phase::Accept) => match self.mode {
                Mode::OmniPaxos => self.op_forward_proposal(entry),
                Mode::FastPaxos => self.fp_fast_propose(entry),
            },
            _ => self.buffered_proposals.push(entry),
        }
    }

    pub(crate) fn handle_buffered_proposals(&mut self) {
        if !self.buffered_proposals.is_empty() {
            let entries = std::mem::take(&mut self.buffered_proposals);
            for entry in entries {
                self.try_append(entry);
            }
        }
    }

    fn get_current_leader(&self) -> NodeId {
        self.internal_storage.get_promise().pid
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub(crate) fn reconnected(&mut self, pid: NodeId) {
        if pid == self.pid {
            return;
        } else if pid == self.get_current_leader() {
            self.state = (Role::Follower, Phase::Recover);
        }
        let prepreq = PrepareReq {
            n: self.internal_storage.get_promise(),
        };
        self.send_msg_to(pid, PaxosMsg::PrepareReq(prepreq));
    }

    pub(crate) fn op_forward_proposal(&mut self, entry: (EntryId, T)) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entry.clone());
            self.send_msg_to(leader, pf);
        } else {
            self.buffered_proposals.push(entry);
        }
    }

    fn next_data_id(&mut self) -> EntryId {
        self.entry_id.1 += 1;
        self.entry_id
    }

    pub(crate) fn send_msg_to(&mut self, pid: NodeId, msg: PaxosMsg<T>) {
        self.outgoing.push(PaxosMessage {
            from: self.pid,
            to: pid,
            msg,
        });
    }

    pub(crate) fn send_to_all_peers(&mut self, msg: PaxosMsg<T>) {
        for pid in &self.peers {
            let m = PaxosMessage {
                from: self.pid,
                to: *pid,
                msg: msg.clone(),
            };
            self.outgoing.push(m);
        }
    }

    /// Handle an incoming message.
    pub(crate) fn handle(&mut self, m: PaxosMessage<T>) {
        match m.msg {
            // Prepare Phase
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),

            // Accept Phase
            PaxosMsg::ProposalForward(entry) => self.try_append(entry),
            PaxosMsg::Accept(acc) => match acc.accept_status {
                AcceptStatus::FpFastAccepted => self.handle_fast_accept(acc),
                AcceptStatus::OpAccepted | AcceptStatus::FpSlowAccepted => {
                    self.handle_slow_accept(acc)
                }
            },
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),

            // Learn Phase
            PaxosMsg::Decide(d) => self.handle_decide(d),
        }
    }
}
