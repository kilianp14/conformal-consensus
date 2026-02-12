#[cfg(feature = "logging")]
use crate::utils::create_logger;
use crate::{
    sequence_paxos::log::MemoryStorage,
    utils::{
        defaults::DEFAULT_MODE, Ballot, Entry, FlexibleQuorum, LogSync, Mode, NodeId, Phase,
        Quorum, Role, SequenceNumber,
    },
    OmniPaxosConfig,
};
#[cfg(feature = "logging")]
use slog::{info, Logger};
use std::{fmt::Debug, vec};

mod follower;
mod leader;
mod log;
/// The different messages used by the SequencePaxos layer
pub mod messages;
//mod temp;
//mod util;

pub(crate) use leader::LeaderState;
use messages::*;
//use util::ReplicatedData;

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `flexible_quorum` : Defines read and write quorum sizes. Can be used for different latency vs fault tolerance tradeoffs.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `batch_size`: The size of the buffer for log batching. The default is 1, which means no batching.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    flexible_quorum: Option<FlexibleQuorum>,
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
            flexible_quorum: config.cluster_config.flexible_quorum,
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
    buffered_proposals: Vec<T>,
    outgoing: Vec<PaxosMessage<T>>,
    leader_state: LeaderState<T>,
    latest_accepted_meta: Option<(Ballot, usize)>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    //replicated_data: ReplicatedData<T>,
    cached_promise_message: Option<Promise<T>>,
    quorum_size: usize,
    super_quorum_size: usize,
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
        let quorum = Quorum::with(config.flexible_quorum, num_nodes);
        let quorum_size = match quorum {
            Quorum::Majority(m) => m,
            Quorum::Flexible(_) => unimplemented!(),
        };
        let super_quorum_size = {
            let sq = (num_nodes * 3).div_ceil(4);
            std::cmp::min(num_nodes, sq)
        };
        let leader = Ballot::default();
        assert!(
            quorum_size < super_quorum_size,
            "Quorum size: {} must be less than super quorum size: {}. N: {}",
            quorum_size,
            super_quorum_size,
            num_nodes
        );
        let max_peer_pid = peers.iter().max().unwrap();
        let max_pid = *std::cmp::max(max_peer_pid, &pid) as usize;
        let outgoing = Vec::with_capacity(config.buffer_size);
        let mode = DEFAULT_MODE;
        let mut paxos = SequencePaxos {
            internal_storage: MemoryStorage::new(),
            pid,
            peers,
            state: (Role::Follower, Phase::None),
            buffered_proposals: vec![],
            outgoing,
            leader_state: LeaderState::<T>::with(leader, max_pid, quorum),
            latest_accepted_meta: None,
            current_seq_num: SequenceNumber::default(),
            cached_promise_message: None,
            //replicated_data: ReplicatedData::<T>::with_capacity(10000),
            quorum_size,
            super_quorum_size,
            mode,
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
            info!(
                paxos.logger,
                "Paxos component pid: {} created!. Q: {}, SQ: {}",
                pid,
                quorum_size,
                super_quorum_size
            );
        }
        paxos
    }

    pub(crate) fn get_state(&self) -> &(Role, Phase) {
        &self.state
    }

    /// Detects if a Prepare, Promise, AcceptStopSign, Decide of a Stopsign, or PrepareReq message
    /// has been sent but not been received. If so resends them. Note: We can't detect if a
    /// StopSign's Decide message has been received so we always resend to be safe.
    pub(crate) fn resend_message_timeout(&mut self) {
        match self.state.0 {
            Role::Leader => self.resend_messages_leader(),
            Role::Follower => self.resend_messages_follower(),
        }
    }

    /// Clears and returns the outgoing messages.
    pub(crate) fn take_outgoing_messages(&mut self) -> Vec<PaxosMessage<T>> {
        let msgs = std::mem::take(&mut self.outgoing);
        self.leader_state.reset_latest_accept_meta();
        self.latest_accepted_meta = None;
        msgs
    }

    /// Append an entry to the replicated log.
    pub(crate) fn append(&mut self, entry: T) {
        match self.state {
            (Role::Leader, Phase::Prepare) => self.buffered_proposals.push(entry),
            (Role::Leader, Phase::Accept) => match self.mode {
                Mode::OmniPaxos => self.accept_entry_leader(entry),
                Mode::FastPaxos => self.fast_propose(entry),
            },
            (Role::Follower, Phase::Accept) => match self.mode {
                Mode::OmniPaxos => self.forward_proposals(vec![entry]),
                Mode::FastPaxos => self.fast_propose(entry),
            },
            _ => self.forward_proposals(vec![entry]),
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

    pub(crate) fn forward_proposals(&mut self, mut entries: Vec<T>) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            let pf = PaxosMsg::ProposalForward(entries);
            self.send_msg_to(leader, pf);
        } else {
            self.buffered_proposals.append(&mut entries);
        }
    }

    /// Returns `LogSync`, a struct to help other servers synchronize their log to correspond to the
    /// current state of our own log. The `common_prefix_idx` marks where in the log the other server
    /// needs to be sync from.
    fn create_log_sync(&self, common_prefix_idx: usize) -> LogSync<T> {
        LogSync {
            suffix: self.internal_storage.get_suffix(common_prefix_idx),
            sync_idx: common_prefix_idx,
        }
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
            PaxosMsg::PrepareReq(prepreq) => self.handle_preparereq(prepreq, m.from),
            PaxosMsg::Prepare(prep) => self.handle_prepare(prep, m.from),
            PaxosMsg::Promise(prom) => match &self.state {
                (Role::Leader, Phase::Prepare) => self.handle_promise_prepare(prom, m.from),
                (Role::Leader, Phase::Accept) => self.handle_promise_accept(prom, m.from),
                _ => {}
            },
            PaxosMsg::AcceptSync(acc_sync) => self.handle_acceptsync(acc_sync, m.from),
            PaxosMsg::SlowAccept(slow_acc) => self.handle_slow_accept(slow_acc),
            PaxosMsg::FastAccept(fast_acc) => self.handle_fast_accept(fast_acc),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::Decide(d) => self.handle_decide(d),
            PaxosMsg::ProposalForward(proposals) => self.handle_forwarded_proposal(proposals),
        }
    }
}
