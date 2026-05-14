#[cfg(feature = "logging")]
use crate::utils::create_logger;
use crate::{
    sequence_paxos::{
        log::MemoryStorage,
        utils::{LeaderState, SlotId},
    },
    utils::{AcceptStatus, Ballot, Entry, Mode, NodeId, Phase, Quorum, Role, SequenceNumber},
    OmniPaxosConfig,
};
#[cfg(feature = "logging")]
use slog::{info, Logger};
use std::{collections::HashMap, fmt::Debug};
#[cfg(feature = "adaptive")]
use std::{
    collections::VecDeque,
    time::{Duration, Instant},
};

mod follower;
mod leader;
mod log;
/// The different messages used by the SequencePaxos layer
pub mod messages;
#[cfg(feature = "adaptive")]
mod predictor;
mod utils;

use messages::*;
#[cfg(feature = "adaptive")]
use predictor::{ConformalModePredictor, Features, Label};

/// Configuration for `SequencePaxos`.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `mode`: Operating mode of sequence_paxos (OmniPaxos or FastPaxos)
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct SequencePaxosConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    buffer_size: usize,
    mode: Mode,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
    #[cfg(feature = "adaptive")]
    significance_level: f64,
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
            mode: config.server_config.mode,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
            #[cfg(feature = "adaptive")]
            significance_level: config.server_config.significance_level,
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
    mode: Mode,
    peers: Vec<NodeId>, // excluding self pid
    state: (Role, Phase),
    outgoing: Vec<PaxosMessage<T>>,
    quorum: Quorum,
    leader_state: LeaderState<T>,
    cached_promise_message: Option<Promise<T>>,
    // Keeps track of sequence of accepts from leader where AcceptSync = 1
    current_seq_num: SequenceNumber,
    // Proposals of this node currently in transit
    #[cfg(not(feature = "adaptive"))]
    pending_proposals: HashMap<SlotId, T>,
    // Proposals currently in transit with current features
    #[cfg(feature = "adaptive")]
    pending_proposals: HashMap<SlotId, (T, Option<Features>)>,
    // Proposals currently in transit testing fast-path success with number of test accepted
    #[cfg(feature = "adaptive")]
    test_proposals: Vec<(SlotId, T, Features, usize)>,
    #[cfg(feature = "adaptive")]
    conformal_mode_predictor: ConformalModePredictor,
    #[cfg(feature = "adaptive")]
    fast_quorum_latency_in_s: Option<f64>,
    #[cfg(feature = "adaptive")]
    incoming_proposals_timestamps: VecDeque<Instant>,
    #[cfg(feature = "adaptive")]
    calibration_data: Vec<(Features, Label)>,
    #[cfg(feature = "adaptive")]
    calibrated: bool,
    #[cfg(feature = "adaptive")]
    calibrated_append_attempts: u64,
    #[cfg(feature = "adaptive")]
    calibrated_fast_path_errors: u64,
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
        let quorum = Quorum::with(num_nodes);
        let leader = Ballot::default();
        let outgoing = Vec::with_capacity(config.buffer_size);

        #[cfg(feature = "logging")]
        let logger = {
            if let Some(log) = config.custom_logger {
                log
            } else {
                let s = config
                    .logger_file_path
                    .clone()
                    .unwrap_or_else(|| format!("logs/paxos_{}.log", pid));
                create_logger(s.as_str())
            }
        };
        let mut paxos = SequencePaxos {
            internal_storage: MemoryStorage::new(),
            pid,
            mode: config.mode,
            peers,
            state: (Role::Follower, Phase::None),
            pending_proposals: HashMap::new(),
            outgoing,
            quorum,
            leader_state: LeaderState::<T>::with(leader, quorum),
            cached_promise_message: None,
            current_seq_num: SequenceNumber::default(),
            #[cfg(feature = "adaptive")]
            test_proposals: Vec::new(),
            #[cfg(feature = "adaptive")]
            conformal_mode_predictor: ConformalModePredictor::new(config.significance_level),
            #[cfg(feature = "adaptive")]
            fast_quorum_latency_in_s: None,
            #[cfg(feature = "adaptive")]
            incoming_proposals_timestamps: VecDeque::with_capacity(50000),
            #[cfg(feature = "adaptive")]
            calibration_data: Vec::with_capacity(10000),
            #[cfg(feature = "adaptive")]
            calibrated: false,
            #[cfg(feature = "adaptive")]
            calibrated_append_attempts: 0,
            #[cfg(feature = "adaptive")]
            calibrated_fast_path_errors: 0,
            #[cfg(feature = "logging")]
            logger,
        };
        paxos.internal_storage.set_promise(leader);
        #[cfg(feature = "logging")]
        {
            info!(
                paxos.logger,
                "Paxos component pid: {} created! Initial Mode: {:?}", pid, config.mode
            );
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
        match self.state {
            (Role::Leader, Phase::Accept) => self.op_accept_entry_leader(entry),
            (Role::Follower, Phase::Accept) => {
                #[cfg(not(feature = "adaptive"))]
                {
                    match self.mode {
                        Mode::OmniPaxos => self.op_forward_proposal(entry),
                        Mode::FastPaxos => self.fp_fast_propose(entry),
                    }
                }
                #[cfg(feature = "adaptive")]
                {
                    if let Some(fql) = self.fast_quorum_latency_in_s {
                        let now = Instant::now();
                        let one_second_ago = now - Duration::from_secs(1);
                        while self
                            .incoming_proposals_timestamps
                            .front()
                            .is_some_and(|&t| t <= one_second_ago)
                        {
                            self.incoming_proposals_timestamps.pop_front();
                        }
                        let other_nodes_proposals_per_s =
                            self.incoming_proposals_timestamps.len() as f64;
                        let features = Features {
                            fast_quorum_latency_in_s: fql,
                            other_nodes_proposals_per_s,
                        };
                        self.mode = self.conformal_mode_predictor.get_new_mode(&features);
                        if self.calibrated {
                            self.calibrated_append_attempts += 1;
                        }
                        match self.mode {
                            Mode::OmniPaxos => self.op_forward_proposal(entry, features),
                            Mode::FastPaxos => self.fp_fast_propose(entry, features),
                        }
                    }
                }
            }
            _ => {}
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

    pub(crate) fn op_forward_proposal(
        &mut self,
        entry: T,
        #[cfg(feature = "adaptive")] features: Features,
    ) {
        let leader = self.get_current_leader();
        if leader > 0 && self.pid != leader {
            #[cfg(feature = "adaptive")]
            {
                // Send test accept to all peers
                if !self.calibrated {
                    let slot_idx = self.internal_storage.get_next_empty_slot();
                    let acc = Accept {
                        n: self.internal_storage.get_promise(),
                        seq_num: SequenceNumber::default(), // not needed for fast path
                        entry: entry.clone(),
                        slot_idx,
                        accept_status: AcceptStatus::TestAccepted,
                    };
                    self.send_to_all_peers(PaxosMsg::Accept(acc));
                    self.test_proposals
                        .push((slot_idx, entry.clone(), features, 1));
                }
            }
            let pf = PaxosMsg::ProposalForward(entry.clone());
            self.send_msg_to(leader, pf);
        } else {
            self.append(entry);
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

    /// Setting latency to reach a fast quorum
    #[cfg(feature = "adaptive")]
    pub(crate) fn set_fast_quorum_latency(&mut self, latency: Option<f64>) {
        self.fast_quorum_latency_in_s = latency;
    }

    #[cfg(feature = "adaptive")]
    pub(crate) fn get_fast_path_success_rate(&self) -> Option<f64> {
        if self.calibrated_append_attempts != 0 {
            let error_rate =
                self.calibrated_fast_path_errors as f64 / self.calibrated_append_attempts as f64;
            #[cfg(feature = "logging")]
            info!(
                self.logger,
                "Node {} has a fast-path error rate of {} from {} attempts",
                self.pid,
                error_rate,
                self.calibrated_append_attempts
            );
            Some(error_rate)
        } else {
            None
        }
    }

    /// Calibrate
    #[cfg(feature = "adaptive")]
    pub(crate) fn calibrate(&mut self, end: bool) {
        if self.calibrated {
            #[cfg(feature = "logging")]
            slog::warn!(self.logger, "Already successfully calibrated; skipping.");
            return;
        }
        match self
            .conformal_mode_predictor
            .calibrate(&self.calibration_data)
        {
            Ok((_l_hat, _emp_risk)) => {
                #[cfg(feature = "logging")]
                slog::info!(
                    self.logger,
                    "Node {}: Calibration successful on calibration set of size {}! Lambda: {}. Empirical risk on calibration data: {}.",
                    self.pid,
                    self.calibration_data.len(),
                    _l_hat,
                    _emp_risk
                );
                self.calibration_data.clear();
                self.calibrated = end;
            }
            Err(_e) => {
                #[cfg(feature = "logging")]
                slog::warn!(self.logger, "Node {}: Calibration failed: {}", self.pid, _e);
            }
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
            PaxosMsg::ProposalForward(entry) => self.append(entry),
            PaxosMsg::Accept(acc) => {
                #[cfg(feature = "adaptive")]
                {
                    // Track proposals from other nodes
                    if acc.accept_status == AcceptStatus::FpFastAccepted
                        || acc.accept_status == AcceptStatus::OpAccepted
                    {
                        self.incoming_proposals_timestamps.push_back(Instant::now());
                    }
                }
                match acc.accept_status {
                    AcceptStatus::FpFastAccepted => self.handle_fast_accept(acc),
                    AcceptStatus::OpAccepted => self.handle_omnipaxos_accept(acc),
                    AcceptStatus::FpSlowAccepted => self.handle_slow_accept(acc),
                    AcceptStatus::TestAccepted => self.handle_test_accept(acc, m.from),
                }
            }
            PaxosMsg::Accepted(accepted) => self.handle_accepted(accepted, m.from),
            PaxosMsg::NotAccepted(not_acc) => self.handle_notaccepted(not_acc, m.from),

            // Learn Phase
            PaxosMsg::Decide(d) => self.handle_decide(d),
        }
    }
}
