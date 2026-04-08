#[cfg(feature = "adaptive")]
use std::{
    collections::{HashMap, VecDeque},
    time::{SystemTime, UNIX_EPOCH},
};

/// Ballot Leader Election algorithm for electing new leaders
use crate::utils::{defaults::*, Quorum};

#[cfg(feature = "logging")]
use crate::utils::create_logger;
use crate::{
    utils::{Ballot, NodeId},
    OmniPaxosConfig,
};
#[cfg(feature = "logging")]
use slog::{info, trace, Logger};

/// The different messages used by the BallotLeaderElection layer
pub mod messages;
use messages::*;

/// A Ballot Leader Election component. Used in conjunction with OmniPaxos to handle the election of a leader for a cluster of OmniPaxos servers,
/// incoming messages and produces outgoing messages that the user has to fetch periodically and send using a network implementation.
/// User also has to periodically fetch the decided entries that are guaranteed to be strongly consistent and linearizable, and therefore also safe to be used in the higher level application.
pub(crate) struct BallotLeaderElection {
    /// Process identifier used to uniquely identify this instance.
    pid: NodeId,
    /// Vector that holds the pids of all the other servers.
    peers: Vec<NodeId>,
    /// The current round of the heartbeat cycle.
    hb_round: u32,
    /// The heartbeat replies this instance received during the current round.
    heartbeat_replies: Vec<HeartbeatReply>,
    /// Vector that holds all the received heartbeats from the previous heartbeat round, including the current node. Only used to display the connectivity of this node in the UI.
    /// Represents nodes that are currently alive from the view of the current node.
    prev_replies: Vec<HeartbeatReply>,
    /// Holds the current ballot of this instance.
    current_ballot: Ballot,
    /// The current leader of this instance.
    leader: Ballot,
    /// A happy node either sees that it is, is connected to, or sees evidence of a potential leader
    /// for the cluster. If a node is unhappy then it is seeking a new leader.
    happy: bool,
    /// The number of replicas inside the cluster whose heartbeats are needed to become and remain the leader.
    quorum: Quorum,
    /// Vector which holds all the outgoing messages of the BLE instance.
    outgoing: Vec<BLEMessage>,
    /// Per-node rolling window of one-way latencies (RTT / 2) in microseconds.
    #[cfg(feature = "adaptive")]
    latency_histories: HashMap<NodeId, VecDeque<f64>>,
    /// Maximum size of the rolling window.
    #[cfg(feature = "adaptive")]
    latency_window_size: usize,
    /// Logger used to output the status of the component.
    #[cfg(feature = "logging")]
    logger: Logger,
}

impl BallotLeaderElection {
    /// Construct a new BallotLeaderElection node
    pub(crate) fn with(config: BLEConfig) -> Self {
        let pid = config.pid;
        let peers = config.peers;
        let num_nodes = &peers.len() + 1;
        let quorum = Quorum::with(num_nodes);
        let initial_ballot = Ballot::with(1, config.priority, pid);
        let initial_leader = initial_ballot;
        #[cfg(feature = "adaptive")]
        let latency_histories = peers
            .iter()
            .map(|&id| (id, VecDeque::with_capacity(config.latency_window_size)))
            .collect();
        let mut ble = BallotLeaderElection {
            pid,
            peers,
            hb_round: 0,
            heartbeat_replies: Vec::with_capacity(num_nodes),
            prev_replies: Vec::with_capacity(num_nodes),
            current_ballot: initial_ballot,
            leader: initial_leader,
            happy: true,
            quorum,
            outgoing: Vec::with_capacity(config.buffer_size),
            #[cfg(feature = "adaptive")]
            latency_histories,
            #[cfg(feature = "adaptive")]
            latency_window_size: config.latency_window_size,
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
        #[cfg(feature = "logging")]
        {
            info!(
                ble.logger,
                "Ballot Leader Election component pid: {} created!", pid
            );
        }
        ble.new_hb_round();
        ble
    }

    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub(crate) fn set_priority(&mut self, p: u32) {
        self.current_ballot.priority = p;
    }

    /// Clears and returns the outgoing messages.
    pub(crate) fn take_outgoing_messages(&mut self) -> Vec<BLEMessage> {
        std::mem::take(&mut self.outgoing)
    }

    /// Handle an incoming message.
    /// # Arguments
    /// * `m` - the message to be handled.
    pub(crate) fn handle(&mut self, m: BLEMessage) {
        match m.msg {
            HeartbeatMsg::Request(req) => self.handle_request(m.from, req),
            HeartbeatMsg::Reply(rep) => self.handle_reply(m.from, rep),
        }
    }

    /// Initiates a new heartbeat round.
    fn new_hb_round(&mut self) {
        self.prev_replies = std::mem::take(&mut self.heartbeat_replies);
        self.hb_round += 1;
        #[cfg(feature = "logging")]
        trace!(
            self.logger,
            "Initiate new heartbeat round: {}",
            self.hb_round
        );
        for peer in &self.peers {
            let hb_request = HeartbeatRequest {
                round: self.hb_round,
                #[cfg(feature = "adaptive")]
                sent_at: SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64,
            };
            self.outgoing.push(BLEMessage {
                from: self.pid,
                to: *peer,
                msg: HeartbeatMsg::Request(hb_request),
            });
        }
    }

    /// End of a heartbeat round. Returns current leader and election status.
    pub(crate) fn hb_timeout(&mut self, seq_paxos_promise: Ballot) -> Option<Ballot> {
        self.update_leader();
        self.update_happiness();
        self.check_takeover();
        self.new_hb_round();
        if seq_paxos_promise > self.leader {
            // Sync leader with Paxos promise in case ballot didn't make it to BLE followers
            // or become_leader() was called.
            self.leader = seq_paxos_promise;
            if seq_paxos_promise.pid == self.pid {
                self.current_ballot = seq_paxos_promise;
            }
            self.happy = true;
        }
        if self.leader == self.current_ballot {
            Some(self.current_ballot)
        } else {
            None
        }
    }

    fn update_leader(&mut self) {
        let max_reply_ballot = self.heartbeat_replies.iter().map(|r| r.ballot).max();
        if let Some(max) = max_reply_ballot {
            if max > self.leader {
                self.leader = max;
            }
        }
    }

    fn update_happiness(&mut self) {
        self.happy = if self.leader == self.current_ballot {
            let potential_followers = self
                .heartbeat_replies
                .iter()
                .filter(|hb_reply| hb_reply.leader <= self.current_ballot)
                .count();
            let can_form_quorum = self.quorum.is_majority_quorum(potential_followers + 1);
            if can_form_quorum {
                true
            } else {
                let see_larger_happy_leader = self
                    .heartbeat_replies
                    .iter()
                    .any(|r| r.leader > self.current_ballot && r.happy);
                see_larger_happy_leader
            }
        } else {
            self.heartbeat_replies
                .iter()
                .any(|r| r.ballot == self.leader && r.happy)
        };
    }

    fn check_takeover(&mut self) {
        if !self.happy {
            let all_neighbors_unhappy = self.heartbeat_replies.iter().all(|r| !r.happy);
            let im_quorum_connected = self
                .quorum
                .is_majority_quorum(self.heartbeat_replies.len() + 1);
            if all_neighbors_unhappy && im_quorum_connected {
                // We increment past our leader instead of max of unhappy ballots because we
                // assume we have already checked leader for this round so they should be equal
                self.current_ballot.n = self.leader.n + 1;
                self.leader = self.current_ballot;
                self.happy = true;
            }
        }
    }

    fn handle_request(&mut self, from: NodeId, req: HeartbeatRequest) {
        let hb_reply = HeartbeatReply {
            round: req.round,
            ballot: self.current_ballot,
            leader: self.leader,
            happy: self.happy,
            #[cfg(feature = "adaptive")]
            sent_at: req.sent_at, // Echo the timestamp
        };
        self.outgoing.push(BLEMessage {
            from: self.pid,
            to: from,
            msg: HeartbeatMsg::Reply(hb_reply),
        });
    }

    fn handle_reply(&mut self, _from: NodeId, rep: HeartbeatReply) {
        if rep.round == self.hb_round {
            #[cfg(feature = "adaptive")]
            {
                let now = SystemTime::now()
                    .duration_since(UNIX_EPOCH)
                    .unwrap()
                    .as_micros() as u64;
                let rtt = (now - rep.sent_at) as f64;
                let one_way_secs = rtt / 2000000.0;

                if let Some(history) = self.latency_histories.get_mut(&_from) {
                    if history.len() >= self.latency_window_size {
                        history.pop_front();
                    }
                    history.push_back(one_way_secs);
                }
            }
            self.heartbeat_replies.push(rep);
        }
    }

    pub(crate) fn get_current_ballot(&self) -> Ballot {
        self.current_ballot
    }

    /// Returns the rolling average of one-way-latencies needed to reach a fast_quorum
    #[cfg(feature = "adaptive")]
    pub fn get_fast_quorum_latency(&self) -> Option<f64> {
        let mut latencies: Vec<f64> = self
            .peers
            .iter()
            .filter_map(|&id| self.get_node_latency_avg(id))
            .collect();
        latencies.push(0.0); // Own latency
        if latencies.len() < self.quorum.fast_quorum {
            return None;
        }
        latencies.sort_by(|a, b| a.partial_cmp(b).unwrap_or(std::cmp::Ordering::Equal));
        Some(latencies[self.quorum.fast_quorum - 1])
    }

    #[cfg(feature = "adaptive")]
    fn get_node_latency_avg(&self, node_id: NodeId) -> Option<f64> {
        self.latency_histories.get(&node_id).and_then(|history| {
            if history.is_empty() {
                None
            } else {
                let sum: f64 = history.iter().sum();
                Some(sum / history.len() as f64)
            }
        })
    }
}

/// Configuration for `BallotLeaderElection`.
/// # Fields
/// * `configuration_id`: The identifier for the configuration that this node is part of.
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `peers`: The peers of this node i.e. the `pid`s of the other servers in the configuration.
/// * `priority`: Set custom priority for this node to be elected as the leader.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `logger_file_path`: The path where the default logger logs events.
#[derive(Clone, Debug)]
pub(crate) struct BLEConfig {
    pid: NodeId,
    peers: Vec<NodeId>,
    priority: u32,
    buffer_size: usize,
    #[cfg(feature = "adaptive")]
    latency_window_size: usize,
    #[cfg(feature = "logging")]
    logger_file_path: Option<String>,
    #[cfg(feature = "logging")]
    custom_logger: Option<Logger>,
}

impl From<OmniPaxosConfig> for BLEConfig {
    fn from(config: OmniPaxosConfig) -> Self {
        let pid = config.server_config.pid;
        let peers = config
            .cluster_config
            .nodes
            .into_iter()
            .filter(|x| *x != pid)
            .collect();

        Self {
            pid,
            peers,
            priority: config.server_config.leader_priority,
            buffer_size: BLE_BUFFER_SIZE,
            #[cfg(feature = "adaptive")]
            latency_window_size: LATENCY_TRACKING_WINDOW_SIZE,
            #[cfg(feature = "logging")]
            logger_file_path: config.server_config.logger_file_path,
            #[cfg(feature = "logging")]
            custom_logger: config.server_config.custom_logger,
        }
    }
}
