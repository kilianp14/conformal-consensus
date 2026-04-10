use crate::{
    errors::{valid_config, ConfigError},
    leader_election::BallotLeaderElection,
    messages::Message,
    sequence_paxos::SequencePaxos,
    utils::{
        defaults::{BUFFER_SIZE, ELECTION_TIMEOUT, RESEND_MESSAGE_TIMEOUT},
        Ballot, Entry, LogEntry, LogicalClock, Mode, NodeId, Phase,
    },
};
#[cfg(any(feature = "toml_config", feature = "serde"))]
use serde::Deserialize;
#[cfg(feature = "serde")]
use serde::Serialize;
#[cfg(feature = "toml_config")]
use std::fs;
use std::{
    error::Error,
    fmt::{Debug, Display},
    ops::RangeBounds,
};

/// Configuration for `OmniPaxos`.
/// # Fields
/// * `cluster_config`: The configuration settings that are cluster-wide.
/// * `server_config`: The configuration settings that are specific to this OmniPaxos server.
#[allow(missing_docs)]
#[derive(Clone, Debug, Default)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct OmniPaxosConfig {
    pub cluster_config: ClusterConfig,
    pub server_config: ServerConfig,
}

impl OmniPaxosConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        self.cluster_config.validate()?;
        self.server_config.validate()?;
        valid_config!(
            self.cluster_config.nodes.contains(&self.server_config.pid),
            "Nodes must include own server pid"
        );
        Ok(())
    }

    /// Creates a new `OmniPaxosConfig` from a `toml` file.
    #[cfg(feature = "toml_config")]
    pub fn with_toml(file_path: &str) -> Result<Self, ConfigError> {
        let config_file = fs::read_to_string(file_path)?;
        let config: OmniPaxosConfig = toml::from_str(&config_file)?;
        config.validate()?;
        Ok(config)
    }

    /// Checks all configuration fields and returns the local OmniPaxos node if successful.
    pub fn build<T>(self) -> Result<OmniPaxos<T>, ConfigError>
    where
        T: Entry,
    {
        self.validate()?;
        // Use stored ballot as initial BLE leader
        Ok(OmniPaxos {
            election_clock: LogicalClock::with(self.server_config.election_tick_timeout),
            resend_message_clock: LogicalClock::with(
                self.server_config.resend_message_tick_timeout,
            ),
            ble: BallotLeaderElection::with(self.clone().into()),
            seq_paxos: SequencePaxos::with(self.clone().into()),
        })
    }
}

/// Configuration for an `OmniPaxos` cluster.
/// # Fields
/// * `nodes`: The nodes in the cluster i.e. the `pid`s of the other servers in the configuration.
#[derive(Clone, Debug, PartialEq, Default)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "toml_config", serde(default))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct ClusterConfig {
    /// The nodes in the cluster i.e. the `pid`s of the servers in the configuration.
    pub nodes: Vec<NodeId>,
}

impl ClusterConfig {
    /// Checks that all the fields of the cluster config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        let num_nodes = self.nodes.len();
        valid_config!(num_nodes > 1, "Need more than 1 node");
        Ok(())
    }

    /// Checks all configuration fields and builds a local OmniPaxos node with settings for this
    /// node defined in `server_config`
    pub fn build_for_server<T>(
        self,
        server_config: ServerConfig,
    ) -> Result<OmniPaxos<T>, ConfigError>
    where
        T: Entry,
    {
        let op_config = OmniPaxosConfig {
            cluster_config: self,
            server_config,
        };
        op_config.build()
    }
}

/// Configuration for a singular `OmniPaxos` instance in a cluster.
/// # Fields
/// * `pid`: The unique identifier of this node. Must not be 0.
/// * `election_tick_timeout`: The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms. Must not be 0.
/// * `resend_message_tick_timeout`: The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
/// * `buffer_size`: The buffer size for outgoing messages.
/// * `logger_file_path`: The path where the default logger logs events.
/// * `leader_priority` : Custom priority for this node to be elected as the leader.
#[derive(Clone, Debug)]
#[cfg_attr(feature = "toml_config", derive(Deserialize), serde(default))]
pub struct ServerConfig {
    /// The unique identifier of this node. Must not be 0.
    pub pid: NodeId,
    /// Operating mode of sequence_paxos (OmniPaxos or FastPaxos)
    pub mode: Mode,
    /// The number of calls to `tick()` before leader election is updated. If this is set to 5 and `tick()` is called every 10ms, then the election timeout will be 50ms.
    pub election_tick_timeout: u64,
    /// The number of calls to `tick()` before a message is considered dropped and thus resent. Must not be 0.
    pub resend_message_tick_timeout: u64,
    /// The buffer size for outgoing messages.
    pub buffer_size: usize,
    /// Custom priority for this node to be elected as the leader.
    pub leader_priority: u32,
    /// The path where the default logger logs events.
    #[cfg(feature = "logging")]
    pub logger_file_path: Option<String>,
    /// Custom logger, if provided, will be used instead of the default logger.
    #[cfg(feature = "logging")]
    #[cfg_attr(feature = "toml_config", serde(skip_deserializing))]
    pub custom_logger: Option<slog::Logger>,
}

impl ServerConfig {
    /// Checks that all the fields of the server config are valid.
    pub fn validate(&self) -> Result<(), ConfigError> {
        valid_config!(self.pid != 0, "Server pid cannot be 0");
        valid_config!(self.buffer_size != 0, "Buffer size must be greater than 0");
        valid_config!(
            self.election_tick_timeout != 0,
            "Election tick timeout must be greater than 0"
        );
        valid_config!(
            self.resend_message_tick_timeout != 0,
            "Resend message tick timeout must be greater than 0"
        );
        Ok(())
    }
}

impl Default for ServerConfig {
    fn default() -> Self {
        Self {
            pid: 0,
            mode: Mode::OmniPaxos,
            election_tick_timeout: ELECTION_TIMEOUT,
            resend_message_tick_timeout: RESEND_MESSAGE_TIMEOUT,
            buffer_size: BUFFER_SIZE,
            leader_priority: 0,
            #[cfg(feature = "logging")]
            logger_file_path: None,
            #[cfg(feature = "logging")]
            custom_logger: None,
        }
    }
}

/// The `OmniPaxos` struct represents an OmniPaxos server. Maintains the replicated log that can be read from and appended to.
/// It also handles incoming messages and produces outgoing messages that you need to fetch and send periodically using your own network implementation.
pub struct OmniPaxos<T>
where
    T: Entry,
{
    seq_paxos: SequencePaxos<T>,
    ble: BallotLeaderElection,
    election_clock: LogicalClock,
    resend_message_clock: LogicalClock,
}

impl<T> OmniPaxos<T>
where
    T: Entry,
{
    /// Return the decided index. 0 means that no entry has been decided.
    pub fn get_decided_idx(&self) -> usize {
        self.seq_paxos.internal_storage.get_decided_idx()
    }

    /// Returns the ID of the current leader and whether the node's `Phase` is `Phase::Accepted`.
    ///
    /// If the node's phase is `Phase::Accepted`, this implies that the returned leader is also
    /// in the accepted phase. However, a `Phase::Prepare` or a `false` response does not
    /// necessarily imply that the leader is not in the accepted phase; it only reflects the current
    /// phase of this node.
    pub fn get_current_leader(&self) -> Option<(NodeId, bool)> {
        let promised_pid = self.get_promise().pid;
        if promised_pid == 0 {
            None
        } else {
            let is_accepted = self.seq_paxos.get_state().1 == Phase::Accept;
            Some((promised_pid, is_accepted))
        }
    }

    /// Returns the promised ballot of this node.
    pub fn get_promise(&self) -> Ballot {
        self.seq_paxos.internal_storage.get_promise()
    }

    /// Moves outgoing messages from this server into the buffer. The messages should then be sent via the network implementation.
    pub fn take_outgoing_messages(&mut self, buffer: &mut Vec<Message<T>>) {
        let paxos_msgs = self.seq_paxos.take_outgoing_messages();
        buffer.extend(paxos_msgs.into_iter().map(Message::SequencePaxos));

        let ble_msgs = self.ble.take_outgoing_messages();
        buffer.extend(ble_msgs.into_iter().map(Message::BLE));
    }

    /// Read entry at index `idx` in the log. Returns `None` if `idx` is out of bounds.
    pub fn read(&self, idx: usize) -> Option<LogEntry<T>> {
        match self.seq_paxos.internal_storage.read(idx..idx + 1) {
            Some(mut v) => v.pop(),
            None => None,
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub fn read_entries<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
    where
        R: RangeBounds<usize>,
    {
        self.seq_paxos.internal_storage.read(r)
    }

    /// Read all decided entries starting at `from_idx` (inclusive) in the log. Returns `None` if `from_idx` is out of bounds.
    pub fn read_decided_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        self.seq_paxos
            .internal_storage
            .read_decided_suffix(from_idx)
    }

    /// Handle an incoming message
    pub fn handle_incoming(&mut self, m: Message<T>) {
        match m {
            Message::SequencePaxos(p) => self.seq_paxos.handle(p),
            Message::BLE(b) => self.ble.handle(b),
        }
    }

    /// Append an entry to the replicated log.
    pub fn append(&mut self, entry: T) {
        self.seq_paxos.append(entry)
    }

    /// Handles re-establishing a connection to a previously disconnected peer.
    /// This should only be called if the underlying network implementation indicates that a connection has been re-established.
    pub fn reconnected(&mut self, pid: NodeId) {
        self.seq_paxos.reconnected(pid)
    }

    /// Increments the internal logical clock. This drives the processes for leader changes, resending dropped messages, and flushing batched log entries.
    /// Each of these is triggered every `election_tick_timeout`, `resend_message_tick_timeout`, and `mode_change_tick_timeout` number of calls to this function
    /// (See how to configure these timeouts in `ServerConfig`).
    pub fn tick(&mut self) {
        if self.election_clock.tick_and_check_timeout() {
            self.election_timeout();
        }
        if self.resend_message_clock.tick_and_check_timeout() {
            self.seq_paxos.resend_message_timeout();
        }
    }

    /// Manually attempt to become the leader by incrementing this instance's Ballot. Calling this
    /// function may not result in gainig leadership if other instances are competing for
    /// leadership with higher Ballots.
    pub fn try_become_leader(&mut self) {
        let mut my_ballot = self.ble.get_current_ballot();
        let promise = self.get_promise();
        my_ballot.n = promise.n + 1;
        self.seq_paxos.handle_leader(my_ballot);
    }

    /*** BLE calls ***/
    /// Update the custom priority used in the Ballot for this server. Note that changing the
    /// priority triggers a leader re-election.
    pub fn set_priority(&mut self, p: u32) {
        self.ble.set_priority(p)
    }

    /// If the heartbeat of a leader is not received when election_timeout() is called, the server might attempt to become the leader.
    /// It is also used for the election process, where the server checks if it can become the leader.
    /// For instance if `election_timeout()` is called every 100ms, then if the leader fails, the servers will detect it after 100ms and elect a new server after another 100ms if possible.
    fn election_timeout(&mut self) {
        if let Some(new_leader) = self.ble.hb_timeout(self.get_promise()) {
            self.seq_paxos.handle_leader(new_leader);
        }
        #[cfg(feature = "adaptive")]
        self.seq_paxos
            .set_fast_quorum_latency(self.ble.get_fast_quorum_latency());
    }
}

/// An error returning the proposal that was failed due to that the current configuration is stopped.
#[derive(Copy, Clone, Debug)]
pub enum CompactionErr {
    /// Snapshot was called with an index that is not decided yet. Returns the currently decided index.
    UndecidedIndex(usize),
    /// Snapshot was called with an index which is already trimmed. Returns the currently compacted index.
    TrimmedIndex(usize),
    /// Trim was called with an index that is not decided by all servers yet. Returns the index decided by ALL servers currently.
    NotAllDecided(usize),
    /// Trim was called at a follower node. Trim must be called by the leader, which is the returned NodeId.
    NotCurrentLeader(NodeId),
}

impl Error for CompactionErr {}
impl Display for CompactionErr {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        Debug::fmt(self, f)
    }
}
