#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "logging")]
use slog::{o, Drain, Logger};
use std::{cmp::Ordering, fmt::Debug};
#[cfg(feature = "logging")]
use std::{fs::OpenOptions, sync::Mutex};

/// ID for an OmniPaxos node
pub type NodeId = u64;

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug {}

/// Id for an entry
pub type EntryId = (NodeId, u64);

/// The status of an Undecided entry in the log
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AcceptStatus {
    /// Accepted via OmniPaxos
    OpAccepted,
    /// Accepted via the fast path of FastPaxos
    FpFastAccepted,
    /// Accepted via the slow path of FastPaxos
    FpSlowAccepted,
}

/// The entry read in the log.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T),
    /// The entry is NOT decided. Might be removed from log at later time. Empty slots possible
    Undecided(EntryId, T, AcceptStatus),
    /// Slot is currently empty
    Empty,
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(v1), LogEntry::Decided(v2)) => v1 == v2,
            (LogEntry::Empty, LogEntry::Empty) => true,
            (LogEntry::Undecided(id1, _, _), LogEntry::Undecided(id2, _, _)) => id1 == id2,
            _ => false,
        }
    }
}

pub(crate) mod defaults {
    use super::Mode;

    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 1;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
    pub(crate) const DEFAULT_MODE: Mode = Mode::OmniPaxos;
}

/// Used for checking the ordering of message sequences in the accept phase
#[derive(PartialEq, Eq)]
pub(crate) enum MessageStatus {
    /// Expected message sequence progression
    Expected,
    /// Identified a message sequence break
    DroppedPreceding,
    /// An already identified message sequence break
    Outdated,
}

/// Keeps track of the ordering of messages in the accept phase
#[derive(Copy, Clone, Debug, Default, PartialEq, Eq, PartialOrd, Ord)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct SequenceNumber {
    /// Meant to refer to a TCP session
    pub session: u64,
    /// The sequence number with respect to a session
    pub counter: u64,
}

impl SequenceNumber {
    /// Compares this sequence number with the sequence number of an incoming message.
    pub(crate) fn check_msg_status(&self, msg_seq_num: SequenceNumber) -> MessageStatus {
        if msg_seq_num.session == self.session && msg_seq_num.counter == self.counter + 1 {
            MessageStatus::Expected
        } else if msg_seq_num <= *self {
            MessageStatus::Outdated
        } else {
            MessageStatus::DroppedPreceding
        }
    }
}

/// Used to define a Sequence Paxos epoch
#[derive(Clone, Copy, Eq, Debug, Default, PartialEq, Hash)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct Ballot {
    /// Ballot number
    pub n: u32,
    /// Custom priority parameter
    pub priority: u32,
    /// The pid of the process
    pub pid: NodeId,
}

impl Ballot {
    /// Creates a new Ballot
    /// # Arguments
    /// * `n` - Ballot number.
    /// * `pid` -  Used as tiebreaker for total ordering of ballots.
    pub fn with(n: u32, priority: u32, pid: NodeId) -> Ballot {
        Ballot { n, priority, pid }
    }
}

impl Ord for Ballot {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.n, self.priority, self.pid).cmp(&(other.n, other.priority, other.pid))
    }
}

impl PartialOrd for Ballot {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

pub(crate) struct LogicalClock {
    time: u64,
    timeout: u64,
}

impl LogicalClock {
    pub fn with(timeout: u64) -> Self {
        Self { time: 0, timeout }
    }

    pub fn tick_and_check_timeout(&mut self) -> bool {
        self.time += 1;
        if self.time == self.timeout {
            self.time = 0;
            true
        } else {
            false
        }
    }
}

/// Flexible quorums can be used to increase/decrease the read and write quorum sizes,
/// for different latency vs fault tolerance tradeoffs.
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(any(feature = "serde", feature = "toml_config"), derive(Deserialize))]
#[cfg_attr(feature = "serde", derive(Serialize))]
pub struct FlexibleQuorum {
    /// The number of nodes a leader needs to consult to get an up-to-date view of the log.
    pub read_quorum_size: usize,
    /// The number of acknowledgments a leader needs to commit an entry to the log
    pub write_quorum_size: usize,
}

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) enum Quorum {
    /// Both the read quorum and the write quorums are a majority of nodes
    Majority(usize),
    /// The read and write quorum sizes are defined by a `FlexibleQuorum`
    Flexible(FlexibleQuorum),
}

impl Quorum {
    pub(crate) fn with(flexible_quorum_config: Option<FlexibleQuorum>, num_nodes: usize) -> Self {
        match flexible_quorum_config {
            Some(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }) => Quorum::Flexible(FlexibleQuorum {
                read_quorum_size,
                write_quorum_size,
            }),
            None => Quorum::Majority(num_nodes / 2 + 1),
        }
    }

    pub(crate) fn is_prepare_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.read_quorum_size,
        }
    }

    pub(crate) fn is_accept_quorum(&self, num_nodes: usize) -> bool {
        match self {
            Quorum::Majority(majority) => num_nodes >= *majority,
            Quorum::Flexible(flex_quorum) => num_nodes >= flex_quorum.write_quorum_size,
        }
    }
}

#[derive(PartialEq, Debug)]
pub(crate) enum Phase {
    Prepare,
    Accept,
    Recover,
    None,
}

#[derive(PartialEq, Debug)]
pub(crate) enum Role {
    Follower,
    Leader,
}

/// Operating Mode of SequencePaxos
#[derive(Copy, Clone, Debug, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum Mode {
    /// FastPaxos based
    FastPaxos,
    /// OmniPaxos based
    OmniPaxos,
}

/// Creates an asynchronous logger which outputs to both the terminal and a specified file_path.
#[cfg(feature = "logging")]
pub fn create_logger(file_path: &str) -> Logger {
    let path = std::path::Path::new(file_path);
    let prefix = path.parent().unwrap(); // todo change unwrap
    std::fs::create_dir_all(prefix).unwrap(); // todo change unwrap

    let file = OpenOptions::new()
        .create(true)
        .write(true)
        .truncate(true)
        .open(file_path)
        .unwrap(); // todo change unwrap

    let term_decorator = slog_term::TermDecorator::new().build();
    let file_decorator = slog_term::PlainSyncDecorator::new(file);

    let term_fuse = slog_term::FullFormat::new(term_decorator).build().fuse();
    let file_fuse = slog_term::FullFormat::new(file_decorator).build().fuse();

    let both = Mutex::new(slog::Duplicate::new(term_fuse, file_fuse)).fuse();
    let both = slog_async::Async::new(both).build().fuse();
    Logger::root(both, o!())
}
