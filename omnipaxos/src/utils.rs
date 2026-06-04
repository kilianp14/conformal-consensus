#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};
#[cfg(feature = "logging")]
use slog::{o, Drain, Logger};
use std::{cmp::Ordering, fmt::Debug};
#[cfg(feature = "logging")]
use std::{
    fmt::{Display, Formatter, Result},
    fs::OpenOptions,
    sync::Mutex,
};

/// ID for an OmniPaxos node
pub type NodeId = u64;

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug + Eq {}

/// The status of an Undecided entry in the log
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum AcceptStatus {
    /// Accepted via OmniPaxos
    LeaderAccept,
    /// Accepted via the fast path of FastPaxos
    FastAccept,
    /// Accepted via the slow path of FastPaxos
    SlowAccept,
    /// Only for testing if fast path would have succeeded
    TestAccept,
}

/// The entry read in the log.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum LogEntry<T>
where
    T: Entry,
{
    /// The entry is decided.
    Decided(T, AcceptStatus),
    /// The entry is NOT decided. Might be removed from log at later time
    Undecided(T, AcceptStatus),
    /// Slot is currently empty
    Empty,
}

impl<T: PartialEq + Entry> PartialEq for LogEntry<T> {
    fn eq(&self, other: &Self) -> bool {
        match (self, other) {
            (LogEntry::Decided(e1, _), LogEntry::Decided(e2, _)) => e1 == e2,
            (LogEntry::Empty, LogEntry::Empty) => true,
            (LogEntry::Undecided(e1, _), LogEntry::Undecided(e2, _)) => e1 == e2,
            _ => false,
        }
    }
}

pub(crate) mod defaults {
    pub(crate) const BUFFER_SIZE: usize = 100000;
    pub(crate) const BLE_BUFFER_SIZE: usize = 100;
    pub(crate) const ELECTION_TIMEOUT: u64 = 1;
    pub(crate) const RESEND_MESSAGE_TIMEOUT: u64 = 100;
    #[cfg(feature = "adaptive")]
    pub(crate) const LATENCY_TRACKING_WINDOW_SIZE: usize = 10;
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

/// The type of quorum used by the OmniPaxos cluster.
#[derive(Copy, Clone, Debug)]
pub(crate) struct Quorum {
    /// Number of Nodes
    pub(crate) total_nodes: usize,
    /// Majority of nodes
    pub(crate) majority_quorum: usize,
    /// Number of nodes for successful fast round
    pub(crate) fast_quorum: usize,
}

impl Quorum {
    pub(crate) fn with(total_nodes: usize) -> Self {
        Self {
            total_nodes,
            majority_quorum: total_nodes / 2 + 1,
            fast_quorum: (total_nodes * 3).div_ceil(4),
        }
    }

    pub(crate) fn is_majority_quorum(&self, num_nodes: usize) -> bool {
        num_nodes >= self.majority_quorum
    }

    pub(crate) fn is_fast_quorum(&self, num_nodes: usize) -> bool {
        num_nodes >= self.fast_quorum
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

/// Stats to be collected during operation regarding fast path utilization
#[derive(Clone, Debug, Default)]
#[cfg(feature = "logging")]
pub struct FastPathStats {
    /// Total number of appends attempted (also counts retries)
    pub append_attempts: u64,
    /// Total number of appends attempted using fast-path
    pub fast_path_attempts: u64,
    /// Total number of appends with a successful fast-path
    pub fast_path_successes: u64,
    /// Total number of appends with an unsuccessful fast-path
    pub fast_path_errors: u64,
}

#[cfg(feature = "logging")]
impl FastPathStats {
    /// Ratio of fast-path attempts compared to total append attempts
    pub fn get_fast_path_utilization(&self) -> Option<f64> {
        if self.append_attempts != 0 {
            Some(self.fast_path_attempts as f64 / self.append_attempts as f64)
        } else {
            None
        }
    }

    /// Ratio of fast-path errors compared to total append attempts
    /// This should be bounded using CRC in adaptive mode
    pub fn get_fast_path_error_rate_total(&self) -> Option<f64> {
        if self.append_attempts != 0 {
            Some(self.fast_path_errors as f64 / self.append_attempts as f64)
        } else {
            None
        }
    }

    /// Ratio of fast-path successes compared to total append attempts
    pub fn get_fast_path_success_rate_total(&self) -> Option<f64> {
        if self.append_attempts != 0 {
            Some(self.fast_path_successes as f64 / self.append_attempts as f64)
        } else {
            None
        }
    }

    /// Ratio of fast-path errors compared to fast path attempts
    pub fn get_fast_path_error_rate(&self) -> Option<f64> {
        if self.fast_path_attempts != 0 {
            Some(self.fast_path_errors as f64 / self.fast_path_attempts as f64)
        } else {
            None
        }
    }

    /// Ratio of fast-path successes compared to fast path attempts
    pub fn get_fast_path_success_rate(&self) -> Option<f64> {
        if self.fast_path_attempts != 0 {
            Some(self.fast_path_successes as f64 / self.fast_path_attempts as f64)
        } else {
            None
        }
    }
}

#[cfg(feature = "logging")]
impl Display for FastPathStats {
    fn fmt(&self, f: &mut Formatter<'_>) -> Result {
        let fmt_pct = |opt: Option<f64>| match opt {
            Some(v) => format!("{:.2}%", v * 100.0),
            None => "N/A".to_string(),
        };

        writeln!(f, "=== Fast Path Statistics ===")?;
        writeln!(f, "Total Append Attempts:  {}", self.append_attempts)?;
        writeln!(
            f,
            "Fast Path Attempts:     {} (Utilization: {})",
            self.fast_path_attempts,
            fmt_pct(self.get_fast_path_utilization())
        )?;
        writeln!(
            f,
            "  ├─ Successes:         {} (Total Rate: {}, Path Success: {})",
            self.fast_path_successes,
            fmt_pct(self.get_fast_path_success_rate_total()),
            fmt_pct(self.get_fast_path_success_rate())
        )?;
        write!(
            f,
            "  └─ Errors:            {} (Total Rate: {}, Path Error: {})",
            self.fast_path_errors,
            fmt_pct(self.get_fast_path_error_rate_total()),
            fmt_pct(self.get_fast_path_error_rate())
        )
    }
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
