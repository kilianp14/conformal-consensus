pub(crate) mod internal_storage;
mod state_cache;

use crate::utils::Ballot;
use std::{error::Error, fmt::Debug};

/// Type of the entries stored in the log.
pub trait Entry: Clone + Debug {}

/// The Result type returned by the storage API.
pub type StorageResult<T> = Result<T, Box<dyn Error>>;

/// The write operations of the storge implementation.
#[derive(Debug)]
pub enum StorageOp<T: Entry> {
    /// Appends an entry to the end of the log.
    AppendEntry(T),
    /// Appends entries to the end of the log.
    AppendEntries(Vec<T>),
    /// Appends entries to the log from the prefix specified by the given index.
    AppendOnPrefix(usize, Vec<T>),
    /// Sets the round that has been promised.
    SetPromise(Ballot),
    /// Sets the decided index in the log.
    SetDecidedIndex(usize),
    /// Sets the latest accepted round.
    SetAcceptedRound(Ballot),
}

/// Trait for implementing the storage backend of Sequence Paxos.
pub trait Storage<T>
where
    T: Entry,
{
    /// **Atomically** perform all storage operations in order.
    /// For correctness, the operations must be atomic i.e., either all operations are performed
    /// successfully or all get rolled back. If the `StorageResult` returns as `Err`, the
    /// operations are assumed to have been rolled back to the previous state before this function
    /// call.
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()>;

    /// Appends an entry to the end of the log.
    fn append_entry(&mut self, entry: T) -> StorageResult<()>;

    /// Appends the entries of `entries` to the end of the log.
    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()>;

    /// Appends the entries of `entries` to the prefix from index `from_index` (inclusive) in the log.
    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()>;

    /// Sets the round that has been promised.
    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()>;

    /// Sets the decided index in the log.
    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()>;

    /// Returns the decided index in the log.
    fn get_decided_idx(&self) -> StorageResult<usize>;

    /// Sets the latest accepted round.
    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()>;

    /// Returns the latest round in which entries have been accepted, returns `None` if no
    /// entries have been accepted.
    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>>;

    /// Returns the entries in the log in the index interval of [from, to).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>>;

    /// Returns the current length of the log (without the trimmed/snapshotted entries).
    fn get_log_len(&self) -> StorageResult<usize>;

    /// Returns the suffix of entries in the log from index `from` (inclusive).
    /// If entries **do not exist for the complete interval**, an empty Vector should be returned.
    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>>;

    /// Returns the round that has been promised.
    fn get_promise(&self) -> StorageResult<Option<Ballot>>;
}
