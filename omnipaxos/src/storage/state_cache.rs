use super::{internal_storage::InternalStorageConfig, Entry};
use crate::utils::Ballot;

/// A simple in-memory storage for simple state values of OmniPaxos.
pub(super) struct StateCache<T>
where
    T: Entry,
{
    /// The maximum number of entries to batch.
    pub batch_size: usize,
    /// Vector which contains all the logged entries in-memory.
    pub batched_entries: Vec<T>,
    /// Last promised round.
    pub promise: Ballot,
    /// Last accepted round.
    pub accepted_round: Ballot,
    /// Length of the decided log.
    pub decided_idx: usize,
    /// Length of the accepted log.
    pub accepted_idx: usize,
}

impl<T> StateCache<T>
where
    T: Entry,
{
    pub(super) fn new(config: InternalStorageConfig) -> Self {
        StateCache {
            batch_size: config.batch_size,
            batched_entries: Vec::with_capacity(config.batch_size),
            promise: Ballot::default(),
            accepted_round: Ballot::default(),
            decided_idx: 0,
            accepted_idx: 0,
        }
    }

    // Appends an entry to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    pub(super) fn append_entry(&mut self, entry: T) -> Option<Vec<T>> {
        self.batched_entries.push(entry);
        self.take_entries_if_batch_is_full()
    }

    // Appends entries to the end of the `batched_entries`. If the batch is full, the
    // batch is flushed and return flushed entries. Else, return None.
    pub(super) fn append_entries(&mut self, entries: Vec<T>) -> Option<Vec<T>> {
        self.batched_entries.extend(entries);
        self.take_entries_if_batch_is_full()
    }

    // Return batched entries if the batch is full that need to be flushed in to storage.
    fn take_entries_if_batch_is_full(&mut self) -> Option<Vec<T>> {
        if self.batched_entries.len() >= self.batch_size {
            Some(self.take_batched_entries())
        } else {
            None
        }
    }

    // Clears the batched entries and returns the cleared entries. If the batch is empty,
    // return an empty vector.
    pub(super) fn take_batched_entries(&mut self) -> Vec<T> {
        std::mem::take(&mut self.batched_entries)
    }
}
