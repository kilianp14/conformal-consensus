use super::state_cache::StateCache;
use crate::{
    storage::{Entry, Storage, StorageOp, StorageResult},
    utils::{AcceptedMetaData, Ballot, LogEntry, LogSync},
};
use std::{
    marker::PhantomData,
    ops::{Bound, RangeBounds},
};

pub(crate) struct InternalStorageConfig {
    pub(crate) batch_size: usize,
}

/// Internal representation of storage. Serves as the interface between Sequence Paxos and the
/// storage back-end.
pub(crate) struct InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    storage: I,
    state_cache: StateCache<T>,
    _t: PhantomData<T>,
}

impl<I, T> InternalStorage<I, T>
where
    I: Storage<T>,
    T: Entry,
{
    pub(crate) fn with(storage: I, config: InternalStorageConfig) -> Self {
        let mut internal_store = InternalStorage {
            storage,
            state_cache: StateCache::new(config),
            _t: Default::default(),
        };
        internal_store.load_cache();
        internal_store
    }

    fn load_cache(&mut self) {
        self.state_cache.promise = self
            .storage
            .get_promise()
            .expect("Failed to load cache from storage.")
            .unwrap_or_default();
        self.state_cache.decided_idx = self.storage.get_decided_idx().unwrap();
        self.state_cache.accepted_round = self
            .storage
            .get_accepted_round()
            .unwrap()
            .unwrap_or_default();
        self.state_cache.accepted_idx = self.storage.get_log_len().unwrap();
    }

    /// Read all decided entries from `from_idx` in the log. Returns `None` if `from_idx` is out of bounds.
    pub(crate) fn read_decided_suffix(
        &self,
        from_idx: usize,
    ) -> StorageResult<Option<Vec<LogEntry<T>>>> {
        let decided_idx = self.get_decided_idx();
        if from_idx < decided_idx {
            self.read(from_idx..decided_idx)
        } else {
            Ok(None)
        }
    }

    /// Read entries in the range `r` in the log. Returns `None` if `r` is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> StorageResult<Option<Vec<LogEntry<T>>>>
    where
        R: RangeBounds<usize>,
    {
        let from_idx = match r.start_bound() {
            Bound::Included(i) => *i,
            Bound::Excluded(e) => *e + 1,
            Bound::Unbounded => 0,
        };
        let to_idx = match r.end_bound() {
            Bound::Included(i) => *i + 1,
            Bound::Excluded(e) => *e,
            Bound::Unbounded => self.get_accepted_idx(),
        };
        if to_idx == 0 {
            return Ok(None);
        }
        let accepted_idx = self.get_accepted_idx();
        if to_idx < accepted_idx {
            return Ok(Some(self.create_read_log_entries(from_idx, to_idx)?));
        }
        Ok(None)
    }

    fn create_read_log_entries(&self, from: usize, to: usize) -> StorageResult<Vec<LogEntry<T>>> {
        let decided_idx = self.get_decided_idx();
        let entries = self
            .get_entries(from, to)?
            .into_iter()
            .enumerate()
            .map(|(idx, e)| {
                let log_idx = idx + from;
                if log_idx < decided_idx {
                    LogEntry::Decided(e)
                } else {
                    LogEntry::Undecided(e)
                }
            })
            .collect();
        Ok(entries)
    }

    // Append entry, if the batch size is reached, flush the batch and return the actual
    // accepted index (not including the batched entries)
    pub(crate) fn append_entry_with_batching(
        &mut self,
        entry: T,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entry(entry);
        self.flush_if_full_batch(append_res)
    }

    // Append entries in batch, if the batch size is reached, flush the batch and return the
    // accepted index and the flushed entries. If the batch size is not reached, return None.
    pub(crate) fn append_entries_with_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let append_res = self.state_cache.append_entries(entries);
        self.flush_if_full_batch(append_res)
    }

    fn flush_if_full_batch(
        &mut self,
        append_res: Option<Vec<T>>,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        if let Some(flushed_entries) = append_res {
            let accepted_idx = self.append_entries_without_batching(flushed_entries.clone())?;
            Ok(Some(AcceptedMetaData {
                accepted_idx,
                entries: flushed_entries,
            }))
        } else {
            Ok(None)
        }
    }

    pub(crate) fn flush_batch(&mut self) -> StorageResult<usize> {
        let flushed_entries = self.state_cache.take_batched_entries();
        self.append_entries_without_batching(flushed_entries)
    }

    pub(crate) fn flush_batch_and_get_entries(
        &mut self,
    ) -> StorageResult<Option<AcceptedMetaData<T>>> {
        let flushed_entries = if !self.state_cache.batched_entries.is_empty() {
            Some(self.state_cache.take_batched_entries())
        } else {
            None
        };
        self.flush_if_full_batch(flushed_entries)
    }

    // Append entries without batching, return the accepted index
    pub(crate) fn append_entries_without_batching(
        &mut self,
        entries: Vec<T>,
    ) -> StorageResult<usize> {
        let num_new_entries = entries.len();
        self.storage.append_entries(entries)?;
        self.state_cache.accepted_idx += num_new_entries;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) fn sync_log(
        &mut self,
        accepted_round: Ballot,
        decided_idx: usize,
        log_sync: Option<LogSync<T>>,
    ) -> StorageResult<usize> {
        self.state_cache.accepted_round = accepted_round;
        self.state_cache.decided_idx = decided_idx;
        let mut sync_txn: Vec<StorageOp<T>> = vec![
            StorageOp::SetAcceptedRound(accepted_round),
            StorageOp::SetDecidedIndex(decided_idx),
        ];
        if let Some(sync) = log_sync {
            self.state_cache.accepted_idx = sync.sync_idx + sync.suffix.len();
            sync_txn.push(StorageOp::AppendOnPrefix(sync.sync_idx, sync.suffix));
        }
        self.storage.write_atomically(sync_txn)?;
        Ok(self.state_cache.accepted_idx)
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.state_cache.promise = n_prom;
        self.storage.set_promise(n_prom)
    }

    pub(crate) fn set_decided_idx(&mut self, idx: usize) -> StorageResult<()> {
        self.state_cache.decided_idx = idx;
        self.storage.set_decided_idx(idx)
    }

    pub(crate) fn get_decided_idx(&self) -> usize {
        self.state_cache.decided_idx
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.state_cache.accepted_round
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        self.storage.get_entries(from, to)
    }

    /// The length of the replicated log, as if log was never compacted.
    pub(crate) fn get_accepted_idx(&self) -> usize {
        self.state_cache.accepted_idx
    }

    pub(crate) fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        self.storage.get_suffix(from)
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.state_cache.promise
    }
}
