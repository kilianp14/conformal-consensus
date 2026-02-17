use crate::{
    sequence_paxos::utils::LogData,
    utils::{Ballot, Entry, LogEntry},
};
use std::{
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

#[derive(Debug)]
pub(crate) struct MemoryStorage<T>
where
    T: Entry,
{
    /// Vector which contains all the logged entries in-memory.
    log: Vec<LogData<T>>,
    /// Last promised round.
    promise: Ballot,
    /// Last accepted round.
    accepted_round: Ballot,
    /// Length of the decided log.
    decided_idx: usize,
}

impl<T> MemoryStorage<T>
where
    T: Entry,
{
    pub(crate) fn new() -> Self {
        Self {
            log: Vec::new(),
            promise: Ballot::default(),
            accepted_round: Ballot::default(),
            decided_idx: 0,
        }
    }

    /// Appends multiple entries and returns the new accepted index (log length).
    pub(crate) fn append_entries(&mut self, mut entries: Vec<LogData<T>>) -> usize {
        self.log.append(&mut entries);
        self.log.len()
    }

    /// Appends a single entry and returns the new accepted index (log length).
    pub fn append_entry(&mut self, entry: LogData<T>) -> usize {
        self.log.push(entry);
        self.log.len()
    }

    /// Truncate the log at index and append the new suffix, returns new accepted index (log length)
    pub(crate) fn append_suffix(&mut self, suffix: Vec<LogData<T>>, from_idx: usize) -> usize {
        self.log.truncate(from_idx);
        self.log.extend(suffix);
        self.log.len()
    }

    /// Read entries in the range `r`. Returns `None` if the range is out of bounds.
    pub(crate) fn read<R>(&self, r: R) -> Option<Vec<LogEntry<T>>>
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
            Bound::Unbounded => self.log.len(),
        };

        if to_idx > self.log.len() || from_idx > to_idx {
            return None;
        }

        let entries = self.log[from_idx..to_idx]
            .iter()
            .enumerate()
            .map(|(i, LogData { id: _, entry })| {
                let current_idx = from_idx + i;
                if current_idx < self.decided_idx {
                    LogEntry::Decided(entry.clone())
                } else {
                    LogEntry::Undecided(entry.clone())
                }
            })
            .collect();

        Some(entries)
    }

    /// Read all decided entries from `from_idx` in the log.
    pub(crate) fn read_decided_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        if from_idx < self.decided_idx {
            self.read(from_idx..self.decided_idx)
        } else {
            None
        }
    }

    /// Returns the suffix of entries in the log from index `from` (inclusive).
    /// If the index is out of bounds, it returns an empty vector.
    pub(crate) fn get_suffix(&self, from: usize) -> Vec<LogData<T>> {
        match self.log.get(from..) {
            Some(suffix) => suffix.to_vec(),
            None => vec![],
        }
    }

    pub(crate) fn set_promise(&mut self, n_prom: Ballot) {
        self.promise = n_prom;
    }

    pub(crate) fn get_promise(&self) -> Ballot {
        self.promise
    }

    pub(crate) fn set_decided_idx(&mut self, idx: usize) {
        self.decided_idx = idx;
    }

    pub(crate) fn get_decided_idx(&self) -> usize {
        self.decided_idx
    }

    pub(crate) fn set_accepted_round(&mut self, bal: Ballot) {
        self.accepted_round = bal;
    }

    pub(crate) fn get_accepted_round(&self) -> Ballot {
        self.accepted_round
    }

    pub(crate) fn get_accepted_idx(&self) -> usize {
        self.log.len()
    }
}
