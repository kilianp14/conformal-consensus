use crate::{
    sequence_paxos::utils::SlotId,
    utils::{Ballot, Entry, LogEntry},
};
use std::{
    collections::BTreeSet,
    fmt::Debug,
    ops::{Bound, RangeBounds},
};

#[derive(Debug)]
pub(crate) struct MemoryStorage<T>
where
    T: Entry,
{
    /// Vector which contains all the logged entries in-memory.
    log: Vec<LogEntry<T>>,
    /// Slots in the log that are empty
    empty_slots: BTreeSet<SlotId>,
    /// Last promised round.
    promise: Ballot,
    /// Length of the decided log.
    decided_idx: SlotId,
}

impl<T> MemoryStorage<T>
where
    T: Entry,
{
    pub(crate) fn new() -> Self {
        Self {
            log: Vec::new(),
            empty_slots: BTreeSet::new(),
            promise: Ballot::default(),
            decided_idx: 0,
        }
    }

    /// Inserts a single entry into the log. Picks the first empty index if there exists one,
    /// otherwise appends at the end
    pub fn add_entry(&mut self, entry: LogEntry<T>) -> usize {
        match self.empty_slots.pop_first() {
            Some(index) => {
                self.insert_at_index(index, entry);
                index
            }
            None => {
                self.log.push(entry);
                self.log.len() - 1
            }
        }
    }

    /// Truncate the log at index and append the new suffix, returns new accepted index (log length)
    pub(crate) fn append_suffix(&mut self, suffix: Vec<LogEntry<T>>, from_idx: usize) {
        self.log.truncate(from_idx);
        self.log.extend(suffix);
    }

    /// Inserts a value at a specific index, empty slots in between are filled with None
    pub(crate) fn insert_at_index(&mut self, index: SlotId, value: LogEntry<T>) {
        if index < self.decided_idx {
            panic!("Cannot overwrite decided entry at index {}", index);
        }
        if index < self.log.len() {
            self.log[index] = value;
        } else if index == self.log.len() {
            self.log.push(value);
        } else {
            // Fill the gap with None
            for gap_idx in self.log.len()..index {
                self.empty_slots.insert(gap_idx);
            }
            self.log.resize(index, LogEntry::Empty);
            self.log.push(value);
        }
    }

    /// Returns the suffix of entries in the log from index `from` (inclusive).
    /// If the index is out of bounds, it returns an empty vector.
    pub(crate) fn get_suffix(&self, from: usize) -> Vec<LogEntry<T>> {
        match self.log.get(from..) {
            Some(suffix) => suffix.to_vec(),
            None => vec![],
        }
    }

    /// Checks whether a specific slot in the log is empty
    pub(crate) fn slot_is_empty(&self, index: SlotId) -> bool {
        self.empty_slots.contains(&index)
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

        Some(self.log[from_idx..to_idx].to_vec())
    }

    /// Read all decided entries from `from_idx` in the log.
    pub(crate) fn read_decided_suffix(&self, from_idx: usize) -> Option<Vec<LogEntry<T>>> {
        if from_idx < self.decided_idx {
            self.read(from_idx..self.decided_idx)
        } else {
            None
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
}
