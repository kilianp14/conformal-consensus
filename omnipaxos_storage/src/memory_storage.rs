use omnipaxos::{
    storage::{Entry, Storage, StorageOp, StorageResult},
    utils::Ballot,
};
/// An in-memory storage implementation for SequencePaxos.
#[derive(Clone)]
pub struct MemoryStorage<T>
where
    T: Entry,
{
    /// Vector which contains all the logged entries in-memory.
    log: Vec<T>,
    /// Last promised round.
    n_prom: Option<Ballot>,
    /// Last accepted round.
    acc_round: Option<Ballot>,
    /// Length of the decided log.
    ld: usize,
}

impl<T> Storage<T> for MemoryStorage<T>
where
    T: Entry,
{
    fn write_atomically(&mut self, ops: Vec<StorageOp<T>>) -> StorageResult<()> {
        for op in ops {
            match op {
                StorageOp::AppendEntry(entry) => self.append_entry(entry)?,
                StorageOp::AppendEntries(entries) => self.append_entries(entries)?,
                StorageOp::AppendOnPrefix(from_idx, entries) => {
                    self.append_on_prefix(from_idx, entries)?
                }
                StorageOp::SetPromise(bal) => self.set_promise(bal)?,
                StorageOp::SetDecidedIndex(idx) => self.set_decided_idx(idx)?,
                StorageOp::SetAcceptedRound(bal) => self.set_accepted_round(bal)?,
            }
        }
        Ok(())
    }

    fn append_entry(&mut self, entry: T) -> StorageResult<()> {
        self.log.push(entry);
        Ok(())
    }

    fn append_entries(&mut self, entries: Vec<T>) -> StorageResult<()> {
        let mut e = entries;
        self.log.append(&mut e);
        Ok(())
    }

    fn append_on_prefix(&mut self, from_idx: usize, entries: Vec<T>) -> StorageResult<()> {
        self.log.truncate(from_idx);
        self.append_entries(entries)
    }

    fn set_promise(&mut self, n_prom: Ballot) -> StorageResult<()> {
        self.n_prom = Some(n_prom);
        Ok(())
    }

    fn set_decided_idx(&mut self, ld: usize) -> StorageResult<()> {
        self.ld = ld;
        Ok(())
    }

    fn get_decided_idx(&self) -> StorageResult<usize> {
        Ok(self.ld)
    }

    fn set_accepted_round(&mut self, na: Ballot) -> StorageResult<()> {
        self.acc_round = Some(na);
        Ok(())
    }

    fn get_accepted_round(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.acc_round)
    }

    fn get_entries(&self, from: usize, to: usize) -> StorageResult<Vec<T>> {
        Ok(self.log.get(from..to).unwrap_or(&[]).to_vec())
    }

    fn get_log_len(&self) -> StorageResult<usize> {
        Ok(self.log.len())
    }

    fn get_suffix(&self, from: usize) -> StorageResult<Vec<T>> {
        Ok(match self.log.get((from)..) {
            Some(s) => s.to_vec(),
            None => vec![],
        })
    }

    fn get_promise(&self) -> StorageResult<Option<Ballot>> {
        Ok(self.n_prom)
    }
}

impl<T: Entry> Default for MemoryStorage<T> {
    fn default() -> Self {
        Self {
            log: vec![],
            n_prom: None,
            acc_round: None,
            ld: 0,
        }
    }
}
