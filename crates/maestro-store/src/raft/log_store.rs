use std::fmt::Debug;
use std::ops::{Bound, RangeBounds};
use std::path::Path;
use std::sync::Arc;

use openraft::storage::{LogFlushed, LogState, RaftLogStorage};
use openraft::{
    Entry, LogId, OptionalSend, RaftLogId, RaftLogReader, StorageError, StorageIOError, Vote,
};
use redb::{Database, ReadableDatabase, ReadableTable, TableDefinition};
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::TypeConfig;
use crate::error::StoreError;

const LOG: TableDefinition<u64, &[u8]> = TableDefinition::new("log");
const LOG_META: TableDefinition<&str, &[u8]> = TableDefinition::new("log_meta");

const VOTE_KEY: &str = "vote";
const COMMITTED_KEY: &str = "committed";
const LAST_PURGED_KEY: &str = "last_purged";

type RaftResult<T> = Result<T, StorageError<u64>>;

#[derive(Clone)]
pub(crate) struct LogStore {
    database: Arc<Database>,
}

impl LogStore {
    pub(crate) fn open(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        let database = Database::create(path)?;
        let transaction = database.begin_write()?;
        transaction.open_table(LOG)?;
        transaction.open_table(LOG_META)?;
        transaction.commit()?;
        Ok(Self {
            database: Arc::new(database),
        })
    }

    fn read_meta<T: DeserializeOwned>(&self, key: &str) -> Result<Option<T>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(LOG_META)?;
        let stored = table.get(key)?;
        stored
            .map(|guard| serde_json::from_slice(guard.value()))
            .transpose()
            .map_err(StoreError::from)
    }

    fn write_meta<T: Serialize>(&self, key: &str, value: &T) -> Result<(), StoreError> {
        let encoded = serde_json::to_vec(value)?;
        let transaction = self.database.begin_write()?;
        transaction
            .open_table(LOG_META)?
            .insert(key, encoded.as_slice())?;
        transaction.commit()?;
        Ok(())
    }

    fn last_entry_id(&self) -> Result<Option<LogId<u64>>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(LOG)?;
        let last = table.last()?;
        last.map(|(_, value)| decode_entry(value.value()).map(|entry| *entry.get_log_id()))
            .transpose()
    }

    fn entries(&self, start: u64, end: Bound<u64>) -> Result<Vec<Entry<TypeConfig>>, StoreError> {
        let transaction = self.database.begin_read()?;
        let table = transaction.open_table(LOG)?;
        table
            .range((Bound::Included(start), end))?
            .map(|item| decode_entry(item?.1.value()))
            .collect()
    }

    fn append_entries(
        &self,
        entries: impl IntoIterator<Item = Entry<TypeConfig>>,
    ) -> Result<(), StoreError> {
        let transaction = self.database.begin_write()?;
        {
            let mut table = transaction.open_table(LOG)?;
            entries.into_iter().try_for_each(|entry| {
                let encoded = serde_json::to_vec(&entry)?;
                table.insert(entry.get_log_id().index, encoded.as_slice())?;
                Ok::<_, StoreError>(())
            })?;
        }
        transaction.commit()?;
        Ok(())
    }

    fn remove_range(&self, range: impl RangeBounds<u64>) -> Result<(), StoreError> {
        let transaction = self.database.begin_write()?;
        transaction
            .open_table(LOG)?
            .retain_in(range, |_, _| false)?;
        transaction.commit()?;
        Ok(())
    }
}

impl RaftLogReader<TypeConfig> for LogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> RaftResult<Vec<Entry<TypeConfig>>> {
        let start = match range.start_bound() {
            Bound::Included(index) => *index,
            Bound::Excluded(index) => index + 1,
            Bound::Unbounded => 0,
        };
        self.entries(start, range.end_bound().cloned())
            .map_err(|error| StorageIOError::read_logs(&error).into())
    }
}

impl RaftLogStorage<TypeConfig> for LogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> RaftResult<LogState<TypeConfig>> {
        let last_purged_log_id: Option<LogId<u64>> = self
            .read_meta(LAST_PURGED_KEY)
            .map_err(|error| StorageIOError::read_logs(&error))?;
        let last_log_id = self
            .last_entry_id()
            .map_err(|error| StorageIOError::read_logs(&error))?
            .or(last_purged_log_id);
        Ok(LogState {
            last_purged_log_id,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<u64>) -> RaftResult<()> {
        self.write_meta(VOTE_KEY, vote)
            .map_err(|error| StorageIOError::write_vote(&error).into())
    }

    async fn read_vote(&mut self) -> RaftResult<Option<Vote<u64>>> {
        self.read_meta(VOTE_KEY)
            .map_err(|error| StorageIOError::read_vote(&error).into())
    }

    async fn save_committed(&mut self, committed: Option<LogId<u64>>) -> RaftResult<()> {
        self.write_meta(COMMITTED_KEY, &committed)
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn read_committed(&mut self) -> RaftResult<Option<LogId<u64>>> {
        self.read_meta::<Option<LogId<u64>>>(COMMITTED_KEY)
            .map(Option::flatten)
            .map_err(|error| StorageIOError::read_logs(&error).into())
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<TypeConfig>) -> RaftResult<()>
    where
        I: IntoIterator<Item = Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let result = self.append_entries(entries);
        let flushed = result
            .as_ref()
            .map(|_| ())
            .map_err(|error| std::io::Error::other(error.to_string()));
        callback.log_io_completed(flushed);
        result.map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn truncate(&mut self, log_id: LogId<u64>) -> RaftResult<()> {
        self.remove_range(log_id.index..)
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }

    async fn purge(&mut self, log_id: LogId<u64>) -> RaftResult<()> {
        self.write_meta(LAST_PURGED_KEY, &log_id)
            .and_then(|_| self.remove_range(..=log_id.index))
            .map_err(|error| StorageIOError::write_logs(&error).into())
    }
}

fn decode_entry(bytes: &[u8]) -> Result<Entry<TypeConfig>, StoreError> {
    Ok(serde_json::from_slice(bytes)?)
}
