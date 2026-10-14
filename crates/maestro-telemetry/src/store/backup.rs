use std::path::PathBuf;

use duckdb::params;

use super::{LogStore, partition_directory};
use crate::error::Result;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PendingPartition {
    pub key: String,
    pub directory: PathBuf,
}

impl LogStore {
    pub fn partitions_pending_backup(&self) -> Result<Vec<PendingPartition>> {
        let connection = self.db.reader()?;
        let mut statement = connection.prepare(
            "SELECT DISTINCT partition_key FROM partition_state WHERE state = 'exported' ORDER BY 1",
        )?;
        let keys = statement
            .query_map([], |row| row.get::<_, String>(0))?
            .collect::<duckdb::Result<Vec<_>>>()?;
        Ok(keys
            .into_iter()
            .map(|key| PendingPartition {
                directory: partition_directory(&self.parts_root, &key),
                key,
            })
            .collect())
    }

    pub fn mark_backed_up(&self, key: &str, seq_los: &[i64]) -> Result<()> {
        let mut connection = self.db.writer()?;
        let transaction = connection.transaction()?;
        seq_los.iter().try_for_each(|seq_lo| {
            transaction.execute(
                "UPDATE partition_state SET state = 'backed_up' WHERE partition_key = ? AND seq_lo = ?",
                params![key, seq_lo],
            )?;
            Ok::<_, crate::error::TelemetryError>(())
        })?;
        transaction.commit()?;
        Ok(())
    }

    pub fn prune_backed_up_before(&self, cutoff_date: &str) -> Result<usize> {
        let keys = {
            let connection = self.db.reader()?;
            let mut statement = connection.prepare(
                "SELECT partition_key FROM partition_state GROUP BY partition_key \
                 HAVING bool_and(state = 'backed_up')",
            )?;
            statement
                .query_map([], |row| row.get::<_, String>(0))?
                .collect::<duckdb::Result<Vec<_>>>()?
        };
        let expired = keys
            .into_iter()
            .filter(|key| partition_date(key).is_some_and(|date| date < cutoff_date))
            .collect::<Vec<_>>();
        let _visibility = self.db.write_parquet()?;
        expired.iter().try_for_each(|key| {
            let directory = partition_directory(&self.parts_root, key);
            if directory.exists() {
                std::fs::remove_dir_all(&directory)?;
            }
            self.db.writer()?.execute(
                "DELETE FROM partition_state WHERE partition_key = ?",
                params![key],
            )?;
            Ok::<_, crate::error::TelemetryError>(())
        })?;
        Ok(expired.len())
    }
}

fn partition_date(key: &str) -> Option<&str> {
    key.rsplit('/')
        .next()
        .map(|segment| segment.strip_prefix("date=").unwrap_or(segment))
}
