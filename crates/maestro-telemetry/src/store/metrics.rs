use duckdb::params;
use serde::Deserialize;

use super::LogStore;
use crate::error::{Result, TelemetryError};
use crate::metrics::MetricPoint;

pub(super) const METRICS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS metrics (
    ts BIGINT NOT NULL,
    source VARCHAR NOT NULL,
    cpu_percent DOUBLE NOT NULL,
    memory_bytes UBIGINT NOT NULL,
    memory_limit_bytes UBIGINT NOT NULL,
    net_rx_bytes UBIGINT NOT NULL,
    net_tx_bytes UBIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS metrics_source_ts ON metrics (source, ts);
"#;

const INSERT_METRIC: &str = "INSERT INTO metrics VALUES (?, ?, ?, ?, ?, ?, ?)";

#[derive(Clone, Debug, Default, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct MetricsQuery {
    pub source: Option<String>,
    pub source_prefix: Option<String>,
    pub from: i64,
    pub to: i64,
}

impl LogStore {
    pub fn insert_metrics(&self, points: &[MetricPoint]) -> Result<()> {
        let mut connection = self.db.writer()?;
        let transaction = connection.transaction()?;
        {
            let mut statement = transaction.prepare_cached(INSERT_METRIC)?;
            points.iter().try_for_each(|point| {
                statement.execute(params![
                    point.ts,
                    point.source,
                    point.cpu_percent,
                    point.memory_bytes,
                    point.memory_limit_bytes,
                    point.net_rx_bytes,
                    point.net_tx_bytes,
                ])?;
                Ok::<_, TelemetryError>(())
            })?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub async fn read_metrics(&self, query: MetricsQuery) -> Result<Vec<MetricPoint>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let connection = db.reader()?;
            let source_filter = match (&query.source, &query.source_prefix) {
                (Some(_), _) => "source = ?",
                (None, Some(_)) => "starts_with(source, ?)",
                (None, None) => "source IS NOT NULL OR ? IS NULL",
            };
            let mut statement = connection.prepare(&format!(
                "SELECT ts, source, cpu_percent, memory_bytes, memory_limit_bytes, net_rx_bytes, net_tx_bytes \
                 FROM metrics WHERE ({source_filter}) AND ts >= ? AND ts <= ? ORDER BY ts, source"
            ))?;
            let filter_value = query.source.clone().or(query.source_prefix.clone());
            let rows = statement.query_map(params![filter_value, query.from, query.to], |row| {
                Ok(MetricPoint {
                    ts: row.get(0)?,
                    source: row.get(1)?,
                    cpu_percent: row.get(2)?,
                    memory_bytes: row.get(3)?,
                    memory_limit_bytes: row.get(4)?,
                    net_rx_bytes: row.get(5)?,
                    net_tx_bytes: row.get(6)?,
                })
            })?;
            Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub fn prune_metrics(&self, before_ts: i64) -> Result<usize> {
        Ok(self
            .db
            .maintenance()?
            .execute("DELETE FROM metrics WHERE ts < ?", params![before_ts])?)
    }
}
