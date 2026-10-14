use duckdb::params;
use serde::Deserialize;

use super::LogStore;
use crate::error::{Result, TelemetryError};
use crate::traffic::TrafficPoint;

pub(super) const TRAFFIC_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS traffic_metrics (
    ts BIGINT NOT NULL,
    service_id VARCHAR NOT NULL,
    deployment_id VARCHAR,
    status_code USMALLINT NOT NULL,
    method VARCHAR NOT NULL,
    requests UBIGINT NOT NULL,
    bytes_in UBIGINT NOT NULL,
    bytes_out UBIGINT NOT NULL,
    lat_le_1s UBIGINT NOT NULL,
    lat_le_5s UBIGINT NOT NULL,
    lat_le_10s UBIGINT NOT NULL,
    lat_total UBIGINT NOT NULL
);
CREATE INDEX IF NOT EXISTS traffic_service_ts ON traffic_metrics (service_id, ts);
"#;

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficQuery {
    pub service_id: String,
    pub from: i64,
    pub to: i64,
}

impl LogStore {
    pub fn insert_traffic(&self, points: &[TrafficPoint]) -> Result<()> {
        let mut connection = self.db.writer()?;
        let transaction = connection.transaction()?;
        {
            let mut statement = transaction.prepare_cached(
                "INSERT INTO traffic_metrics VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            )?;
            points.iter().try_for_each(|point| {
                statement.execute(params![
                    point.ts,
                    point.service_id,
                    point.deployment_id,
                    point.status_code,
                    point.method,
                    point.requests,
                    point.bytes_in,
                    point.bytes_out,
                    point.lat_le_1s,
                    point.lat_le_5s,
                    point.lat_le_10s,
                    point.lat_total,
                ])?;
                Ok::<_, TelemetryError>(())
            })?;
        }
        transaction.commit()?;
        Ok(())
    }

    pub async fn read_traffic(&self, query: TrafficQuery) -> Result<Vec<TrafficPoint>> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let connection = db.reader()?;
            let mut statement = connection.prepare(
                "SELECT ts, service_id, deployment_id, status_code, method, requests, bytes_in, \
                 bytes_out, lat_le_1s, lat_le_5s, lat_le_10s, lat_total FROM traffic_metrics \
                 WHERE service_id = ? AND ts >= ? AND ts <= ? ORDER BY ts",
            )?;
            let rows =
                statement.query_map(params![query.service_id, query.from, query.to], |row| {
                    Ok(TrafficPoint {
                        ts: row.get(0)?,
                        service_id: row.get(1)?,
                        deployment_id: row.get(2)?,
                        status_code: row.get(3)?,
                        method: row.get(4)?,
                        requests: row.get(5)?,
                        bytes_in: row.get(6)?,
                        bytes_out: row.get(7)?,
                        lat_le_1s: row.get(8)?,
                        lat_le_5s: row.get(9)?,
                        lat_le_10s: row.get(10)?,
                        lat_total: row.get(11)?,
                    })
                })?;
            Ok(rows.collect::<duckdb::Result<Vec<_>>>()?)
        })
        .await?
    }

    pub fn prune_traffic(&self, before_ts: i64) -> Result<usize> {
        Ok(self.db.maintenance()?.execute(
            "DELETE FROM traffic_metrics WHERE ts < ?",
            params![before_ts],
        )?)
    }
}
