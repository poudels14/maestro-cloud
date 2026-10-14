use duckdb::{Transaction, params};
use serde::Deserialize;

use super::LogStore;
use crate::access_log::{
    AccessEvent, BLOCKED_ROUTER_PREFIX, IngressTrafficBreakdown, TrafficBreakdownEntry,
};
use crate::error::Result;

pub(super) const INGRESS_SCHEMA: &str = r#"
CREATE TABLE IF NOT EXISTS ingress_traffic (
    bucket_at_ms BIGINT NOT NULL,
    router VARCHAR NOT NULL,
    dimension VARCHAR NOT NULL,
    value VARCHAR NOT NULL,
    status_code USMALLINT NOT NULL,
    requests UBIGINT NOT NULL,
    last_seen_at_ms BIGINT NOT NULL,
    PRIMARY KEY (bucket_at_ms, router, dimension, value, status_code)
);
"#;

const UPSERT_TRAFFIC: &str = r#"
INSERT INTO ingress_traffic VALUES (?, ?, ?, ?, ?, 1, ?)
ON CONFLICT (bucket_at_ms, router, dimension, value, status_code) DO UPDATE SET
    requests = ingress_traffic.requests + 1,
    last_seen_at_ms = greatest(ingress_traffic.last_seen_at_ms, excluded.last_seen_at_ms)
"#;

const DIMENSIONS: [&str; 2] = ["ip", "path"];

#[derive(Clone, Copy, Debug, Default, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum TrafficScope {
    #[default]
    Cluster,
    Blocked,
    Service,
}

#[derive(Clone, Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTrafficQuery {
    #[serde(default)]
    pub scope: TrafficScope,
    pub service_id: Option<String>,
    pub from: i64,
    pub to: i64,
    pub limit: usize,
}

pub(super) fn record_access(transaction: &Transaction, event: &AccessEvent) -> Result<()> {
    let mut statement = transaction.prepare_cached(UPSERT_TRAFFIC)?;
    [("ip", &event.client_ip), ("path", &event.path)]
        .into_iter()
        .try_for_each(|(dimension, value)| {
            statement.execute(params![
                event.bucket_at_ms,
                event.router,
                dimension,
                value,
                event.status_code,
                event.at_ms,
            ])?;
            Ok(())
        })
}

impl LogStore {
    pub async fn read_ingress_traffic(
        &self,
        query: IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown> {
        let db = self.db.clone();
        tokio::task::spawn_blocking(move || {
            let connection = db.reader()?;
            let (router_filter, first, second) = match query.scope {
                TrafficScope::Cluster => (
                    "NOT starts_with(router, ?) AND ? IS NOT NULL",
                    BLOCKED_ROUTER_PREFIX.to_string(),
                    String::new(),
                ),
                TrafficScope::Blocked => (
                    "starts_with(router, ?) AND ? IS NOT NULL",
                    BLOCKED_ROUTER_PREFIX.to_string(),
                    String::new(),
                ),
                TrafficScope::Service => {
                    let service_id = query.service_id.clone().unwrap_or_default();
                    (
                        "(router = ? OR regexp_full_match(router, ?))",
                        format!("svc-{service_id}@file"),
                        format!("svc-{}-d[0-9a-z]+-[0-9]+@file", regex_escape(&service_id)),
                    )
                }
            };
            let sql = format!(
                r#"
                WITH filtered AS (
                    SELECT value, status_code, sum(requests) AS requests, max(last_seen_at_ms) AS last_seen
                    FROM ingress_traffic
                    WHERE dimension = ? AND bucket_at_ms >= ? AND bucket_at_ms <= ? AND {router_filter}
                    GROUP BY value, status_code
                ),
                top_values AS (
                    SELECT value FROM filtered GROUP BY value
                    ORDER BY sum(requests) DESC, max(last_seen) DESC, value
                    LIMIT ?
                )
                SELECT value, status_code, requests, last_seen FROM filtered
                WHERE value IN (SELECT value FROM top_values)
                ORDER BY value, status_code
                "#
            );
            let from = query.from - query.from.rem_euclid(60_000);
            let mut breakdown = DIMENSIONS.iter().map(|dimension| {
                let mut statement = connection.prepare(&sql)?;
                let rows = statement.query_map(
                    params![dimension, from, query.to, first, second, query.limit as i64],
                    |row| {
                        Ok(TrafficBreakdownEntry {
                            value: row.get(0)?,
                            status_code: row.get(1)?,
                            requests: row.get(2)?,
                            last_seen_at_ms: row.get(3)?,
                        })
                    },
                )?;
                Ok::<_, crate::error::TelemetryError>(rows.collect::<duckdb::Result<Vec<_>>>()?)
            });
            Ok(IngressTrafficBreakdown {
                by_ip: breakdown.next().transpose()?.unwrap_or_default(),
                by_path: breakdown.next().transpose()?.unwrap_or_default(),
            })
        })
        .await?
    }

    pub fn prune_ingress_traffic(&self, before_ts: i64) -> Result<usize> {
        Ok(self.db.maintenance()?.execute(
            "DELETE FROM ingress_traffic WHERE bucket_at_ms < ?",
            params![before_ts],
        )?)
    }
}

fn regex_escape(value: &str) -> String {
    value
        .chars()
        .map(
            |character| match character.is_ascii_alphanumeric() || character == '-' {
                true => character.to_string(),
                false => format!("\\{character}"),
            },
        )
        .collect()
}
