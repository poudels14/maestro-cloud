use std::collections::BTreeMap;

use duckdb::{params_from_iter, types::Value};
use serde::{Deserialize, Serialize};

use super::query::log_source;
use super::{LogStore, search_value};
use crate::error::Result;
use crate::search::LogSearchQuery;

const MINUTE_MS: i64 = 60_000;
const HOUR_MS: i64 = 60 * MINUTE_MS;
const MAX_BUCKETS: i64 = 1000;
pub const MAX_HISTOGRAM_RANGE_MS: i64 = 7 * 24 * HOUR_MS;
const STATUS_FIELDS: [&str; 5] = [
    "DownstreamStatus",
    "status",
    "status_code",
    "statusCode",
    "http.status_code",
];

#[derive(Clone, Copy, Debug, Default, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub enum HistogramGroup {
    #[default]
    Level,
    Status,
}

#[derive(Clone, Debug)]
pub struct HistogramQuery {
    pub search: Option<LogSearchQuery>,
    pub from: i64,
    pub to: i64,
    pub bucket_ms: i64,
    pub group_by: HistogramGroup,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogHistogram {
    pub from: i64,
    pub to: i64,
    pub bucket_ms: i64,
    pub buckets: Vec<HistogramBucket>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct HistogramBucket {
    pub ts: i64,
    pub count: u64,
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub levels: BTreeMap<String, u64>,
}

impl LogStore {
    pub async fn log_histogram(&self, query: HistogramQuery) -> Result<LogHistogram> {
        let db = self.db.clone();
        let parts_root = self.parts_root.clone();
        tokio::task::spawn_blocking(move || {
            let _visibility = db.read_parquet()?;
            let group = match query.group_by {
                HistogramGroup::Level => "lower(level)".to_string(),
                HistogramGroup::Status => status_class_sql(),
            };
            let mut sql = format!(
                "SELECT ts - ts % ? AS bucket, {group} AS grp, count(*) FROM ({}) q \
                 WHERE ts >= ? AND ts <= ?",
                log_source(&parts_root)
            );
            let mut values = vec![
                Value::BigInt(query.bucket_ms),
                Value::BigInt(query.from),
                Value::BigInt(query.to),
            ];
            if let Some(search) = &query.search {
                let compiled = search.compile();
                sql.push_str(" AND ");
                sql.push_str(&compiled.sql);
                values.extend(compiled.values.into_iter().map(search_value));
            }
            sql.push_str(" GROUP BY bucket, grp ORDER BY bucket");
            let connection = db.reader()?;
            let mut statement = connection.prepare(&sql)?;
            let rows = statement.query_map(params_from_iter(values.iter()), |row| {
                Ok((
                    row.get::<_, i64>(0)?,
                    row.get::<_, Option<String>>(1)?,
                    row.get::<_, i64>(2)?,
                ))
            })?;
            let counts = rows.collect::<duckdb::Result<Vec<_>>>()?;
            Ok(fill_buckets(query.from, query.to, query.bucket_ms, counts))
        })
        .await?
    }
}

pub fn histogram_bucket(from: i64, to: i64, requested: Option<i64>) -> i64 {
    let range = (to - from).max(0);
    let default = match range {
        range if range <= HOUR_MS => MINUTE_MS,
        range if range <= 6 * HOUR_MS => 5 * MINUTE_MS,
        range if range <= 24 * HOUR_MS => 10 * MINUTE_MS,
        _ => 2 * HOUR_MS,
    };
    requested
        .unwrap_or(default)
        .max(range / MAX_BUCKETS)
        .max(MINUTE_MS)
}

pub fn fill_buckets(
    from: i64,
    to: i64,
    bucket_ms: i64,
    counts: Vec<(i64, Option<String>, i64)>,
) -> LogHistogram {
    let first = from - from.rem_euclid(bucket_ms);
    let mut buckets = (0..)
        .map(|index| first + index * bucket_ms)
        .take_while(|ts| *ts <= to)
        .map(|ts| {
            (
                ts,
                HistogramBucket {
                    ts,
                    ..HistogramBucket::default()
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    counts.into_iter().for_each(|(ts, group, count)| {
        let bucket = buckets.entry(ts).or_insert_with(|| HistogramBucket {
            ts,
            ..HistogramBucket::default()
        });
        let count = u64::try_from(count).unwrap_or_default();
        bucket.count += count;
        if let Some(group) = group {
            *bucket.levels.entry(group).or_default() += count;
        }
    });
    LogHistogram {
        from,
        to,
        bucket_ms,
        buckets: buckets.into_values().collect(),
    }
}

fn status_class_sql() -> String {
    let codes = STATUS_FIELDS
        .iter()
        .map(|field| {
            format!("TRY_CAST(json_extract_string(attributes, '$.\"{field}\"') AS INTEGER)")
        })
        .collect::<Vec<_>>()
        .join(", ");
    format!(
        "CASE WHEN coalesce({codes}) BETWEEN 100 AND 599 \
         THEN CAST(coalesce({codes}) // 100 AS VARCHAR) || 'xx' ELSE NULL END"
    )
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn picks_bucket_sizes_like_v1() {
        assert_eq!(histogram_bucket(0, HOUR_MS, None), MINUTE_MS);
        assert_eq!(histogram_bucket(0, 5 * HOUR_MS, None), 5 * MINUTE_MS);
        assert_eq!(histogram_bucket(0, 20 * HOUR_MS, None), 10 * MINUTE_MS);
        assert_eq!(histogram_bucket(0, 7 * 24 * HOUR_MS, None), 2 * HOUR_MS);
        assert_eq!(histogram_bucket(0, HOUR_MS, Some(1)), MINUTE_MS);
    }

    #[test]
    fn fills_gaps_between_counted_buckets() {
        let histogram = fill_buckets(
            61_000,
            240_000,
            60_000,
            vec![
                (60_000, Some("info".to_string()), 2),
                (180_000, Some("error".to_string()), 1),
            ],
        );
        let shape = histogram
            .buckets
            .iter()
            .map(|bucket| (bucket.ts, bucket.count))
            .collect::<Vec<_>>();
        assert_eq!(
            shape,
            [(60_000, 2), (120_000, 0), (180_000, 1), (240_000, 0)]
        );
        assert_eq!(histogram.buckets[2].levels["error"], 1);
    }
}
