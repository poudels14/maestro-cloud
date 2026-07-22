use std::collections::BTreeMap;

use async_trait::async_trait;

use crate::StatsMetricPoint;

/// Largest operational-history page accepted by a node-local store.
pub const MAXIMUM_STATS_METRIC_QUERY_LIMIT: usize = 10_000;

const MAXIMUM_METRIC_NAME_BYTES: usize = 256;
const MAXIMUM_LABELS: usize = 64;
const MAXIMUM_LABEL_KEY_BYTES: usize = 128;
const MAXIMUM_LABEL_VALUE_BYTES: usize = 1_024;

/// Outcome of one atomic, replay-safe operational metric append.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct StatsMetricAppendReport {
    /// New metric identities committed by this append.
    pub committed: usize,
    /// Exact metric identities already present in the store.
    pub deduplicated: usize,
}

/// Validated node-local operational-history query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct StatsMetricQuery {
    name: Option<String>,
    from: i64,
    to: i64,
    limit: usize,
}

impl StatsMetricQuery {
    /// Creates an inclusive wall-clock range with an optional exact metric-name match.
    pub fn new(
        name: Option<String>,
        from: i64,
        to: i64,
        limit: usize,
    ) -> Result<Self, StatsMetricStoreError> {
        if from > to {
            return Err(rejected("stats metric range must not be inverted"));
        }
        if !(1..=MAXIMUM_STATS_METRIC_QUERY_LIMIT).contains(&limit) {
            return Err(rejected(format!(
                "stats metric query limit must be between 1 and {MAXIMUM_STATS_METRIC_QUERY_LIMIT}"
            )));
        }
        if let Some(name) = name.as_deref() {
            validate_metric_name(name)?;
        }
        Ok(Self {
            name,
            from,
            to,
            limit,
        })
    }

    /// Returns the optional exact metric name.
    pub fn name(&self) -> Option<&str> {
        self.name.as_deref()
    }

    /// Returns the inclusive lower wall-clock bound.
    pub fn from(&self) -> i64 {
        self.from
    }

    /// Returns the inclusive upper wall-clock bound.
    pub fn to(&self) -> i64 {
        self.to
    }

    /// Returns the maximum number of points to return.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Durable append and ordered query boundary for controller and backup history.
#[async_trait]
pub trait StatsMetricStore: Send + Sync {
    /// Atomically appends points, deduplicating exact identity-and-value replays.
    async fn append_stats_metrics(
        &self,
        points: &[StatsMetricPoint],
    ) -> Result<StatsMetricAppendReport, StatsMetricStoreError>;

    /// Returns points ordered by timestamp, name, then labels.
    async fn query_stats_metrics(
        &self,
        query: &StatsMetricQuery,
    ) -> Result<Vec<StatsMetricPoint>, StatsMetricStoreError>;
}

/// Validates one point before a backend starts an append transaction.
pub fn validate_stats_metric_point(point: &StatsMetricPoint) -> Result<(), StatsMetricStoreError> {
    validate_metric_name(&point.name)?;
    if !point.value.is_finite() {
        return Err(rejected("stats metric value must be finite"));
    }
    validate_labels(&point.labels)
}

fn validate_metric_name(name: &str) -> Result<(), StatsMetricStoreError> {
    if name.is_empty() {
        return Err(rejected("stats metric name must not be empty"));
    }
    if name.len() > MAXIMUM_METRIC_NAME_BYTES {
        return Err(rejected(format!(
            "stats metric name exceeds {MAXIMUM_METRIC_NAME_BYTES} bytes"
        )));
    }
    Ok(())
}

fn validate_labels(labels: &BTreeMap<String, String>) -> Result<(), StatsMetricStoreError> {
    if labels.len() > MAXIMUM_LABELS {
        return Err(rejected(format!(
            "stats metric has more than {MAXIMUM_LABELS} labels"
        )));
    }
    for (key, value) in labels {
        if key.is_empty() {
            return Err(rejected("stats metric label key must not be empty"));
        }
        if key.len() > MAXIMUM_LABEL_KEY_BYTES {
            return Err(rejected(format!(
                "stats metric label key exceeds {MAXIMUM_LABEL_KEY_BYTES} bytes"
            )));
        }
        if value.len() > MAXIMUM_LABEL_VALUE_BYTES {
            return Err(rejected(format!(
                "stats metric label value exceeds {MAXIMUM_LABEL_VALUE_BYTES} bytes"
            )));
        }
    }
    Ok(())
}

fn rejected(message: impl Into<String>) -> StatsMetricStoreError {
    StatsMetricStoreError::Rejected {
        message: message.into(),
    }
}

/// Failure to append or query operational metric history.
#[derive(Debug, thiserror::Error)]
pub enum StatsMetricStoreError {
    /// Content or a query permanently violates the storage contract.
    #[error("stats metric store rejected request: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The store is temporarily unable to serve a valid request.
    #[error("stats metric store is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}
