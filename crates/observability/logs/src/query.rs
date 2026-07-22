use std::collections::BTreeMap;

use async_trait::async_trait;
use kernel_api::{BuildId, DeploymentId, ServiceId, Timestamp};
use logql::LogQuery;
use serde::{Deserialize, Serialize};

use crate::{LogSequence, SequencedLogEntry};

/// Largest page accepted by the storage-neutral log query contract.
pub const MAXIMUM_LOG_QUERY_LIMIT: usize = 10_000;
const MAXIMUM_HISTOGRAM_BUCKETS: i64 = 2_000;

/// Typed ownership boundary for one node-local log read.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LogQueryScope {
    /// Every normalized record stored on this node.
    All,
    /// Workload records owned by one service, across deployments.
    Service(ServiceId),
    /// Workload records owned by one immutable deployment.
    Deployment(DeploymentId),
    /// Every system-component record stored on this node.
    System,
    /// Records emitted by one exact system component.
    SystemComponent(String),
    /// Records produced by one artifact build.
    Build(BuildId),
}

/// Stable ordering for a paginated node-local log read.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogReadOrder {
    /// Oldest accepted store sequence first.
    OldestFirst,
    /// Newest accepted store sequence first.
    NewestFirst,
}

/// Exclusive sequence boundary interpreted independently from display order.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LogReadCursor {
    /// Select sequences strictly greater than this value.
    After(LogSequence),
    /// Select sequences strictly less than this value.
    Before(LogSequence),
}

/// Validated storage-neutral request for normalized logs.
#[derive(Debug, Clone, PartialEq)]
pub struct LogReadQuery {
    scope: LogQueryScope,
    search: Option<LogQuery>,
    from: Option<Timestamp>,
    to: Option<Timestamp>,
    cursor: Option<LogReadCursor>,
    order: LogReadOrder,
    limit: usize,
}

impl LogReadQuery {
    /// Creates a bounded request without a time, cursor, or LogQL filter.
    pub fn new(
        scope: LogQueryScope,
        order: LogReadOrder,
        limit: usize,
    ) -> Result<Self, LogQueryError> {
        validate_scope(&scope)?;
        validate_limit(limit)?;
        Ok(Self {
            scope,
            search: None,
            from: None,
            to: None,
            cursor: None,
            order,
            limit,
        })
    }

    /// Applies a validated LogQL expression.
    pub fn with_search(mut self, search: LogQuery) -> Self {
        self.search = Some(search);
        self
    }

    /// Applies an optional half-open event-time interval `[from, to)`.
    pub fn within(
        mut self,
        from: Option<Timestamp>,
        to: Option<Timestamp>,
    ) -> Result<Self, LogQueryError> {
        if from.zip(to).is_some_and(|(from, to)| from.0 >= to.0) {
            return Err(invalid("log query start must be earlier than its end"));
        }
        self.from = from;
        self.to = to;
        Ok(self)
    }

    /// Applies one exclusive store-sequence cursor.
    pub fn with_cursor(mut self, cursor: LogReadCursor) -> Self {
        self.cursor = Some(cursor);
        self
    }

    /// Returns the typed ownership boundary.
    pub fn scope(&self) -> &LogQueryScope {
        &self.scope
    }

    /// Returns the optional validated LogQL expression.
    pub fn search(&self) -> Option<&LogQuery> {
        self.search.as_ref()
    }

    /// Returns the inclusive event-time lower bound.
    pub fn from(&self) -> Option<Timestamp> {
        self.from
    }

    /// Returns the exclusive event-time upper bound.
    pub fn to(&self) -> Option<Timestamp> {
        self.to
    }

    /// Returns the optional exclusive sequence cursor.
    pub fn cursor(&self) -> Option<LogReadCursor> {
        self.cursor
    }

    /// Returns the requested row ordering.
    pub fn order(&self) -> LogReadOrder {
        self.order
    }

    /// Replaces the display order while preserving filters and cursor state.
    pub fn with_order(mut self, order: LogReadOrder) -> Self {
        self.order = order;
        self
    }

    /// Returns the validated maximum row count.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Supported histogram grouping dimensions.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub enum LogHistogramGroupBy {
    /// Normalize severity names to lowercase.
    #[default]
    Level,
    /// Group a valid canonical HTTP response status into `1xx` through `5xx`.
    HttpStatusClass,
}

/// Validated histogram request over a non-empty event-time interval.
#[derive(Debug, Clone, PartialEq)]
pub struct LogHistogramQuery {
    scope: LogQueryScope,
    search: Option<LogQuery>,
    from: Timestamp,
    to: Timestamp,
    bucket_ms: i64,
    group_by: LogHistogramGroupBy,
}

impl LogHistogramQuery {
    /// Creates a bounded histogram request without a LogQL filter.
    pub fn new(
        scope: LogQueryScope,
        from: Timestamp,
        to: Timestamp,
        bucket_ms: i64,
        group_by: LogHistogramGroupBy,
    ) -> Result<Self, LogQueryError> {
        validate_scope(&scope)?;
        if from.0 >= to.0 {
            return Err(invalid("log histogram start must be earlier than its end"));
        }
        if bucket_ms <= 0 {
            return Err(invalid("log histogram bucket must be positive"));
        }
        let span = to.0.saturating_sub(from.0);
        let buckets = span.saturating_add(bucket_ms - 1) / bucket_ms;
        if buckets > MAXIMUM_HISTOGRAM_BUCKETS {
            return Err(invalid("log histogram cannot exceed 2000 buckets"));
        }
        Ok(Self {
            scope,
            search: None,
            from,
            to,
            bucket_ms,
            group_by,
        })
    }

    /// Applies a validated LogQL expression.
    pub fn with_search(mut self, search: LogQuery) -> Self {
        self.search = Some(search);
        self
    }

    /// Returns the typed ownership boundary.
    pub fn scope(&self) -> &LogQueryScope {
        &self.scope
    }

    /// Returns the optional validated LogQL expression.
    pub fn search(&self) -> Option<&LogQuery> {
        self.search.as_ref()
    }

    /// Returns the inclusive event-time lower bound.
    pub fn from(&self) -> Timestamp {
        self.from
    }

    /// Returns the exclusive event-time upper bound.
    pub fn to(&self) -> Timestamp {
        self.to
    }

    /// Returns the positive bucket width in milliseconds.
    pub fn bucket_ms(&self) -> i64 {
        self.bucket_ms
    }

    /// Returns the requested grouping dimension.
    pub fn group_by(&self) -> LogHistogramGroupBy {
        self.group_by
    }
}

/// One event-time bucket with stable, sorted group counts.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogHistogramBucket {
    /// Inclusive bucket start in Unix milliseconds.
    pub bucket_at: Timestamp,
    /// Total records in this bucket.
    pub count: u64,
    /// Counts keyed by normalized severity or status class.
    pub groups: BTreeMap<String, u64>,
}

/// Query construction failed before reaching a backend.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid log query: {message}")]
pub struct LogQueryError {
    message: String,
}

/// Backend-neutral log read and histogram boundary.
#[async_trait]
pub trait LogQueryStore: Send + Sync {
    /// Reads normalized records matching the validated request.
    async fn query_logs(
        &self,
        query: &LogReadQuery,
    ) -> Result<Vec<SequencedLogEntry>, LogQueryStoreError>;

    /// Aggregates matching records into event-time buckets.
    async fn query_log_histogram(
        &self,
        query: &LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>, LogQueryStoreError>;
}

/// A validated log query could not be completed by its backend.
#[derive(Debug, thiserror::Error)]
pub enum LogQueryStoreError {
    /// Persisted data or an execution bound was invalid.
    #[error("log query store rejected request: {message}")]
    Rejected {
        /// Stable validation detail.
        message: String,
    },
    /// Durable query data is temporarily unavailable.
    #[error("log query store is unavailable: {message}")]
    Unavailable {
        /// Safe backend diagnostic.
        message: String,
    },
}

fn validate_scope(scope: &LogQueryScope) -> Result<(), LogQueryError> {
    if let LogQueryScope::SystemComponent(component) = scope
        && (component.is_empty() || component.len() > 128)
    {
        return Err(invalid("system component must contain 1-128 bytes"));
    }
    Ok(())
}

fn validate_limit(limit: usize) -> Result<(), LogQueryError> {
    if !(1..=MAXIMUM_LOG_QUERY_LIMIT).contains(&limit) {
        return Err(invalid("log query limit must be between 1 and 10000"));
    }
    Ok(())
}

fn invalid(message: impl Into<String>) -> LogQueryError {
    LogQueryError {
        message: message.into(),
    }
}
