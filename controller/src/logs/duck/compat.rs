use std::sync::Arc;

use anyhow::Result;

use super::{DuckLogStore, IngestLogEntry};
use crate::logs::{LogEntry, LogOrigin, LogStore};

/// Lets existing controller SQLite storage and probe DuckDB storage share the API surface.
pub enum TelemetryStore {
    Sqlite(Arc<LogStore>),
    Duck(Arc<DuckLogStore>),
}

macro_rules! delegate {
    ($self:expr, $method:ident ( $($arg:expr),* $(,)? )) => {
        match $self {
            Self::Sqlite(store) => store.$method($($arg),*).await,
            Self::Duck(store) => store.$method($($arg),*).await,
        }
    };
}

impl TelemetryStore {
    pub fn sqlite(store: Arc<LogStore>) -> Arc<Self> {
        Arc::new(Self::Sqlite(store))
    }

    pub fn duck(store: Arc<DuckLogStore>) -> Arc<Self> {
        Arc::new(Self::Duck(store))
    }

    pub async fn append_ingest(&self, entries: &[IngestLogEntry]) -> Result<()> {
        match self {
            Self::Sqlite(store) => {
                store
                    .append(
                        &entries
                            .iter()
                            .map(|value| value.entry.clone())
                            .collect::<Vec<_>>(),
                    )
                    .await
            }
            Self::Duck(store) => store.append_ingest(entries).await,
        }
    }

    pub async fn read_tail_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(self, read_tail_by_prefix_origin(prefix, origin, limit))
    }

    pub async fn read_after_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(
            self,
            read_after_by_prefix_origin(prefix, origin, after, limit)
        )
    }

    pub async fn read_before_by_prefix_origin(
        &self,
        prefix: &str,
        origin: Option<LogOrigin>,
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(
            self,
            read_before_by_prefix_origin(prefix, origin, before, limit)
        )
    }

    pub async fn read_tail(&self, source: &str, limit: usize) -> Result<Vec<LogEntry>> {
        delegate!(self, read_tail(source, limit))
    }

    pub async fn read_after_for_source(
        &self,
        source: &str,
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(self, read_after_for_source(source, after, limit))
    }

    pub async fn read_before_for_source(
        &self,
        source: &str,
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(self, read_before_for_source(source, before, limit))
    }

    pub async fn read_tail_sources(&self, sources: &[&str], limit: usize) -> Result<Vec<LogEntry>> {
        delegate!(self, read_tail_sources(sources, limit))
    }

    pub async fn read_after_sources(
        &self,
        sources: &[&str],
        after: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(self, read_after_sources(sources, after, limit))
    }

    pub async fn read_before_sources(
        &self,
        sources: &[&str],
        before: i64,
        limit: usize,
    ) -> Result<Vec<LogEntry>> {
        delegate!(self, read_before_sources(sources, before, limit))
    }

    pub async fn append_metrics(&self, entries: &[crate::metrics::MetricPoint]) -> Result<()> {
        delegate!(self, append_metrics(entries))
    }

    pub async fn append_stats_metrics(
        &self,
        entries: &[crate::cluster_stats::StatsMetricPoint],
    ) -> Result<()> {
        delegate!(self, append_stats_metrics(entries))
    }

    pub async fn read_stats_metrics(
        &self,
        name: Option<&str>,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::cluster_stats::StatsMetricPoint>> {
        delegate!(self, read_stats_metrics(name, from, to))
    }

    pub async fn read_metrics(
        &self,
        source: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        delegate!(self, read_metrics(source, from, to))
    }

    pub async fn read_metrics_by_prefix(
        &self,
        prefix: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::MetricPoint>> {
        delegate!(self, read_metrics_by_prefix(prefix, from, to))
    }

    pub async fn append_traffic_metrics(
        &self,
        entries: &[crate::metrics::TrafficPoint],
    ) -> Result<()> {
        delegate!(self, append_traffic_metrics(entries))
    }

    pub async fn read_traffic_metrics(
        &self,
        service_id: &str,
        from: i64,
        to: i64,
    ) -> Result<Vec<crate::metrics::TrafficPoint>> {
        delegate!(self, read_traffic_metrics(service_id, from, to))
    }
}
