use std::sync::Arc;

use anyhow::Result;

use super::{DuckLogStore, IngestLogEntry};
use crate::logs::{
    LogEntry, LogHistogramBucket, LogHistogramQuery, LogReadQuery, LogReadScope, LogStore,
};

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
                    .append_telemetry(
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

    pub async fn read_logs(&self, query: LogReadQuery) -> Result<Vec<LogEntry>> {
        match self {
            Self::Sqlite(store) => store.read_logs(query).await,
            Self::Duck(store) => store.read_logs(query).await,
        }
    }

    pub async fn read_log_histogram(
        &self,
        query: LogHistogramQuery,
    ) -> Result<Vec<LogHistogramBucket>> {
        match self {
            Self::Sqlite(store) => store.read_log_histogram(query).await,
            Self::Duck(store) => store.read_log_histogram(query).await,
        }
    }

    pub async fn latest_log_seq(&self, scope: &LogReadScope) -> Result<i64> {
        match self {
            Self::Sqlite(store) => store.latest_log_seq().await,
            Self::Duck(store) => store.latest_log_seq(scope).await,
        }
    }

    pub async fn read_ingress_traffic(
        &self,
        service_id: &str,
        from: i64,
        to: i64,
        limit: usize,
    ) -> Result<crate::logs::IngressTrafficBreakdown> {
        delegate!(self, read_ingress_traffic(service_id, from, to, limit))
    }

    pub async fn read_blocked_ingress_traffic(
        &self,
        from: i64,
        to: i64,
        limit: usize,
    ) -> Result<crate::logs::IngressTrafficBreakdown> {
        delegate!(self, read_blocked_ingress_traffic(from, to, limit))
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
