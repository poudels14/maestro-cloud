use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use serde::Serialize;

use crate::{
    HostMetricPoint, HostMetricSink, MetricHttpRequest, MetricHttpTransport, MetricSink,
    MetricSinkError, MetricSinkId, SequencedHostMetricPoint, SequencedMetricPoint,
    WorkloadMetricPoint,
};

const DATADOG_GAUGE: u8 = 3;
const REPLICA_INDEX_LABEL: &str = "maestro.replica-index";
const MAX_GLOBAL_TAGS: usize = 256;
const MAX_TAG_BYTES: usize = 200;

/// Validated Datadog Metrics intake configuration.
///
/// This type intentionally omits `Debug` so API credentials cannot be formatted accidentally.
pub struct DatadogMetricSinkSettings {
    api_key: String,
    endpoint: String,
    cluster_name: String,
    hostname: String,
    global_tags: Vec<String>,
}

impl DatadogMetricSinkSettings {
    /// Builds the official series endpoint for a validated Datadog site.
    pub fn new(
        api_key: impl Into<String>,
        site: impl Into<String>,
        cluster_name: impl Into<String>,
        hostname: impl Into<String>,
        global_tags: Vec<String>,
    ) -> Result<Self, DatadogMetricSinkSettingsError> {
        let site = site.into();
        if !valid_datadog_site(&site) {
            return Err(DatadogMetricSinkSettingsError::InvalidSite);
        }
        Self::with_endpoint(
            api_key,
            format!("https://api.{site}/api/v2/series"),
            cluster_name,
            hostname,
            global_tags,
        )
    }

    pub(crate) fn with_endpoint(
        api_key: impl Into<String>,
        endpoint: String,
        cluster_name: impl Into<String>,
        hostname: impl Into<String>,
        global_tags: Vec<String>,
    ) -> Result<Self, DatadogMetricSinkSettingsError> {
        let api_key = api_key.into();
        let cluster_name = cluster_name.into();
        let hostname = hostname.into();
        if api_key.is_empty() || api_key.len() > 1_024 {
            return Err(DatadogMetricSinkSettingsError::InvalidApiKey);
        }
        if !(endpoint.starts_with("https://") || endpoint.starts_with("http://")) {
            return Err(DatadogMetricSinkSettingsError::InvalidEndpoint);
        }
        if cluster_name.is_empty() || hostname.is_empty() {
            return Err(DatadogMetricSinkSettingsError::InvalidIdentity);
        }
        if global_tags.len() > MAX_GLOBAL_TAGS
            || global_tags.iter().any(|tag| {
                tag.is_empty() || tag.len() > MAX_TAG_BYTES || tag.chars().any(char::is_control)
            })
        {
            return Err(DatadogMetricSinkSettingsError::InvalidTag);
        }
        Ok(Self {
            api_key,
            endpoint,
            cluster_name,
            hostname,
            global_tags,
        })
    }
}

/// Datadog metric sink configuration was unsafe or ambiguous.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DatadogMetricSinkSettingsError {
    /// API key was empty or unreasonably large.
    #[error("Datadog API key must be 1-1024 bytes")]
    InvalidApiKey,
    /// Site was empty, oversized, or contained URL syntax.
    #[error("Datadog site must be a 1-255 character DNS name")]
    InvalidSite,
    /// Test or internal endpoint was not absolute HTTP(S).
    #[error("Datadog metrics endpoint must be an absolute HTTP(S) URL")]
    InvalidEndpoint,
    /// Cluster or host identity was empty.
    #[error("Datadog metrics cluster and host identity must be non-empty")]
    InvalidIdentity,
    /// A global tag was empty, oversized, unsafe, or the tag list was unbounded.
    #[error("Datadog metrics tags must contain at most 256 bounded non-control values")]
    InvalidTag,
}

/// Datadog workload metric sink with deterministic cumulative-counter deltas.
pub struct DatadogMetricSink {
    id: MetricSinkId,
    settings: DatadogMetricSinkSettings,
    transport: Arc<dyn MetricHttpTransport>,
}

impl DatadogMetricSink {
    /// Constructs a sink over an injected HTTP boundary.
    pub fn new(
        settings: DatadogMetricSinkSettings,
        transport: Arc<dyn MetricHttpTransport>,
    ) -> Self {
        Self {
            id: MetricSinkId::built_in("datadog"),
            settings,
            transport,
        }
    }

    fn build_series(
        &self,
        points: &[SequencedMetricPoint],
    ) -> Result<Vec<DatadogSeries>, MetricSinkError> {
        let mut series = Vec::with_capacity(points.len().saturating_mul(4));
        for sequenced in points {
            validate_baseline(sequenced)?;
            self.append_point_series(sequenced, &mut series);
        }
        Ok(series)
    }

    fn append_point_series(&self, sequenced: &SequencedMetricPoint, out: &mut Vec<DatadogSeries>) {
        let point = &sequenced.point;
        let timestamp = point.id.collected_at.0.div_euclid(1_000);
        let tags = self.tags(point);
        if let Some((previous, elapsed)) = sequenced.previous.as_ref().and_then(|previous| {
            elapsed_seconds(previous, point).map(|elapsed| (previous, elapsed))
        }) {
            let cpu_delta = point.cpu_usage_usec.saturating_sub(previous.cpu_usage_usec) as f64;
            out.push(gauge(
                "maestro.service.cpu.percent",
                timestamp,
                cpu_delta / 1_000_000.0 / elapsed * 100.0,
                tags.clone(),
                Some("percent"),
            ));
        }
        out.push(gauge(
            "maestro.service.memory.bytes",
            timestamp,
            point.memory_current_bytes as f64,
            tags.clone(),
            Some("byte"),
        ));
        let Some((previous, elapsed)) = sequenced.previous.as_ref().and_then(|previous| {
            elapsed_seconds(previous, point).map(|elapsed| (previous, elapsed))
        }) else {
            return;
        };
        let (Some(receive), Some(previous_receive), Some(transmit), Some(previous_transmit)) = (
            point.network_receive_bytes,
            previous.network_receive_bytes,
            point.network_transmit_bytes,
            previous.network_transmit_bytes,
        ) else {
            return;
        };
        out.push(gauge(
            "maestro.service.network.rx.bytes_per_sec",
            timestamp,
            receive.saturating_sub(previous_receive) as f64 / elapsed,
            tags.clone(),
            Some("byte"),
        ));
        out.push(gauge(
            "maestro.service.network.tx.bytes_per_sec",
            timestamp,
            transmit.saturating_sub(previous_transmit) as f64 / elapsed,
            tags,
            Some("byte"),
        ));
    }

    fn tags(&self, point: &WorkloadMetricPoint) -> Vec<String> {
        let mut tags = self.host_tags();
        tags.push(format!("service:{}", point.metadata.service_id));
        tags.push(format!("deployment:{}", point.metadata.deployment_id));
        if let Some(replica) = point.metadata.labels.get(REPLICA_INDEX_LABEL) {
            tags.push(format!("replica:{replica}"));
        }
        tags
    }

    fn host_tags(&self) -> Vec<String> {
        let mut tags = Vec::with_capacity(self.settings.global_tags.len().saturating_add(2));
        tags.push(format!("cluster:{}", self.settings.cluster_name));
        tags.push(format!("host:{}", self.settings.hostname));
        tags.extend(self.settings.global_tags.iter().cloned());
        tags
    }

    fn build_host_series(
        &self,
        points: &[SequencedHostMetricPoint],
    ) -> Result<Vec<DatadogSeries>, MetricSinkError> {
        let mut series = Vec::with_capacity(points.len().saturating_mul(7));
        for sequenced in points {
            validate_host_baseline(sequenced)?;
            self.append_host_point_series(sequenced, &mut series);
        }
        Ok(series)
    }

    fn append_host_point_series(
        &self,
        sequenced: &SequencedHostMetricPoint,
        out: &mut Vec<DatadogSeries>,
    ) {
        let point = &sequenced.point;
        let timestamp = point.id.collected_at.0.div_euclid(1_000);
        let tags = self.host_tags();
        if let Some(resources) = point.resources {
            let baseline = sequenced
                .previous_resources
                .as_ref()
                .and_then(|previous| {
                    host_elapsed_seconds(previous, point).map(|elapsed| (previous, elapsed))
                })
                .and_then(|(previous, elapsed)| {
                    previous.resources.map(|resources| (resources, elapsed))
                });
            if let Some((previous, _elapsed)) = baseline {
                out.push(gauge(
                    "maestro.node.cpu.percent",
                    timestamp,
                    host_cpu_percent(previous, resources),
                    tags.clone(),
                    Some("percent"),
                ));
            }
            out.push(gauge(
                "maestro.node.memory.bytes",
                timestamp,
                resources.memory_used_bytes as f64,
                tags.clone(),
                Some("byte"),
            ));
            if let Some((previous, elapsed)) = baseline {
                out.push(gauge(
                    "maestro.node.network.rx.bytes_per_sec",
                    timestamp,
                    resources
                        .network_receive_bytes
                        .saturating_sub(previous.network_receive_bytes) as f64
                        / elapsed,
                    tags.clone(),
                    Some("byte"),
                ));
                out.push(gauge(
                    "maestro.node.network.tx.bytes_per_sec",
                    timestamp,
                    resources
                        .network_transmit_bytes
                        .saturating_sub(previous.network_transmit_bytes) as f64
                        / elapsed,
                    tags.clone(),
                    Some("byte"),
                ));
            }
        }
        for disk in point.disks.iter().flatten() {
            let mut disk_tags = tags.clone();
            disk_tags.push(format!("mount:{}", disk.mount_point));
            let used = disk.total_bytes.saturating_sub(disk.available_bytes);
            out.push(gauge(
                "maestro.node.disk.used.bytes",
                timestamp,
                used as f64,
                disk_tags.clone(),
                Some("byte"),
            ));
            out.push(gauge(
                "maestro.node.disk.total.bytes",
                timestamp,
                disk.total_bytes as f64,
                disk_tags.clone(),
                Some("byte"),
            ));
            out.push(gauge(
                "maestro.node.disk.usage.percent",
                timestamp,
                used as f64 / disk.total_bytes as f64 * 100.0,
                disk_tags,
                Some("percent"),
            ));
        }
    }

    async fn send_series(&self, series: Vec<DatadogSeries>) -> Result<(), MetricSinkError> {
        if series.is_empty() {
            return Ok(());
        }
        let body = serde_json::to_vec(&DatadogPayload { series }).map_err(|error| {
            MetricSinkError::Rejected {
                message: format!("Datadog metric payload could not be encoded: {error}"),
            }
        })?;
        let response = self
            .transport
            .send(MetricHttpRequest {
                url: self.settings.endpoint.clone(),
                headers: BTreeMap::from([
                    ("Content-Type".to_owned(), "application/json".to_owned()),
                    ("DD-API-KEY".to_owned(), self.settings.api_key.clone()),
                ]),
                body,
            })
            .await
            .map_err(|error| MetricSinkError::Unavailable {
                message: error.to_string(),
            })?;
        if (200..300).contains(&response.status) {
            Ok(())
        } else if response.status == 408
            || response.status == 429
            || (500..600).contains(&response.status)
        {
            Err(MetricSinkError::Unavailable {
                message: format!(
                    "Datadog metrics intake returned HTTP {}: {}",
                    response.status, response.body
                ),
            })
        } else {
            Err(MetricSinkError::Rejected {
                message: format!(
                    "Datadog metrics intake returned HTTP {}: {}",
                    response.status, response.body
                ),
            })
        }
    }
}

#[async_trait]
impl MetricSink for DatadogMetricSink {
    fn id(&self) -> &MetricSinkId {
        &self.id
    }

    async fn send(&self, points: &[SequencedMetricPoint]) -> Result<(), MetricSinkError> {
        let series = self.build_series(points)?;
        self.send_series(series).await
    }
}

#[async_trait]
impl HostMetricSink for DatadogMetricSink {
    fn id(&self) -> &MetricSinkId {
        &self.id
    }

    async fn send_host_metrics(
        &self,
        points: &[SequencedHostMetricPoint],
    ) -> Result<(), MetricSinkError> {
        let series = self.build_host_series(points)?;
        self.send_series(series).await
    }
}

#[derive(Serialize)]
struct DatadogPayload {
    series: Vec<DatadogSeries>,
}

#[derive(Serialize)]
struct DatadogSeries {
    metric: &'static str,
    #[serde(rename = "type")]
    metric_type: u8,
    points: Vec<DatadogPoint>,
    tags: Vec<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    unit: Option<&'static str>,
}

#[derive(Serialize)]
struct DatadogPoint {
    timestamp: i64,
    value: f64,
}

fn gauge(
    metric: &'static str,
    timestamp: i64,
    value: f64,
    tags: Vec<String>,
    unit: Option<&'static str>,
) -> DatadogSeries {
    DatadogSeries {
        metric,
        metric_type: DATADOG_GAUGE,
        points: vec![DatadogPoint { timestamp, value }],
        tags,
        unit,
    }
}

fn elapsed_seconds(previous: &WorkloadMetricPoint, current: &WorkloadMetricPoint) -> Option<f64> {
    let elapsed_millis = current
        .id
        .collected_at
        .0
        .checked_sub(previous.id.collected_at.0)
        .and_then(|elapsed| u64::try_from(elapsed).ok())?;
    (elapsed_millis != 0).then(|| Duration::from_millis(elapsed_millis).as_secs_f64())
}

fn host_elapsed_seconds(previous: &HostMetricPoint, current: &HostMetricPoint) -> Option<f64> {
    let elapsed_millis = current
        .id
        .collected_at
        .0
        .checked_sub(previous.id.collected_at.0)
        .and_then(|elapsed| u64::try_from(elapsed).ok())?;
    (elapsed_millis != 0).then(|| Duration::from_millis(elapsed_millis).as_secs_f64())
}

fn host_cpu_percent(
    previous: crate::HostResourceMetricPoint,
    current: crate::HostResourceMetricPoint,
) -> f64 {
    if current.cpu_total_ticks < previous.cpu_total_ticks
        || current.cpu_idle_ticks < previous.cpu_idle_ticks
    {
        return 0.0;
    }
    let total = current.cpu_total_ticks - previous.cpu_total_ticks;
    let idle = current.cpu_idle_ticks - previous.cpu_idle_ticks;
    if total == 0 {
        0.0
    } else {
        total.saturating_sub(idle) as f64 / total as f64 * 100.0
    }
}

fn validate_baseline(point: &SequencedMetricPoint) -> Result<(), MetricSinkError> {
    let Some(previous) = &point.previous else {
        return Ok(());
    };
    if previous.id.node_id != point.point.id.node_id
        || previous.id.workload_id != point.point.id.workload_id
    {
        return Err(MetricSinkError::Rejected {
            message: "metric rate baseline belongs to a different workload".to_owned(),
        });
    }
    Ok(())
}

fn validate_host_baseline(point: &SequencedHostMetricPoint) -> Result<(), MetricSinkError> {
    let Some(previous) = &point.previous_resources else {
        return Ok(());
    };
    if previous.id.cluster_id != point.point.id.cluster_id
        || previous.id.node_id != point.point.id.node_id
        || previous.resources.is_none()
        || point.point.resources.is_none()
        || previous.id.collected_at.0 >= point.point.id.collected_at.0
    {
        return Err(MetricSinkError::Rejected {
            message: "host metric rate baseline is not a prior resource point for this node"
                .to_owned(),
        });
    }
    Ok(())
}

fn valid_datadog_site(site: &str) -> bool {
    !site.is_empty()
        && site.len() <= 255
        && !site.starts_with('.')
        && !site.ends_with('.')
        && !site.contains("..")
        && site
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'.'))
}
