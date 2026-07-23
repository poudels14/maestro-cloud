use std::sync::Arc;
use std::time::Duration;

use kernel_api::SecretValue;
use logs::{
    DatadogLogSink, DatadogLogSinkSettings, DatadogLogSinkSettingsError, HttpTransport,
    LogFilterKind, LogSink, LogStoreRuntime, ReqwestHttpTransport,
};
use metrics::{
    DatadogMetricSink, DatadogMetricSinkSettings, HostMetricSink, MetricHttpTransport, MetricSink,
    ReqwestMetricHttpTransport,
};
use serde::{Deserialize, Serialize};

use crate::DaemonLaunchError;

/// Node-local Datadog log-delivery view extracted from the authoritative config.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogLaunchConfig {
    /// Secret API credential exposed only while constructing the sink adapter.
    pub api_key: SecretValue,
    /// Datadog intake site such as `datadoghq.com` or `datadoghq.eu`.
    pub site: String,
    /// Whether ingress service and access-log records are delivered.
    #[serde(default = "default_true")]
    pub include_ingress_logs: bool,
    /// Whether useful Tailscale records are delivered after noise filtering.
    #[serde(default = "default_true")]
    pub include_tailscale_logs: bool,
    /// Sink-local log filtering choices.
    #[serde(default)]
    pub logs: DatadogLogsLaunchConfig,
    /// Opt-in Datadog workload metric delivery and its global tags.
    #[serde(default)]
    pub metrics: DatadogMetricsLaunchConfig,
}

impl DatadogLaunchConfig {
    pub(crate) fn validate(&self) -> Result<(), DaemonLaunchError> {
        self.log_sink_settings()?;
        if self.metrics.enabled {
            self.metric_sink_settings("validation-cluster", "validation-host")?;
        }
        Ok(())
    }

    fn log_sink_settings(&self) -> Result<DatadogLogSinkSettings, DatadogLogSinkSettingsError> {
        let filters = if self.logs.include_healthcheck {
            Vec::new()
        } else {
            vec![LogFilterKind::SuccessfulHealthcheck]
        };
        DatadogLogSinkSettings::new(self.api_key.expose(), &self.site).map(|settings| {
            settings
                .include_ingress_logs(self.include_ingress_logs)
                .include_tailscale_logs(self.include_tailscale_logs)
                .filters(filters)
        })
    }

    fn metric_sink_settings(
        &self,
        cluster_name: &str,
        hostname: &str,
    ) -> Result<DatadogMetricSinkSettings, metrics::DatadogMetricSinkSettingsError> {
        DatadogMetricSinkSettings::new(
            self.api_key.expose(),
            &self.site,
            cluster_name,
            hostname,
            self.metrics.tags.clone(),
        )
    }
}

/// Datadog log-specific filtering view.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogLogsLaunchConfig {
    /// Whether successful configured workload healthchecks are retained.
    #[serde(default = "default_true")]
    pub include_healthcheck: bool,
}

impl Default for DatadogLogsLaunchConfig {
    fn default() -> Self {
        Self {
            include_healthcheck: true,
        }
    }
}

/// Datadog metric-specific enablement and global series tags.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogMetricsLaunchConfig {
    /// Enables durable node-local delivery of normalized workload metrics.
    #[serde(default)]
    pub enabled: bool,
    /// Additional tags appended after cluster and host identity.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub tags: Vec<String>,
}

pub(crate) struct ConfiguredDatadog {
    log_settings: DatadogLogSinkSettings,
    log_transport: Arc<dyn HttpTransport>,
    metrics: Option<ConfiguredDatadogMetrics>,
}

struct ConfiguredDatadogMetrics {
    settings: DatadogMetricSinkSettings,
    metric_transport: Arc<dyn MetricHttpTransport>,
}

pub(crate) struct DatadogSinks {
    pub(crate) logs: Vec<Arc<dyn LogSink>>,
    pub(crate) metrics: Vec<Arc<dyn MetricSink>>,
    pub(crate) host_metrics: Vec<Arc<dyn HostMetricSink>>,
}

pub(crate) fn configure_datadog(
    config: Option<&DatadogLaunchConfig>,
    cluster_name: &str,
    hostname: &str,
) -> Result<Option<ConfiguredDatadog>, DaemonLaunchError> {
    config
        .map(|config| {
            Ok(ConfiguredDatadog {
                log_settings: config.log_sink_settings()?,
                log_transport: Arc::new(ReqwestHttpTransport::new(Duration::from_secs(30))?),
                metrics: config
                    .metrics
                    .enabled
                    .then(|| {
                        Ok::<_, DaemonLaunchError>(ConfiguredDatadogMetrics {
                            settings: config.metric_sink_settings(cluster_name, hostname)?,
                            metric_transport: Arc::new(ReqwestMetricHttpTransport::new(
                                Duration::from_secs(15),
                            )?),
                        })
                    })
                    .transpose()?,
            })
        })
        .transpose()
}

pub(crate) fn build_datadog_sinks(
    datadog: Option<ConfiguredDatadog>,
    log_store_runtime: &dyn LogStoreRuntime,
) -> DatadogSinks {
    let Some(configured) = datadog else {
        return DatadogSinks {
            logs: Vec::new(),
            metrics: Vec::new(),
            host_metrics: Vec::new(),
        };
    };
    let log_sinks: Vec<Arc<dyn LogSink>> = vec![Arc::new(DatadogLogSink::new(
        configured.log_settings,
        configured.log_transport,
        log_store_runtime.dead_letter_store(),
    ))];
    let (metric_sinks, host_metric_sinks) = configured.metrics.map_or_else(
        || (Vec::new(), Vec::new()),
        |metrics| {
            let sink = Arc::new(DatadogMetricSink::new(
                metrics.settings,
                metrics.metric_transport,
            ));
            (
                vec![sink.clone() as Arc<dyn MetricSink>],
                vec![sink as Arc<dyn HostMetricSink>],
            )
        },
    );
    DatadogSinks {
        logs: log_sinks,
        metrics: metric_sinks,
        host_metrics: host_metric_sinks,
    }
}

fn default_true() -> bool {
    true
}
