use std::sync::Arc;
use std::time::Duration;

use crate::DaemonLaunchError;
use cluster::DatadogLaunchConfig;
use logs::{
    DatadogLogSink, DatadogLogSinkSettings, DatadogLogSinkSettingsError, HttpTransport,
    LogFilterKind, LogSink, LogSourceInclusion, LogStoreRuntime, ReqwestHttpTransport,
};
use metrics::{
    DatadogMetricSink, DatadogMetricSinkSettings, HostMetricSink, MetricHttpTransport, MetricSink,
    ReqwestMetricHttpTransport,
};

pub(crate) fn validate_datadog(config: &DatadogLaunchConfig) -> Result<(), DaemonLaunchError> {
    log_sink_settings(config)?;
    if config.metrics.enabled {
        metric_sink_settings(config, "validation-cluster", "validation-host")?;
    }
    Ok(())
}

fn log_sink_settings(
    config: &DatadogLaunchConfig,
) -> Result<DatadogLogSinkSettings, DatadogLogSinkSettingsError> {
    let filters = if config.logs.include_healthcheck {
        Vec::new()
    } else {
        vec![LogFilterKind::SuccessfulHealthcheck]
    };
    DatadogLogSinkSettings::new(config.api_key.expose(), &config.site).map(|settings| {
        settings
            .ingress_logs(if config.include_ingress_logs {
                LogSourceInclusion::Include
            } else {
                LogSourceInclusion::Exclude
            })
            .tailscale_logs(if config.include_tailscale_logs {
                LogSourceInclusion::Include
            } else {
                LogSourceInclusion::Exclude
            })
            .filters(filters)
    })
}

fn metric_sink_settings(
    config: &DatadogLaunchConfig,
    cluster_name: &str,
    hostname: &str,
) -> Result<DatadogMetricSinkSettings, metrics::DatadogMetricSinkSettingsError> {
    DatadogMetricSinkSettings::new(
        config.api_key.expose(),
        &config.site,
        cluster_name,
        hostname,
        config.metrics.tags.clone(),
    )
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
                log_settings: log_sink_settings(config)?,
                log_transport: Arc::new(ReqwestHttpTransport::new(Duration::from_secs(30))?),
                metrics: config
                    .metrics
                    .enabled
                    .then(|| {
                        Ok::<_, DaemonLaunchError>(ConfiguredDatadogMetrics {
                            settings: metric_sink_settings(config, cluster_name, hostname)?,
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
