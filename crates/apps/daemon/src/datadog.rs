use std::sync::Arc;
use std::time::Duration;

use kernel_api::SecretValue;
use logs::{
    DatadogLogSink, DatadogLogSinkSettings, DatadogLogSinkSettingsError, HttpTransport,
    LogFilterKind, LogSink, LogStoreRuntime, ReqwestHttpTransport,
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
}

impl DatadogLaunchConfig {
    pub(crate) fn validate(&self) -> Result<(), DatadogLogSinkSettingsError> {
        self.sink_settings().map(|_settings| ())
    }

    fn sink_settings(&self) -> Result<DatadogLogSinkSettings, DatadogLogSinkSettingsError> {
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

pub(crate) struct ConfiguredDatadog {
    settings: DatadogLogSinkSettings,
    transport: Arc<dyn HttpTransport>,
}

pub(crate) fn configure_datadog(
    config: Option<&DatadogLaunchConfig>,
) -> Result<Option<ConfiguredDatadog>, DaemonLaunchError> {
    config
        .map(|config| {
            Ok(ConfiguredDatadog {
                settings: config.sink_settings()?,
                transport: Arc::new(ReqwestHttpTransport::new(Duration::from_secs(30))?),
            })
        })
        .transpose()
}

pub(crate) fn build_log_sinks(
    datadog: Option<ConfiguredDatadog>,
    log_store_runtime: &dyn LogStoreRuntime,
) -> Vec<Arc<dyn LogSink>> {
    let Some(configured) = datadog else {
        return Vec::new();
    };
    vec![Arc::new(DatadogLogSink::new(
        configured.settings,
        configured.transport,
        log_store_runtime.dead_letter_store(),
    ))]
}

fn default_true() -> bool {
    true
}
