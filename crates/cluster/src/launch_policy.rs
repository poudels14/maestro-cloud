use std::path::PathBuf;

use kernel_api::SecretValue;
use serde::{Deserialize, Serialize};

/// Optional production integrations copied into every protected node launch document.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ClusterLaunchPolicy {
    /// Optional node-local Datadog log and metric delivery.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub datadog: Option<DatadogLaunchConfig>,
    /// Optional cluster-wide Depot remote-builder credentials.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub depot: Option<DepotLaunchConfig>,
    /// Optional node-local S3 log backup and retention target.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub log_backup: Option<LogBackupLaunchConfig>,
    /// Optional cluster-wide GitHub pull-request previews.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub preview: Option<PreviewLaunchConfig>,
    /// Optional NixOS staging override; absence selects `/etc/maestro#default`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nixos_upgrade: Option<NixosUpgradeLaunchConfig>,
}

/// Node-local Datadog log and metric delivery configuration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DatadogLaunchConfig {
    /// Secret API credential exposed only while constructing sink adapters.
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

/// Datadog log-specific filtering configuration.
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

/// Cluster-wide Depot remote-builder credentials and process limits.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DepotLaunchConfig {
    /// Depot project or organization token passed only through `DEPOT_TOKEN`.
    pub token: SecretValue,
    /// Depot CLI executable or command name.
    #[serde(default = "default_depot_executable")]
    pub executable: PathBuf,
    /// Maximum wall-clock duration of one Depot build.
    #[serde(default = "default_depot_timeout_secs")]
    pub timeout_secs: u64,
    /// Uses Depot Registry for immutable build output instead of downloading archives.
    #[serde(default, skip_serializing_if = "is_false")]
    pub registry: bool,
}

/// Node-local S3 destination for committed cold log partitions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct LogBackupLaunchConfig {
    /// S3 bucket receiving node-qualified Parquet objects and manifests.
    pub bucket: String,
    /// Required AWS KMS key ARN or S3-compatible KMS key identity.
    pub kms_key_id: String,
    /// Optional AWS region override; the standard provider chain is used otherwise.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub region: Option<String>,
    /// Optional object-key prefix; the cluster name is used when absent or empty.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub prefix: Option<String>,
    /// Optional number of UTC days retained locally after verified remote backup.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub retention_days: Option<u64>,
}

/// Cluster-wide pull-request preview integration.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct PreviewLaunchConfig {
    /// DNS suffix used for stable preview hostnames.
    pub domain: String,
    /// GitHub token with pull-request read and deployment read/write access.
    pub github_token: SecretValue,
    /// Maximum previews retained cluster-wide, including close grace periods.
    pub max_concurrent_previews: usize,
}

/// Overrides the standard `/etc/maestro#default` NixOS host-upgrade policy.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct NixosUpgradeLaunchConfig {
    /// Absolute flake directory whose lock file tracks the desired system.
    pub flake: PathBuf,
    /// NixOS configuration selected from the flake.
    #[serde(default = "default_configuration")]
    pub configuration: String,
    /// Optional hermetic path to the Nix executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nix_binary: Option<PathBuf>,
    /// Optional hermetic path to the NixOS rebuild executable.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nixos_rebuild_binary: Option<PathBuf>,
    /// Optional hermetic path to systemctl.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub systemctl_binary: Option<PathBuf>,
}

impl NixosUpgradeLaunchConfig {
    /// Selects a flake with the standard configuration and process paths.
    pub fn new(flake: impl Into<PathBuf>) -> Self {
        Self {
            flake: flake.into(),
            configuration: default_configuration(),
            nix_binary: None,
            nixos_rebuild_binary: None,
            systemctl_binary: None,
        }
    }
}

const fn default_true() -> bool {
    true
}

fn is_false(value: &bool) -> bool {
    !*value
}

fn default_depot_executable() -> PathBuf {
    PathBuf::from("depot")
}

const fn default_depot_timeout_secs() -> u64 {
    30 * 60
}

fn default_configuration() -> String {
    "default".to_owned()
}
