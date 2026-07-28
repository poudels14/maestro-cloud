use std::path::{Path, PathBuf};

use cluster::{
    ClusterCertificateAuthority, ClusterConfig, NodeCertificateBundle, StoreJoinTicket,
    StoreStartMode,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, SecretValue};
use runtime::ContainerdRuntimeSettings;
use serde::{Deserialize, Serialize};

use crate::launch_error::{DaemonLaunchError, invalid};
use crate::{
    DatadogLaunchConfig, DepotLaunchConfig, LogBackupLaunchConfig, NixosUpgradeLaunchConfig,
    PreviewLaunchConfig,
};

/// Store process decision supplied explicitly on every daemon start.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", deny_unknown_fields)]
pub enum StoreLaunchMode {
    /// Create the designated master's first store member.
    Bootstrap,
    /// Start one member previously staged by the active cluster leader.
    Join { ticket: StoreJoinTicket },
    /// Reopen the member already persisted in this node's data directory.
    Restart,
    /// Connect a worker agent without starting a local store member.
    Client,
}

impl StoreLaunchMode {
    pub(super) fn provider_mode(&self) -> Option<StoreStartMode> {
        match self {
            Self::Bootstrap => Some(StoreStartMode::Bootstrap),
            Self::Join { ticket } => Some(StoreStartMode::Join(ticket.clone())),
            Self::Restart => Some(StoreStartMode::Restart),
            Self::Client => None,
        }
    }
}

/// Protected launch document consumed by the control-plane daemon executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DaemonLaunchConfig {
    /// Authoritative topology and fixed cluster settings.
    pub cluster: ClusterConfig,
    /// Stable local node selected from the topology.
    pub node_id: NodeId,
    /// Root of all role and provider persistence.
    pub data_directory: PathBuf,
    /// Containerd gRPC Unix socket used by Linux; ignored by the macOS Docker profile.
    #[serde(default = "default_containerd_socket")]
    pub containerd_socket: PathBuf,
    /// Exact etcd executable on control-plane nodes; absent on workers.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub etcd_binary: Option<PathBuf>,
    /// Explicit local store initialization decision.
    pub store_mode: StoreLaunchMode,
    /// Node-specific mutual TLS identity granted during bootstrap or join.
    pub security: NodeCertificateBundle,
    /// Cluster CA signer retained only by control-plane-capable nodes.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub certificate_issuer: Option<ClusterCertificateAuthority>,
    /// Cluster-wide HS256 key used for operator and internal authentication.
    pub jwt_secret_key: SecretValue,
    /// Cluster-wide master secret used to encrypt internal persisted values.
    pub store_encryption_secret: SecretValue,
    /// Optional deterministic process identity, primarily for cluster tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<NodeInstanceId>,
    /// Optional node-local Datadog log delivery.
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
    /// Optional NixOS staging and reboot policy; absence disables cluster upgrades.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub nixos_upgrade: Option<NixosUpgradeLaunchConfig>,
}

/// Private on-disk daemon configuration.
pub type DaemonLaunchDocument = DaemonLaunchConfig;

impl DaemonLaunchConfig {
    fn validate_common(&self) -> Result<(), DaemonLaunchError> {
        self.cluster.preflight()?;
        if let Some(datadog) = &self.datadog {
            crate::datadog::validate_datadog(datadog)?;
        }
        if let Some(depot) = &self.depot {
            crate::depot_config::validate_depot(depot)
                .map_err(|error| invalid(error.to_string()))?;
        }
        if let Some(log_backup) = &self.log_backup {
            crate::log_backup_config::validate_log_backup(
                log_backup,
                &self.cluster.name,
                &self.node_id,
            )?;
        }
        if let Some(preview) = &self.preview {
            crate::preview_config::validate_preview(preview)?;
        }
        if let Some(upgrade) = &self.nixos_upgrade {
            crate::upgrade_config::validate_nixos_upgrade(upgrade)?;
        }
        if !self.data_directory.is_absolute() {
            return Err(invalid("data directory must be an absolute path"));
        }
        #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
        if !self.containerd_socket.is_absolute() {
            return Err(invalid("containerd socket must be an absolute path"));
        }
        #[cfg(any(target_os = "macos", feature = "macos-platform"))]
        {
            if self.cluster.nodes.len() != 1 {
                return Err(invalid(
                    "the macOS Docker profile supports exactly one cluster node",
                ));
            }
            if self.nixos_upgrade.is_some() {
                return Err(invalid(
                    "NixOS upgrades are unavailable in the macOS Docker profile",
                ));
            }
        }
        let node = self.cluster.nodes.get(&self.node_id).ok_or_else(|| {
            invalid(format!(
                "node `{}` is absent from the cluster topology",
                self.node_id
            ))
        })?;
        if self.store_encryption_secret.expose().chars().count() < 32 {
            return Err(invalid(
                "store encryption secret must contain at least 32 characters",
            ));
        }
        if self.jwt_secret_key.expose().len() < 32 || self.jwt_secret_key.expose().contains('\0') {
            return Err(invalid(
                "JWT secret key must contain at least 32 bytes and no NUL bytes",
            ));
        }
        match (&self.etcd_binary, node.role.is_control_plane()) {
            (Some(path), true) if path.is_absolute() => {}
            (Some(_), true) => {
                return Err(invalid("embedded etcd binary must be an absolute path"));
            }
            (None, true) => {
                return Err(invalid(
                    "control-plane nodes require an embedded etcd binary",
                ));
            }
            (None, false) => {}
            (Some(_), false) => {
                return Err(invalid("worker nodes must not configure an etcd binary"));
            }
        }
        match &self.store_mode {
            StoreLaunchMode::Client if node.role.is_control_plane() => Err(invalid(
                "control-plane nodes must start their declared local store member",
            )),
            mode if !node.role.is_control_plane() && !matches!(mode, StoreLaunchMode::Client) => {
                Err(invalid("worker nodes must use client-only store access"))
            }
            StoreLaunchMode::Bootstrap if node.role != NodeRole::Master => Err(invalid(
                "only the designated master may bootstrap the cluster store",
            )),
            StoreLaunchMode::Join { ticket }
                if node.role == NodeRole::Master || ticket.node_id() != &self.node_id =>
            {
                Err(invalid(
                    "join mode requires a non-master ticket bound to the local node",
                ))
            }
            _ => Ok(()),
        }?;
        match (&self.certificate_issuer, node.role.is_control_plane()) {
            (Some(issuer), true) if issuer.certificate_pem == self.security.trust_root_pem => {
                Ok(())
            }
            (Some(_), true) => Err(invalid(
                "certificate issuer must match the node identity trust root",
            )),
            (None, true) => Err(invalid(
                "control-plane nodes require the cluster certificate issuer",
            )),
            (None, false) => Ok(()),
            (Some(_), false) => Err(invalid(
                "worker nodes must not retain cluster certificate signing material",
            )),
        }
    }
}

impl DaemonLaunchConfig {
    /// Validates all resolved launch choices before local state or processes are touched.
    pub fn validate(&self) -> Result<(), DaemonLaunchError> {
        self.validate_common()?;
        let node = self
            .cluster
            .nodes
            .get(&self.node_id)
            .ok_or_else(|| invalid("local node disappeared from validated topology"))?;
        super::api_settings(
            &self.cluster,
            node,
            &self.security,
            self.jwt_secret_key.clone(),
        )
        .validate()?;
        if let Some(settings) =
            super::admin_api_settings(&self.cluster, node, self.jwt_secret_key.clone())?
        {
            settings.validate()?;
        }
        Ok(())
    }
}

/// Reads a private launch document after enforcing owner-only permissions.
pub fn load_launch_document(path: &Path) -> Result<DaemonLaunchDocument, DaemonLaunchError> {
    validate_private_permissions(path)?;
    let bytes = std::fs::read(path).map_err(|source| DaemonLaunchError::Io {
        action: "read",
        path: path.to_path_buf(),
        source,
    })?;
    let config = serde_json::from_slice::<DaemonLaunchDocument>(&bytes).map_err(|source| {
        DaemonLaunchError::InvalidDocument {
            path: path.to_path_buf(),
            source,
        }
    })?;
    config.validate()?;
    Ok(config)
}

/// Loads one owner-only daemon launch document.
pub async fn load_launch_config(path: &Path) -> Result<DaemonLaunchConfig, DaemonLaunchError> {
    load_launch_document(path)
}

fn default_containerd_socket() -> PathBuf {
    ContainerdRuntimeSettings::default().socket
}

fn validate_private_permissions(path: &Path) -> Result<(), DaemonLaunchError> {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|source| DaemonLaunchError::Io {
                action: "inspect permissions of",
                path: path.to_path_buf(),
                source,
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(DaemonLaunchError::InsecurePermissions {
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}
