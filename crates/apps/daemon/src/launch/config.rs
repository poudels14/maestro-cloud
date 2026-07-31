use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use cluster::{
    ClusterCertificateAuthority, ClusterConfig, NodeCertificateBundle, StoreJoinTicket,
    StoreStartMode,
};
use kernel_api::{NodeId, NodeInstanceId, NodeRole, SecretValue};
use kernel_store::{EncryptedValue, derive_key_with_context, open_with_context, seal_with_context};
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

/// Runtime configuration resolved from the live cluster source and node bootstrap document.
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

/// Minimal node-local bootstrap document consumed by the daemon executable.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct DaemonLaunchDocument {
    /// Stable local node selected from the live cluster topology.
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
    /// Node identity and optional first-bootstrap CA seed encrypted by the config key.
    pub protected_bootstrap: String,
    /// Optional deterministic process identity, primarily for cluster tests.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub instance_id: Option<NodeInstanceId>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ProtectedBootstrap {
    security: NodeCertificateBundle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    certificate_issuer: Option<ClusterCertificateAuthority>,
}

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
    let document = serde_json::from_slice::<DaemonLaunchDocument>(&bytes).map_err(|source| {
        DaemonLaunchError::InvalidDocument {
            path: path.to_path_buf(),
            source,
        }
    })?;
    document.validate()?;
    Ok(document)
}

/// Loads current cluster settings and combines them with node-local bootstrap state.
pub async fn load_launch_config(
    path: &Path,
    config_source: &str,
) -> Result<DaemonLaunchConfig, DaemonLaunchError> {
    load_launch_config_with_fallbacks(
        path,
        config_source,
        &maestro_cli::ClusterConfigFallbacks::default(),
    )
    .await
}

/// Loads current cluster settings with CLI fallbacks and combines them with node bootstrap state.
pub async fn load_launch_config_with_fallbacks(
    path: &Path,
    config_source: &str,
    fallbacks: &maestro_cli::ClusterConfigFallbacks,
) -> Result<DaemonLaunchConfig, DaemonLaunchError> {
    let document = load_launch_document(path)?;
    let loaded = maestro_cli::load_cluster_for_node_with_fallbacks(
        config_source,
        document.node_id.clone(),
        &maestro_cli::SystemConfigSourceReader,
        fallbacks,
    )
    .await
    .map_err(|error| invalid(format!("failed to load current cluster config: {error}")))?;
    let protected = document.open(&loaded.encryption_key)?;
    let cluster::ClusterLaunchPolicy {
        datadog,
        depot,
        log_backup,
        preview,
        nixos_upgrade,
    } = loaded.launch_policy;
    let config = DaemonLaunchConfig {
        cluster: loaded.cluster,
        node_id: document.node_id,
        data_directory: document.data_directory,
        containerd_socket: document.containerd_socket,
        etcd_binary: document.etcd_binary,
        store_mode: document.store_mode,
        security: protected.security,
        certificate_issuer: protected.certificate_issuer,
        jwt_secret_key: loaded.jwt_secret_key,
        store_encryption_secret: loaded.encryption_key,
        instance_id: document.instance_id,
        datadog,
        depot,
        log_backup,
        preview,
        nixos_upgrade,
    };
    config.validate()?;
    Ok(config)
}

impl DaemonLaunchDocument {
    /// Creates a minimal launch document with authenticated encrypted bootstrap material.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        node_id: NodeId,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: Option<PathBuf>,
        store_mode: StoreLaunchMode,
        security: NodeCertificateBundle,
        certificate_issuer: Option<ClusterCertificateAuthority>,
        encryption_secret: &SecretValue,
        instance_id: Option<NodeInstanceId>,
    ) -> Result<Self, DaemonLaunchError> {
        let plaintext = serde_json::to_vec(&ProtectedBootstrap {
            security,
            certificate_issuer,
        })
        .map_err(|error| {
            invalid(format!(
                "failed to encode protected bootstrap state: {error}"
            ))
        })?;
        let key = derive_bootstrap_key(encryption_secret)?;
        let envelope = seal_with_context(&key, &plaintext, bootstrap_context(&node_id).as_bytes())
            .map_err(|error| {
                invalid(format!(
                    "failed to encrypt protected bootstrap state: {error}"
                ))
            })?;
        let document = Self {
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary,
            store_mode,
            protected_bootstrap: BASE64.encode(envelope.as_bytes()),
            instance_id,
        };
        document.validate()?;
        Ok(document)
    }

    fn validate(&self) -> Result<(), DaemonLaunchError> {
        if !self.data_directory.is_absolute() {
            return Err(invalid("data directory must be an absolute path"));
        }
        #[cfg(all(target_os = "linux", not(feature = "macos-platform")))]
        if !self.containerd_socket.is_absolute() {
            return Err(invalid("containerd socket must be an absolute path"));
        }
        if self.protected_bootstrap.trim().is_empty() {
            return Err(invalid("protected bootstrap state must not be empty"));
        }
        Ok(())
    }

    fn open(
        &self,
        encryption_secret: &SecretValue,
    ) -> Result<ProtectedBootstrap, DaemonLaunchError> {
        let envelope = BASE64
            .decode(&self.protected_bootstrap)
            .map_err(|_| invalid("protected bootstrap state is not valid Base64"))?;
        let envelope = EncryptedValue::from_bytes(envelope)
            .map_err(|error| invalid(format!("protected bootstrap state is invalid: {error}")))?;
        let key = derive_bootstrap_key(encryption_secret)?;
        let plaintext =
            open_with_context(&key, &envelope, bootstrap_context(&self.node_id).as_bytes())
                .map_err(|error| {
                    invalid(format!(
                        "could not decrypt protected bootstrap state: {error}"
                    ))
                })?;
        serde_json::from_slice(&plaintext)
            .map_err(|error| invalid(format!("protected bootstrap state is invalid: {error}")))
    }
}

fn bootstrap_context(node_id: &NodeId) -> String {
    format!("maestro-node-bootstrap-v1:{node_id}")
}

fn derive_bootstrap_key(
    encryption_secret: &SecretValue,
) -> Result<kernel_store::EncryptionKey, DaemonLaunchError> {
    derive_key_with_context(encryption_secret.expose(), b"node-bootstrap-v1")
        .map_err(|error| invalid(format!("config encryption key is invalid: {error}")))
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
