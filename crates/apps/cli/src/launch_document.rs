use std::path::{Path, PathBuf};

use cluster::{
    ClusterCertificateAuthority, ClusterConfig, ClusterLaunchPolicy, DatadogLaunchConfig,
    DepotLaunchConfig, JoinPayload, LogBackupLaunchConfig, NixosUpgradeLaunchConfig,
    NodeCertificateBundle, PreviewLaunchConfig, StoreJoinTicket, certificate_fingerprint,
};
use kernel_api::{NodeId, NodeRole, SecretValue};
use serde::{Deserialize, Serialize};

use crate::CliError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DaemonLaunchDocument {
    cluster: ClusterConfig,
    node_id: NodeId,
    data_directory: PathBuf,
    containerd_socket: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etcd_binary: Option<PathBuf>,
    store_mode: StoreLaunchDocument,
    security: NodeCertificateBundle,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    certificate_issuer: Option<ClusterCertificateAuthority>,
    operator_jwt_secret: SecretValue,
    store_encryption_secret: SecretValue,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    datadog: Option<DatadogLaunchConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    depot: Option<DepotLaunchConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    log_backup: Option<LogBackupLaunchConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    preview: Option<PreviewLaunchConfig>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    nixos_upgrade: Option<NixosUpgradeLaunchConfig>,
}

impl DaemonLaunchDocument {
    pub(crate) fn bootstrap(
        cluster: ClusterConfig,
        node_id: NodeId,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: PathBuf,
        security: NodeCertificateBundle,
        certificate_issuer: ClusterCertificateAuthority,
        operator_jwt_secret: SecretValue,
        store_encryption_secret: SecretValue,
        launch_policy: ClusterLaunchPolicy,
    ) -> Result<Self, CliError> {
        let ClusterLaunchPolicy {
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        } = launch_policy;
        let document = Self {
            cluster,
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary: Some(etcd_binary),
            store_mode: StoreLaunchDocument::Bootstrap,
            security,
            certificate_issuer: Some(certificate_issuer),
            operator_jwt_secret,
            store_encryption_secret,
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        };
        document.validate()?;
        Ok(document)
    }

    pub(crate) fn joined(
        node_id: NodeId,
        join_secret: SecretValue,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: Option<PathBuf>,
        payload: JoinPayload,
    ) -> Result<Self, CliError> {
        let ClusterLaunchPolicy {
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        } = payload.launch_policy;
        let cluster = ClusterConfig {
            cluster_id: payload.cluster_id,
            name: payload.cluster_name,
            cluster_cidr: payload.cluster_cidr,
            node_limit: payload.node_limit,
            node_prefix: payload.node_prefix,
            nodes: payload.nodes,
            control_allow_cidrs: payload.control_allow_cidrs,
            ports: payload.ports,
            join_secret,
            tailscale: payload.tailscale,
            cloudflare: payload.cloudflare,
        };
        let role = cluster
            .nodes
            .get(&node_id)
            .ok_or_else(|| CliError::invalid_api_response("join grant omitted the local node"))?
            .role;
        let store_mode = StoreLaunchDocument::joined(&node_id, role, payload.store_join_ticket)?;
        let document = Self {
            cluster,
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary,
            store_mode,
            security: payload.certificates,
            certificate_issuer: payload.certificate_issuer,
            operator_jwt_secret: payload.operator_jwt_secret,
            store_encryption_secret: payload.store_encryption_secret,
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        };
        document.validate()?;
        Ok(document)
    }

    pub(crate) fn cutover(
        cluster: ClusterConfig,
        node_id: NodeId,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: PathBuf,
        security: NodeCertificateBundle,
        certificate_issuer: ClusterCertificateAuthority,
        operator_jwt_secret: SecretValue,
        store_encryption_secret: SecretValue,
        launch_policy: ClusterLaunchPolicy,
    ) -> Result<Self, CliError> {
        let role = cluster
            .nodes
            .get(&node_id)
            .ok_or_else(|| {
                CliError::invalid_input(format!(
                    "cutover node `{node_id}` is absent from the cluster topology"
                ))
            })?
            .role;
        let ClusterLaunchPolicy {
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        } = launch_policy;
        let document = Self {
            cluster,
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary: role.is_control_plane().then_some(etcd_binary),
            store_mode: StoreLaunchDocument::cutover(role),
            security,
            certificate_issuer: role.is_control_plane().then_some(certificate_issuer),
            operator_jwt_secret,
            store_encryption_secret,
            datadog,
            depot,
            log_backup,
            preview,
            nixos_upgrade,
        };
        document.validate()?;
        Ok(document)
    }

    pub(crate) fn validate(&self) -> Result<(), CliError> {
        self.cluster
            .preflight()
            .map_err(|error| cluster_error("launch topology failed preflight", error))?;
        if !self.data_directory.is_absolute() || !self.containerd_socket.is_absolute() {
            return Err(CliError::invalid_input(
                "launch data directory and containerd socket must be absolute paths",
            ));
        }
        let node = self.cluster.nodes.get(&self.node_id).ok_or_else(|| {
            CliError::invalid_input(format!(
                "launch node `{}` is absent from the cluster topology",
                self.node_id
            ))
        })?;
        validate_etcd_binary(node.role, self.etcd_binary.as_deref())?;
        self.store_mode.validate(&self.node_id, node.role)?;
        if self.operator_jwt_secret.expose().len() < 32 {
            return Err(CliError::invalid_input(
                "operator JWT secret must contain at least 32 bytes",
            ));
        }
        if self.store_encryption_secret.expose().chars().count() < 32 {
            return Err(CliError::invalid_input(
                "store encryption secret must contain at least 32 characters",
            ));
        }
        certificate_fingerprint(&self.security.trust_root_pem)
            .map_err(|error| cluster_error("launch trust root is invalid", error))?;
        certificate_fingerprint(&self.security.identity.certificate_pem)
            .map_err(|error| cluster_error("launch node certificate is invalid", error))?;
        if self
            .security
            .identity
            .private_key_pem
            .expose()
            .trim()
            .is_empty()
        {
            return Err(CliError::invalid_input(
                "launch node certificate private key must not be empty",
            ));
        }
        match (&self.certificate_issuer, node.role.is_control_plane()) {
            (Some(issuer), true) if issuer.certificate_pem == self.security.trust_root_pem => {
                Ok(())
            }
            (None, false) => Ok(()),
            _ => Err(CliError::invalid_api_response(
                "launch certificate issuer does not match the local node role and trust root",
            )),
        }
    }

    pub(crate) fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub(crate) fn matches_bootstrap(
        &self,
        cluster: &ClusterConfig,
        node_id: &NodeId,
        data_directory: &Path,
        containerd_socket: &Path,
        etcd_binary: &Path,
        authority: &ClusterCertificateAuthority,
        launch_policy: &ClusterLaunchPolicy,
    ) -> bool {
        self.cluster == *cluster
            && self.node_id == *node_id
            && self.data_directory == data_directory
            && self.containerd_socket == containerd_socket
            && self.etcd_binary.as_deref() == Some(etcd_binary)
            && self.store_mode == StoreLaunchDocument::Bootstrap
            && self.certificate_issuer.as_ref() == Some(authority)
            && self.datadog == launch_policy.datadog
            && self.depot == launch_policy.depot
            && self.log_backup == launch_policy.log_backup
            && self.preview == launch_policy.preview
            && self.nixos_upgrade == launch_policy.nixos_upgrade
    }
}

pub(crate) fn validate_etcd_binary(role: NodeRole, path: Option<&Path>) -> Result<(), CliError> {
    match (role.is_control_plane(), path) {
        (true, Some(path)) if path.is_absolute() => Ok(()),
        (true, Some(_)) => Err(CliError::invalid_input(
            "control-plane nodes require an absolute --etcd-binary path",
        )),
        (true, None) => Err(CliError::invalid_input(
            "control-plane nodes require --etcd-binary",
        )),
        (false, None) => Ok(()),
        (false, Some(_)) => Err(CliError::invalid_input(
            "worker nodes must not configure --etcd-binary",
        )),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", tag = "kind", deny_unknown_fields)]
enum StoreLaunchDocument {
    Bootstrap,
    Join { ticket: StoreJoinTicket },
    Restart,
    Client,
}

impl StoreLaunchDocument {
    fn cutover(role: NodeRole) -> Self {
        if role.is_control_plane() {
            Self::Restart
        } else {
            Self::Client
        }
    }

    fn joined(
        node_id: &NodeId,
        role: NodeRole,
        ticket: Option<StoreJoinTicket>,
    ) -> Result<Self, CliError> {
        match (role.is_control_plane(), ticket) {
            (true, Some(ticket)) if ticket.node_id() == node_id => Ok(Self::Join { ticket }),
            (false, None) => Ok(Self::Client),
            _ => Err(CliError::invalid_api_response(
                "join grant store ticket does not match the local node role",
            )),
        }
    }

    fn validate(&self, node_id: &NodeId, role: NodeRole) -> Result<(), CliError> {
        match self {
            Self::Bootstrap if role == NodeRole::Master => Ok(()),
            Self::Join { ticket }
                if role.is_control_plane()
                    && role != NodeRole::Master
                    && ticket.node_id() == node_id =>
            {
                Ok(())
            }
            Self::Restart if role.is_control_plane() => Ok(()),
            Self::Client if !role.is_control_plane() => Ok(()),
            _ => Err(CliError::invalid_input(
                "launch store mode does not match the local node role",
            )),
        }
    }
}

fn cluster_error(action: &str, error: impl std::fmt::Display) -> CliError {
    CliError::cluster(action, error.to_string())
}
