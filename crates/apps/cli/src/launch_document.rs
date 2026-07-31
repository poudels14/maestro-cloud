use std::path::{Path, PathBuf};

use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use cluster::{ClusterCertificateAuthority, JoinPayload, NodeCertificateBundle, StoreJoinTicket};
use kernel_api::{NodeId, NodeRole, SecretValue};
use kernel_store::{EncryptedValue, derive_key_with_context, open_with_context, seal_with_context};
use serde::{Deserialize, Serialize};

use crate::CliError;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct DaemonLaunchDocument {
    node_id: NodeId,
    data_directory: PathBuf,
    containerd_socket: PathBuf,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    etcd_binary: Option<PathBuf>,
    store_mode: StoreLaunchDocument,
    protected_bootstrap: String,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct ProtectedBootstrap<'a> {
    security: &'a NodeCertificateBundle,
    #[serde(skip_serializing_if = "Option::is_none")]
    certificate_issuer: Option<&'a ClusterCertificateAuthority>,
}

#[derive(Debug, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
struct OwnedProtectedBootstrap {
    security: NodeCertificateBundle,
    certificate_issuer: Option<ClusterCertificateAuthority>,
}

impl DaemonLaunchDocument {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn bootstrap(
        node_id: NodeId,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: PathBuf,
        security: NodeCertificateBundle,
        certificate_issuer: ClusterCertificateAuthority,
        encryption_key: &SecretValue,
    ) -> Result<Self, CliError> {
        let protected_bootstrap = protect_bootstrap(
            &node_id,
            &security,
            Some(&certificate_issuer),
            encryption_key,
        )?;
        let document = Self {
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary: Some(etcd_binary),
            store_mode: StoreLaunchDocument::Bootstrap,
            protected_bootstrap,
        };
        document.validate()?;
        Ok(document)
    }

    pub(crate) fn joined(
        node_id: NodeId,
        role: NodeRole,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: Option<PathBuf>,
        encryption_key: &SecretValue,
        payload: JoinPayload,
    ) -> Result<Self, CliError> {
        let store_mode = StoreLaunchDocument::joined(&node_id, role, payload.store_join_ticket)?;
        let protected_bootstrap = protect_bootstrap(
            &node_id,
            &payload.certificates,
            payload.certificate_issuer.as_ref(),
            encryption_key,
        )?;
        let document = Self {
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary,
            store_mode,
            protected_bootstrap,
        };
        document.validate()?;
        Ok(document)
    }

    #[allow(clippy::too_many_arguments)]
    pub(crate) fn cutover(
        node_id: NodeId,
        role: NodeRole,
        data_directory: PathBuf,
        containerd_socket: PathBuf,
        etcd_binary: PathBuf,
        security: NodeCertificateBundle,
        certificate_issuer: ClusterCertificateAuthority,
        encryption_key: &SecretValue,
    ) -> Result<Self, CliError> {
        let protected_bootstrap = protect_bootstrap(
            &node_id,
            &security,
            role.is_control_plane().then_some(&certificate_issuer),
            encryption_key,
        )?;
        let document = Self {
            node_id,
            data_directory,
            containerd_socket,
            etcd_binary: role.is_control_plane().then_some(etcd_binary),
            store_mode: StoreLaunchDocument::cutover(role),
            protected_bootstrap,
        };
        document.validate()?;
        Ok(document)
    }

    pub(crate) fn validate(&self) -> Result<(), CliError> {
        if !self.data_directory.is_absolute() || !self.containerd_socket.is_absolute() {
            return Err(CliError::invalid_input(
                "launch data directory and containerd socket must be absolute paths",
            ));
        }
        if BASE64.decode(&self.protected_bootstrap).is_err() {
            return Err(CliError::invalid_input(
                "protected bootstrap state must be valid Base64",
            ));
        }
        Ok(())
    }

    pub(crate) fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    pub(crate) fn matches_runtime_paths(
        &self,
        data_directory: &Path,
        containerd_socket: &Path,
        etcd_binary: &Path,
    ) -> bool {
        self.data_directory == data_directory
            && self.containerd_socket == containerd_socket
            && self
                .etcd_binary
                .as_ref()
                .is_none_or(|configured| configured == etcd_binary)
    }

    pub(crate) fn matches_bootstrap(
        &self,
        node_id: &NodeId,
        data_directory: &Path,
        containerd_socket: &Path,
        etcd_binary: &Path,
    ) -> bool {
        self.node_id == *node_id
            && self.data_directory == data_directory
            && self.containerd_socket == containerd_socket
            && self.etcd_binary.as_deref() == Some(etcd_binary)
            && self.store_mode == StoreLaunchDocument::Bootstrap
    }

    pub(crate) fn bootstrap_authority(
        &self,
        encryption_secret: &SecretValue,
    ) -> Result<ClusterCertificateAuthority, CliError> {
        self.open_protected(encryption_secret)?
            .certificate_issuer
            .ok_or_else(|| {
                CliError::invalid_input(
                    "bootstrap launch document does not contain an authority seed",
                )
            })
    }

    pub(crate) fn equivalent(
        &self,
        other: &Self,
        encryption_secret: &SecretValue,
    ) -> Result<bool, CliError> {
        Ok(self.node_id == other.node_id
            && self.data_directory == other.data_directory
            && self.containerd_socket == other.containerd_socket
            && self.etcd_binary == other.etcd_binary
            && self.store_mode == other.store_mode
            && self.open_protected(encryption_secret)?
                == other.open_protected(encryption_secret)?)
    }

    fn open_protected(
        &self,
        encryption_secret: &SecretValue,
    ) -> Result<OwnedProtectedBootstrap, CliError> {
        let envelope = BASE64.decode(&self.protected_bootstrap).map_err(|_| {
            CliError::invalid_input("protected bootstrap state must be valid Base64")
        })?;
        let envelope = EncryptedValue::from_bytes(envelope).map_err(|error| {
            CliError::invalid_input(format!("protected bootstrap state is invalid: {error}"))
        })?;
        let key = derive_key_with_context(encryption_secret.expose(), b"node-bootstrap-v1")
            .map_err(|error| {
                CliError::invalid_input(format!("config encryption key is invalid: {error}"))
            })?;
        let context = format!("maestro-node-bootstrap-v1:{}", self.node_id);
        let plaintext =
            open_with_context(&key, &envelope, context.as_bytes()).map_err(|error| {
                CliError::invalid_input(format!(
                    "could not decrypt protected bootstrap state: {error}"
                ))
            })?;
        serde_json::from_slice(&plaintext)
            .map_err(|error| CliError::json("failed to decode protected bootstrap state", error))
    }
}

fn protect_bootstrap(
    node_id: &NodeId,
    security: &NodeCertificateBundle,
    certificate_issuer: Option<&ClusterCertificateAuthority>,
    encryption_secret: &SecretValue,
) -> Result<String, CliError> {
    let plaintext = serde_json::to_vec(&ProtectedBootstrap {
        security,
        certificate_issuer,
    })
    .map_err(|error| CliError::json("failed to encode protected bootstrap state", error))?;
    let key = derive_key_with_context(encryption_secret.expose(), b"node-bootstrap-v1").map_err(
        |error| CliError::invalid_input(format!("config encryption key is invalid: {error}")),
    )?;
    let context = format!("maestro-node-bootstrap-v1:{node_id}");
    let envelope = seal_with_context(&key, &plaintext, context.as_bytes()).map_err(|error| {
        CliError::invalid_input(format!("failed to encrypt node bootstrap state: {error}"))
    })?;
    Ok(BASE64.encode(envelope.as_bytes()))
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
}
