use std::collections::BTreeMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Write};
use std::path::{Path, PathBuf};
use std::process::Command;

use cluster::initialize_restored_etcd_member;
use kernel_api::{ClusterId, NodeId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::LegacySnapshot;
use crate::legacy_identity::LegacyClusterIdentity;
use crate::legacy_membership::LegacyMembershipCatalog;
use crate::legacy_nodes::LegacyNodeCatalog;

const RESTORE_SCHEMA_VERSION: u32 = 1;
const PARTIAL_DIRECTORY: &str = ".store-cutover.partial";
const INTENT_FILE: &str = "cutover-restore-intent.json";
const COMPLETION_FILE: &str = "cutover-restore.json";

/// One rewritten embedded-etcd member produced by native snapshot restore.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyStoreRestoreMember {
    node_id: NodeId,
    host_address: std::net::Ipv4Addr,
    peer_port: u16,
    member_name: String,
    peer_url: String,
}

/// Reviewed topology used to restore a migrated native etcd snapshot.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyStoreRestorePlan {
    schema_version: u32,
    cluster_id: ClusterId,
    logical_source_sha256: String,
    initial_cluster: String,
    members: BTreeMap<NodeId, LegacyStoreRestoreMember>,
}

impl LegacyStoreRestorePlan {
    /// Returns the migrated cluster identity.
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Returns the logical legacy snapshot digest.
    pub fn logical_source_sha256(&self) -> &str {
        &self.logical_source_sha256
    }

    /// Returns restored members in stable node-ID order.
    pub const fn members(&self) -> &BTreeMap<NodeId, LegacyStoreRestoreMember> {
        &self.members
    }
}

impl LegacyStoreRestoreMember {
    /// Returns the node that will own this restored member.
    pub const fn node_id(&self) -> &NodeId {
        &self.node_id
    }

    /// Returns the rewritten etcd member name.
    pub fn member_name(&self) -> &str {
        &self.member_name
    }

    /// Returns the rewritten peer endpoint.
    pub fn peer_url(&self) -> &str {
        &self.peer_url
    }
}

/// Result of one node-local store restore.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum LegacyStoreRestoreOutcome {
    /// A new provider tree was restored and atomically installed.
    Restored,
    /// The exact snapshot-bound provider tree was already installed.
    AlreadyComplete,
}

/// Secret-free proof that one node-local store tree matches reviewed inputs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyStoreRestoreReport {
    schema_version: u32,
    outcome: LegacyStoreRestoreOutcome,
    cluster_id: ClusterId,
    node_id: NodeId,
    logical_source_sha256: String,
    native_snapshot_sha256: String,
    store_directory: PathBuf,
}

impl LegacyStoreRestoreReport {
    /// Returns whether this invocation installed or reused the exact restore.
    pub const fn outcome(&self) -> LegacyStoreRestoreOutcome {
        self.outcome
    }

    /// Returns the atomically installed provider directory.
    pub fn store_directory(&self) -> &Path {
        &self.store_directory
    }
}

/// Extracts and validates the exact store topology from a logical cutover snapshot.
pub fn plan_legacy_store_restore(
    snapshot: &LegacySnapshot,
) -> Result<LegacyStoreRestorePlan, LegacyStoreRestoreError> {
    let nodes = LegacyNodeCatalog::decode(snapshot.entries())
        .map_err(|error| invalid_legacy_state(error.to_string()))?;
    let identity = LegacyClusterIdentity::decode(&nodes.unclaimed, &nodes)
        .map_err(|error| invalid_legacy_state(error.to_string()))?;
    LegacyMembershipCatalog::decode(&identity.unclaimed, &nodes)
        .map_err(|error| invalid_legacy_state(error.to_string()))?;

    let members = nodes
        .control_plane_endpoints()
        .into_iter()
        .map(|(node_id, endpoint)| {
            let member_name = format!("maestro-{node_id}");
            let peer_url = format!("https://{}:{}", endpoint.host_ip, endpoint.etcd_peer_port);
            (
                node_id.clone(),
                LegacyStoreRestoreMember {
                    node_id,
                    host_address: endpoint.host_ip,
                    peer_port: endpoint.etcd_peer_port,
                    member_name,
                    peer_url,
                },
            )
        })
        .collect::<BTreeMap<_, _>>();
    let initial_cluster = members
        .values()
        .map(|member| format!("{}={}", member.member_name, member.peer_url))
        .collect::<Vec<_>>()
        .join(",");
    Ok(LegacyStoreRestorePlan {
        schema_version: RESTORE_SCHEMA_VERSION,
        cluster_id: identity.cluster_id().clone(),
        logical_source_sha256: hex::encode(snapshot.digest()),
        initial_cluster,
        members,
    })
}

/// Restores one member from the post-migration native snapshot.
pub fn restore_legacy_store(
    plan: &LegacyStoreRestorePlan,
    node_id: &NodeId,
    native_snapshot: &Path,
    data_directory: &Path,
    etcdutl_binary: &Path,
) -> Result<LegacyStoreRestoreReport, LegacyStoreRestoreError> {
    validate_restore_paths(native_snapshot, data_directory, etcdutl_binary)?;
    let member =
        plan.members
            .get(node_id)
            .ok_or_else(|| LegacyStoreRestoreError::UnknownMember {
                node_id: node_id.clone(),
            })?;
    let native_snapshot_sha256 = digest_file(native_snapshot)?;
    let binding = RestoreBinding::new(plan, member, native_snapshot_sha256);
    let store_directory = data_directory.join("store");
    if store_directory.exists() {
        verify_installed(&store_directory, &binding)?;
        return Ok(report(
            binding,
            store_directory,
            LegacyStoreRestoreOutcome::AlreadyComplete,
        ));
    }

    fs::create_dir_all(data_directory)
        .map_err(|source| io_error("create data directory", data_directory, source))?;
    let partial = data_directory.join(PARTIAL_DIRECTORY);
    recover_owned_partial(&partial, &binding)?;
    create_private_directory(&partial)?;
    write_new_private_json(&partial.join(INTENT_FILE), &binding)?;

    let restored_data = partial.join("data");
    let output = Command::new(etcdutl_binary)
        .arg("snapshot")
        .arg("restore")
        .arg(native_snapshot)
        .arg("--data-dir")
        .arg(&restored_data)
        .arg("--name")
        .arg(&member.member_name)
        .arg("--initial-cluster")
        .arg(&plan.initial_cluster)
        .arg("--initial-advertise-peer-urls")
        .arg(&member.peer_url)
        .arg("--initial-cluster-token")
        .arg(format!("maestro-{}", plan.cluster_id))
        .output()
        .map_err(|source| io_error("execute etcdutl", etcdutl_binary, source))?;
    if !output.status.success() {
        return Err(LegacyStoreRestoreError::EtcdutlFailed {
            status: output.status.code(),
            stderr: String::from_utf8_lossy(&output.stderr).trim().to_owned(),
        });
    }
    initialize_restored_etcd_member(
        &partial,
        plan.cluster_id.clone(),
        node_id.clone(),
        &binding.native_snapshot_sha256,
    )?;
    write_new_private_json(&partial.join(COMPLETION_FILE), &binding)?;
    sync_directory(&partial)?;
    fs::rename(&partial, &store_directory)
        .map_err(|source| io_error("install restored store", &store_directory, source))?;
    sync_directory(data_directory)?;
    Ok(report(
        binding,
        store_directory,
        LegacyStoreRestoreOutcome::Restored,
    ))
}

/// Verifies one installed member against both reviewed snapshot artifacts.
pub fn verify_legacy_store_restore(
    plan: &LegacyStoreRestorePlan,
    node_id: &NodeId,
    native_snapshot: &Path,
    data_directory: &Path,
) -> Result<LegacyStoreRestoreReport, LegacyStoreRestoreError> {
    if !data_directory.is_absolute() {
        return Err(LegacyStoreRestoreError::UnsafePath {
            path: data_directory.to_path_buf(),
            reason: "data directory must be absolute",
        });
    }
    validate_private_regular_file(native_snapshot, "native snapshot")?;
    let member =
        plan.members
            .get(node_id)
            .ok_or_else(|| LegacyStoreRestoreError::UnknownMember {
                node_id: node_id.clone(),
            })?;
    let binding = RestoreBinding::new(plan, member, digest_file(native_snapshot)?);
    let store_directory = data_directory.join("store");
    verify_installed(&store_directory, &binding)?;
    Ok(report(
        binding,
        store_directory,
        LegacyStoreRestoreOutcome::AlreadyComplete,
    ))
}

fn validate_restore_paths(
    native_snapshot: &Path,
    data_directory: &Path,
    etcdutl_binary: &Path,
) -> Result<(), LegacyStoreRestoreError> {
    if !data_directory.is_absolute() {
        return Err(LegacyStoreRestoreError::UnsafePath {
            path: data_directory.to_path_buf(),
            reason: "data directory must be absolute",
        });
    }
    if !etcdutl_binary.is_absolute() {
        return Err(LegacyStoreRestoreError::UnsafePath {
            path: etcdutl_binary.to_path_buf(),
            reason: "etcdutl binary must be absolute",
        });
    }
    validate_private_regular_file(native_snapshot, "native snapshot")
}

fn validate_private_regular_file(
    path: &Path,
    description: &'static str,
) -> Result<(), LegacyStoreRestoreError> {
    if !path.is_absolute() {
        return Err(LegacyStoreRestoreError::UnsafePath {
            path: path.to_path_buf(),
            reason: "input file must be absolute",
        });
    }
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect input", path, source))?;
    if !metadata.file_type().is_file() {
        return Err(LegacyStoreRestoreError::UnsafePath {
            path: path.to_path_buf(),
            reason: "input must be a regular file",
        });
    }
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        let mode = metadata.permissions().mode() & 0o777;
        if mode & 0o077 != 0 {
            return Err(LegacyStoreRestoreError::InsecurePermissions {
                description,
                path: path.to_path_buf(),
                mode,
            });
        }
    }
    Ok(())
}

fn digest_file(path: &Path) -> Result<String, LegacyStoreRestoreError> {
    let mut file = File::open(path).map_err(|source| io_error("open input", path, source))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 64 * 1_024];
    loop {
        let count = file
            .read(&mut buffer)
            .map_err(|source| io_error("read input", path, source))?;
        if count == 0 {
            break;
        }
        let Some(bytes) = buffer.get(..count) else {
            return Err(io_error(
                "read input",
                path,
                std::io::Error::other("reader returned more bytes than its buffer"),
            ));
        };
        digest.update(bytes);
    }
    Ok(hex::encode(digest.finalize()))
}

fn recover_owned_partial(
    partial: &Path,
    expected: &RestoreBinding,
) -> Result<(), LegacyStoreRestoreError> {
    if !partial.exists() {
        return Ok(());
    }
    let metadata = fs::symlink_metadata(partial)
        .map_err(|source| io_error("inspect partial restore", partial, source))?;
    if !metadata.file_type().is_dir() {
        return Err(LegacyStoreRestoreError::DestinationOccupied {
            path: partial.to_path_buf(),
        });
    }
    let actual = read_binding(&partial.join(INTENT_FILE))?;
    if actual != *expected {
        return Err(LegacyStoreRestoreError::BindingMismatch {
            path: partial.join(INTENT_FILE),
        });
    }
    fs::remove_dir_all(partial)
        .map_err(|source| io_error("remove owned partial restore", partial, source))
}

fn verify_installed(
    store_directory: &Path,
    expected: &RestoreBinding,
) -> Result<(), LegacyStoreRestoreError> {
    let metadata = fs::symlink_metadata(store_directory)
        .map_err(|source| io_error("inspect restored store", store_directory, source))?;
    if !metadata.file_type().is_dir() {
        return Err(LegacyStoreRestoreError::DestinationOccupied {
            path: store_directory.to_path_buf(),
        });
    }
    let marker = store_directory.join(COMPLETION_FILE);
    if read_binding(&marker)? != *expected {
        return Err(LegacyStoreRestoreError::BindingMismatch { path: marker });
    }
    for required in [
        store_directory.join("data/member"),
        store_directory.join("provider-state.json"),
    ] {
        if !required.exists() {
            return Err(LegacyStoreRestoreError::IncompleteDestination { path: required });
        }
    }
    Ok(())
}

fn report(
    binding: RestoreBinding,
    store_directory: PathBuf,
    outcome: LegacyStoreRestoreOutcome,
) -> LegacyStoreRestoreReport {
    LegacyStoreRestoreReport {
        schema_version: RESTORE_SCHEMA_VERSION,
        outcome,
        cluster_id: binding.cluster_id,
        node_id: binding.node_id,
        logical_source_sha256: binding.logical_source_sha256,
        native_snapshot_sha256: binding.native_snapshot_sha256,
        store_directory,
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct RestoreBinding {
    schema_version: u32,
    cluster_id: ClusterId,
    node_id: NodeId,
    logical_source_sha256: String,
    native_snapshot_sha256: String,
    member_name: String,
    peer_url: String,
    initial_cluster: String,
}

impl RestoreBinding {
    fn new(
        plan: &LegacyStoreRestorePlan,
        member: &LegacyStoreRestoreMember,
        native_snapshot_sha256: String,
    ) -> Self {
        Self {
            schema_version: RESTORE_SCHEMA_VERSION,
            cluster_id: plan.cluster_id.clone(),
            node_id: member.node_id.clone(),
            logical_source_sha256: plan.logical_source_sha256.clone(),
            native_snapshot_sha256,
            member_name: member.member_name.clone(),
            peer_url: member.peer_url.clone(),
            initial_cluster: plan.initial_cluster.clone(),
        }
    }
}

fn create_private_directory(path: &Path) -> Result<(), LegacyStoreRestoreError> {
    fs::create_dir(path).map_err(|source| io_error("create partial restore", path, source))?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o700))
            .map_err(|source| io_error("protect partial restore", path, source))?;
    }
    sync_directory(
        path.parent()
            .ok_or_else(|| LegacyStoreRestoreError::UnsafePath {
                path: path.to_path_buf(),
                reason: "partial restore has no parent",
            })?,
    )
}

fn write_new_private_json(
    path: &Path,
    value: &impl Serialize,
) -> Result<(), LegacyStoreRestoreError> {
    let mut encoded =
        serde_json::to_vec_pretty(value).map_err(|error| LegacyStoreRestoreError::Json {
            path: path.to_path_buf(),
            message: error.to_string(),
        })?;
    encoded.push(b'\n');
    let mut options = OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options
        .open(path)
        .map_err(|source| io_error("create restore marker", path, source))?;
    file.write_all(&encoded)
        .and_then(|()| file.sync_all())
        .map_err(|source| io_error("persist restore marker", path, source))
}

fn read_binding(path: &Path) -> Result<RestoreBinding, LegacyStoreRestoreError> {
    let bytes = fs::read(path).map_err(|source| io_error("read restore marker", path, source))?;
    serde_json::from_slice(&bytes).map_err(|error| LegacyStoreRestoreError::Json {
        path: path.to_path_buf(),
        message: error.to_string(),
    })
}

fn sync_directory(path: &Path) -> Result<(), LegacyStoreRestoreError> {
    File::open(path)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync directory", path, source))
}

fn invalid_legacy_state(message: String) -> LegacyStoreRestoreError {
    LegacyStoreRestoreError::InvalidLegacyState { message }
}

fn io_error(action: &'static str, path: &Path, source: std::io::Error) -> LegacyStoreRestoreError {
    LegacyStoreRestoreError::Io {
        action,
        path: path.to_path_buf(),
        source,
    }
}

/// A native store restore could not be planned, applied, or verified safely.
#[derive(Debug, thiserror::Error)]
pub enum LegacyStoreRestoreError {
    /// Legacy membership and node state did not describe a restorable topology.
    #[error("legacy store topology is invalid: {message}")]
    InvalidLegacyState { message: String },
    /// The selected node is not a migrated control-plane member.
    #[error("node `{node_id}` is not a migrated store member")]
    UnknownMember { node_id: NodeId },
    /// An operator-supplied path violated the offline restore contract.
    #[error("unsafe path `{}`: {reason}", path.display())]
    UnsafePath { path: PathBuf, reason: &'static str },
    /// A sensitive snapshot was visible to other users.
    #[error(
        "{description} `{}` has insecure permissions {mode:#o}; require owner-only access",
        path.display()
    )]
    InsecurePermissions {
        description: &'static str,
        path: PathBuf,
        mode: u32,
    },
    /// A provider or partial directory exists without proof that this tool owns it.
    #[error("restore destination `{}` is already occupied", path.display())]
    DestinationOccupied { path: PathBuf },
    /// Existing restore state belongs to different reviewed inputs.
    #[error("restore binding `{}` does not match the reviewed inputs", path.display())]
    BindingMismatch { path: PathBuf },
    /// A completion marker exists without all required provider state.
    #[error("restored store is missing `{}`", path.display())]
    IncompleteDestination { path: PathBuf },
    /// etcdutl rejected the native snapshot or restore topology.
    #[error("etcdutl restore failed with status {status:?}: {stderr}")]
    EtcdutlFailed { status: Option<i32>, stderr: String },
    /// A restore marker could not be encoded or decoded.
    #[error("restore JSON `{}` is invalid: {message}", path.display())]
    Json { path: PathBuf, message: String },
    /// The embedded provider rejected restored member state.
    #[error(transparent)]
    Provider(#[from] cluster::StoreProviderError),
    /// A filesystem or process operation failed.
    #[error("{action} `{}` failed: {source}", path.display())]
    Io {
        action: &'static str,
        path: PathBuf,
        #[source]
        source: std::io::Error,
    },
}
