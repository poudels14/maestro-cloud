use std::fs::{self, File};
use std::io::{Read, Write};
use std::net::Ipv4Addr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use anyhow::{Context, Result, anyhow, bail};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::cluster::NodeRole;
use crate::cluster::join::ClusterVoterCache;
use crate::config::ClusterConfig;

const STATE_FILE: &str = "cluster-auto-migration.json";
const NETWORK_MARKER_FILE: &str = "cluster-network-migration-required";
const IMAGE_MARKER_FILE: &str = "cluster-image-migration-required";
const BACKUP_FORMAT_VERSION: u32 = 2;
const BACKUP_SOURCE_PATH: &str = "system/etcd/data";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct EtcdBackupManifest {
    backup_format_version: u32,
    manifest_created_at_unix_ms: u64,
    manifest_created_by_maestro_version: String,
    source_data_path: String,
    data_tree_sha256: String,
    cluster_id: String,
    cluster_name: String,
    legacy_member_name: String,
    host_ip: Ipv4Addr,
    initial_voter_host_ips: Vec<Ipv4Addr>,
    api_port: u16,
    etcd_client_port: u16,
    etcd_peer_port: u16,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct AutomaticMigration {
    cluster_id: String,
    legacy_member_name: String,
    host_ip: Ipv4Addr,
    initial_voter_host_ips: Vec<Ipv4Addr>,
    api_port: u16,
    etcd_client_port: u16,
    etcd_peer_port: u16,
    ca_sha256: String,
}

#[derive(Debug)]
pub struct MigrationResult {
    pub cluster_id: String,
    pub ca_sha256: String,
    pub etcd_backup: PathBuf,
    pub etcd_backup_manifest: PathBuf,
}

pub fn is_in_progress(data_dir: &Path) -> bool {
    state_path(data_dir).exists()
}

pub fn network_reconfiguration_required(data_dir: &Path) -> bool {
    network_marker_path(data_dir).exists()
}

pub fn complete_network_reconfiguration(data_dir: &Path) -> Result<()> {
    let path = network_marker_path(data_dir);
    if path.exists() {
        fs::remove_file(&path)?;
        sync_directory(
            path.parent()
                .ok_or_else(|| anyhow!("network migration marker has no parent directory"))?,
        )?;
    }
    Ok(())
}

pub fn image_publication_required(data_dir: &Path) -> bool {
    image_marker_path(data_dir).exists()
}

pub async fn publish_legacy_images(
    data_dir: &Path,
    shared_registry: &str,
    runtime: Arc<dyn crate::runtime::RuntimeProvider>,
    store: Arc<dyn crate::deployment::store::ClusterStore>,
    token: &crate::cluster::types::LeadershipToken,
) -> Result<()> {
    if !image_publication_required(data_dir) {
        return Ok(());
    }
    let registry = shared_registry.trim().trim_end_matches('/');
    if registry.is_empty() {
        bail!("cluster.image-registry is required to publish migrated local images");
    }

    for service_id in store.list_service_ids().await? {
        if let Some(mut info) = store.read_service_info(&service_id).await?
            && let Some(source) = info.config.image.clone()
        {
            let digest = Sha256::digest(source.as_bytes());
            let suffix = digest[..6]
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>();
            let target = format!("{registry}/{service_id}:legacy-{suffix}");
            if ensure_registry_image(runtime.as_ref(), &source, &target, true, &service_id).await? {
                info.config.image = Some(target);
                store
                    .update_service_config_fenced(token, &service_id, info.config)
                    .await?;
            }
        }

        for mut deployment in store.list_service_deployments(&service_id).await? {
            let Some(build) = deployment.build.as_ref() else {
                continue;
            };
            let source = build.docker_image_id.clone();
            let target = format!("{registry}/{service_id}:{}", deployment.id);
            let required = matches!(
                deployment.status,
                crate::deployment::types::DeploymentStatus::Queued
                    | crate::deployment::types::DeploymentStatus::Building
                    | crate::deployment::types::DeploymentStatus::PendingReady
                    | crate::deployment::types::DeploymentStatus::Ready
                    | crate::deployment::types::DeploymentStatus::Draining
            );
            if !ensure_registry_image(runtime.as_ref(), &source, &target, required, &service_id)
                .await?
            {
                continue;
            }
            if source != target {
                deployment
                    .build
                    .as_mut()
                    .expect("build checked above")
                    .docker_image_id = target;
                let reference = crate::deployment::types::Deployment {
                    service_id: service_id.clone(),
                    id: deployment.id.clone(),
                    replica_index: 0,
                };
                store
                    .update_deployment_build_info_fenced(token, &reference, &deployment)
                    .await?;
            }
        }
    }

    remove_marker(&image_marker_path(data_dir))?;
    Ok(())
}

async fn ensure_registry_image(
    runtime: &dyn crate::runtime::RuntimeProvider,
    source: &str,
    target: &str,
    required: bool,
    service_id: &str,
) -> Result<bool> {
    if runtime.image_exists(source).await? {
        if source != target {
            runtime.tag_image(source, target).await.with_context(|| {
                format!("failed to tag migrated image `{source}` as `{target}`")
            })?;
        }
        runtime
            .push_image(target)
            .await
            .with_context(|| format!("failed to publish migrated image `{target}`"))?;
        runtime
            .pull_image(target, None, None)
            .await
            .with_context(|| format!("failed to verify published migrated image `{target}`"))?;
        return Ok(true);
    }

    match runtime.pull_image(source, None, None).await {
        Ok(()) => Ok(false),
        Err(_error) if !required => Ok(false),
        Err(error) => Err(error).with_context(|| {
            format!(
                "runnable migrated service `{service_id}` uses image `{source}`, which is neither present locally nor pullable"
            )
        }),
    }
}

pub fn is_legacy_candidate(config: &ClusterConfig, role: NodeRole, data_dir: &Path) -> bool {
    role.is_voter()
        && !config.nodes.is_empty()
        && legacy_member_path(data_dir).exists()
        && !cluster_id_path(data_dir).exists()
}

#[cfg(test)]
fn installed_ca_fingerprint(data_dir: &Path) -> Result<Option<String>> {
    if !cluster_id_path(data_dir).exists() {
        return Ok(None);
    }
    let path = certs_path(data_dir).join("ca.pem");
    match fs::read_to_string(&path) {
        Ok(certificate) => Ok(Some(crate::utils::certs::certificate_fingerprint(
            &certificate,
        )?)),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

pub fn legacy_etcd_container_name(config: &ClusterConfig, data_dir: &Path) -> Result<String> {
    let member = legacy_member_name(config, data_dir)?;
    let suffix = member
        .strip_prefix("maestro-")
        .ok_or_else(|| anyhow!("legacy etcd member name has an invalid prefix"))?;
    Ok(format!("maestro-etcd-{suffix}"))
}

pub fn migrate(
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
    host_ip: Ipv4Addr,
) -> Result<MigrationResult> {
    // A controller restart does not necessarily stop its detached system containers. Keep an
    // exclusive lock on the bbolt database throughout backup and identity installation so that a
    // failed container stop can never turn the verified copy into a live-store copy.
    let _offline_etcd = acquire_offline_etcd_lock(data_dir)?;
    let migration = match read_state(data_dir)? {
        Some(migration) => {
            validate_state(&migration, config, role, data_dir, host_ip)?;
            ensure_etcd_backup(
                config,
                data_dir,
                host_ip,
                &migration.legacy_member_name,
                Some(&migration.cluster_id),
            )?;
            migration
        }
        None => prepare(config, role, data_dir, host_ip)?,
    };
    install(&migration, config, role, data_dir)?;
    Ok(MigrationResult {
        cluster_id: migration.cluster_id,
        ca_sha256: migration.ca_sha256,
        etcd_backup: backup_path(data_dir),
        etcd_backup_manifest: backup_manifest_path(data_dir),
    })
}

fn prepare(
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
    host_ip: Ipv4Addr,
) -> Result<AutomaticMigration> {
    validate_candidate(config, role, data_dir, host_ip)?;
    let legacy_member_name = legacy_member_name(config, data_dir)?;
    let backup = ensure_etcd_backup(config, data_dir, host_ip, &legacy_member_name, None)?;

    let temporary_certs = temporary_certs_path(data_dir);
    if temporary_certs.exists() {
        fs::remove_dir_all(&temporary_certs)?;
    }
    let ca = crate::utils::certs::generate_cluster_ca()?;
    let certificates =
        crate::utils::certs::generate_cluster_node_certs(&ca, host_ip, NodeRole::Voter)?;
    crate::utils::certs::write_etcd_certs(&temporary_certs, &certificates)?;
    crate::utils::certs::write_cluster_ca(&temporary_certs.join("cluster-ca"), &ca)?;
    sync_tree(&temporary_certs)?;

    let ca_sha256 = crate::utils::certs::certificate_fingerprint(&ca.cert_pem)?;
    let migration = AutomaticMigration {
        cluster_id: backup.cluster_id,
        legacy_member_name,
        host_ip,
        initial_voter_host_ips: configured_host_ips(config),
        api_port: config.api_port,
        etcd_client_port: config.etcd_client_port,
        etcd_peer_port: config.etcd_peer_port,
        ca_sha256,
    };
    persist_state(data_dir, &migration)?;
    Ok(migration)
}

fn install(
    migration: &AutomaticMigration,
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
) -> Result<()> {
    let endpoint = crate::cluster::ClusterNodeEndpoint {
        host_ip: migration.host_ip,
        api_port: migration.api_port,
        gateway_port: config.gateway_port,
        etcd_client_port: migration.etcd_client_port,
        etcd_peer_port: migration.etcd_peer_port,
    };
    crate::cluster::identity::persist_cluster_id(data_dir, &migration.cluster_id)?;
    crate::cluster::bootstrap::prepare_legacy_migration(
        data_dir,
        &migration.cluster_id,
        &migration.legacy_member_name,
    )?;
    crate::cluster::join::persist_voter_cache(
        data_dir,
        &ClusterVoterCache {
            cluster_id: migration.cluster_id.clone(),
            voter_host_ips: vec![migration.host_ip],
            voter_endpoints: vec![endpoint],
            initial_voter_host_ips: vec![migration.host_ip],
            initial_voter_endpoints: vec![endpoint],
            api_port: migration.api_port,
            etcd_client_port: migration.etcd_client_port,
            etcd_peer_port: migration.etcd_peer_port,
        },
    )?;
    install_certificates(data_dir, &migration.ca_sha256)?;
    validate_state(migration, config, role, data_dir, migration.host_ip)?;
    persist_network_marker(data_dir)?;
    persist_marker(
        &image_marker_path(data_dir),
        b"publish locally built service images before starting clustered workloads\n",
    )?;
    remove_state(data_dir)
}

fn validate_candidate(
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
    host_ip: Ipv4Addr,
) -> Result<()> {
    if !role.is_voter() {
        bail!("only a legacy voter can be migrated into cluster mode");
    }
    if config.master_node()?.1.endpoint.host_ip() != host_ip {
        bail!("the cluster master must resolve to this legacy host");
    }
    if !legacy_member_path(data_dir).exists() {
        bail!("legacy etcd member data is absent");
    }
    if cluster_id_path(data_dir).exists() {
        bail!("cluster identity already exists without a resumable migration marker");
    }
    if backup_certs_path(data_dir).exists() {
        bail!("legacy certificate backup exists without a resumable migration marker");
    }
    legacy_member_name(config, data_dir)?;
    Ok(())
}

fn validate_state(
    migration: &AutomaticMigration,
    config: &ClusterConfig,
    role: NodeRole,
    data_dir: &Path,
    host_ip: Ipv4Addr,
) -> Result<()> {
    if !role.is_voter()
        || migration.host_ip != host_ip
        || migration.initial_voter_host_ips != configured_host_ips(config)
        || migration.api_port != config.api_port
        || migration.etcd_client_port != config.etcd_client_port
        || migration.etcd_peer_port != config.etcd_peer_port
        || migration.legacy_member_name != legacy_member_name(config, data_dir)?
    {
        bail!("cluster configuration changed while the legacy migration was in progress");
    }
    Ok(())
}

fn legacy_member_name(config: &ClusterConfig, data_dir: &Path) -> Result<String> {
    let path = data_dir.join("system/cluster-instance-id");
    let suffix = fs::read_to_string(&path).with_context(|| {
        format!(
            "legacy cluster instance id is missing at {}",
            path.display()
        )
    })?;
    let suffix = suffix.trim();
    if suffix.len() != 4
        || !suffix
            .chars()
            .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        bail!("invalid legacy cluster instance id in {}", path.display());
    }
    Ok(format!("maestro-{}-{suffix}", config.name.to_lowercase()))
}

fn ensure_etcd_backup(
    config: &ClusterConfig,
    data_dir: &Path,
    host_ip: Ipv4Addr,
    legacy_member_name: &str,
    expected_cluster_id: Option<&str>,
) -> Result<EtcdBackupManifest> {
    let source = etcd_data_path(data_dir);
    let backup = backup_path(data_dir);
    let manifest_path = backup_manifest_path(data_dir);
    recover_backup_finalization(data_dir)?;

    if backup.exists() || manifest_path.exists() {
        if !backup.exists() || !manifest_path.exists() {
            bail!(
                "versioned etcd backup is incomplete: expected both {} and {}",
                backup.display(),
                manifest_path.display()
            );
        }
        let manifest = read_backup_manifest(&manifest_path)?;
        validate_backup_manifest(
            &manifest,
            config,
            data_dir,
            host_ip,
            legacy_member_name,
            expected_cluster_id,
        )?;
        return Ok(manifest);
    }

    remove_incomplete_backup(data_dir)?;
    let legacy_backup = legacy_backup_path(data_dir);
    if legacy_backup.exists() {
        let backup_digest = directory_digest(&legacy_backup)?;
        if expected_cluster_id.is_none() && directory_digest(&source)? != backup_digest {
            bail!(
                "unversioned etcd backup at {} differs from the current source; refusing to rename or overwrite either recovery point",
                legacy_backup.display()
            );
        }
        let manifest = new_backup_manifest(
            config,
            host_ip,
            legacy_member_name,
            expected_cluster_id
                .map(ToString::to_string)
                .unwrap_or_else(crate::cluster::identity::new_cluster_id),
            backup_digest,
        )?;
        finalize_backup(&legacy_backup, data_dir, &manifest)?;
        return Ok(manifest);
    }

    let partial = partial_backup_path(data_dir);
    if let Err(error) = copy_directory(&source, &partial) {
        let _ = fs::remove_dir_all(&partial);
        return Err(error);
    }
    let source_digest = directory_digest(&source)?;
    let backup_digest = directory_digest(&partial)?;
    if source_digest != backup_digest {
        let _ = fs::remove_dir_all(&partial);
        bail!("offline etcd backup verification failed");
    }
    let manifest = new_backup_manifest(
        config,
        host_ip,
        legacy_member_name,
        expected_cluster_id
            .map(ToString::to_string)
            .unwrap_or_else(crate::cluster::identity::new_cluster_id),
        backup_digest,
    )?;
    finalize_backup(&partial, data_dir, &manifest)?;
    Ok(manifest)
}

fn recover_backup_finalization(data_dir: &Path) -> Result<()> {
    let backup = backup_path(data_dir);
    let manifest = backup_manifest_path(data_dir);
    let partial = partial_backup_path(data_dir);
    let temporary_manifest = partial_backup_manifest_path(data_dir);

    if backup.exists() && partial.exists() {
        bail!(
            "finalized etcd backup has unexpected partial recovery artifacts; refusing to modify any recovery point"
        );
    }
    if backup.exists() && manifest.exists() && temporary_manifest.exists() {
        bail!(
            "finalized etcd backup has unexpected partial recovery artifacts; refusing to modify any recovery point"
        );
    }
    if backup.exists() && !manifest.exists() {
        if !temporary_manifest.exists() {
            bail!(
                "versioned etcd backup at {} has no recovery manifest; refusing to modify it",
                backup.display()
            );
        }
        let pending = read_backup_manifest(&temporary_manifest)?;
        let actual_digest = directory_digest(&backup)?;
        if pending.data_tree_sha256 != actual_digest {
            bail!("versioned etcd backup digest does not match its pending recovery manifest");
        }
        fs::rename(&temporary_manifest, &manifest)?;
        sync_directory(
            manifest
                .parent()
                .ok_or_else(|| anyhow!("etcd backup manifest has no parent directory"))?,
        )?;
    } else if !backup.exists() && manifest.exists() {
        bail!(
            "etcd recovery manifest exists without its versioned backup at {}",
            manifest.display()
        );
    }
    Ok(())
}

fn remove_incomplete_backup(data_dir: &Path) -> Result<()> {
    let partial = partial_backup_path(data_dir);
    if partial.exists() {
        fs::remove_dir_all(&partial)?;
    }
    let temporary_manifest = partial_backup_manifest_path(data_dir);
    if temporary_manifest.exists() {
        fs::remove_file(&temporary_manifest)?;
    }
    let legacy_partial = legacy_partial_backup_path(data_dir);
    if legacy_partial.exists() {
        fs::remove_dir_all(&legacy_partial)?;
    }
    Ok(())
}

fn finalize_backup(
    source_backup: &Path,
    data_dir: &Path,
    manifest: &EtcdBackupManifest,
) -> Result<()> {
    let backup = backup_path(data_dir);
    let manifest_path = backup_manifest_path(data_dir);
    let temporary_manifest = partial_backup_manifest_path(data_dir);
    if backup.exists() || manifest_path.exists() || temporary_manifest.exists() {
        bail!("refusing to overwrite an existing versioned etcd backup or manifest");
    }
    persist_backup_manifest(&temporary_manifest, manifest)?;
    fs::rename(source_backup, &backup)?;
    let directory = backup
        .parent()
        .ok_or_else(|| anyhow!("etcd backup has no parent directory"))?;
    sync_directory(directory)?;
    fs::rename(&temporary_manifest, &manifest_path)?;
    sync_directory(directory)
}

fn new_backup_manifest(
    config: &ClusterConfig,
    host_ip: Ipv4Addr,
    legacy_member_name: &str,
    cluster_id: String,
    data_tree_sha256: String,
) -> Result<EtcdBackupManifest> {
    let created_at = SystemTime::now().duration_since(UNIX_EPOCH)?;
    Ok(EtcdBackupManifest {
        backup_format_version: BACKUP_FORMAT_VERSION,
        manifest_created_at_unix_ms: u64::try_from(created_at.as_millis())
            .context("backup manifest timestamp exceeds u64")?,
        manifest_created_by_maestro_version: env!("CARGO_PKG_VERSION").to_string(),
        source_data_path: BACKUP_SOURCE_PATH.to_string(),
        data_tree_sha256,
        cluster_id,
        cluster_name: config.name.to_lowercase(),
        legacy_member_name: legacy_member_name.to_string(),
        host_ip,
        initial_voter_host_ips: configured_host_ips(config),
        api_port: config.api_port,
        etcd_client_port: config.etcd_client_port,
        etcd_peer_port: config.etcd_peer_port,
    })
}

fn validate_backup_manifest(
    manifest: &EtcdBackupManifest,
    config: &ClusterConfig,
    data_dir: &Path,
    host_ip: Ipv4Addr,
    legacy_member_name: &str,
    expected_cluster_id: Option<&str>,
) -> Result<()> {
    let valid_cluster_id = manifest.cluster_id.len() == 32
        && manifest
            .cluster_id
            .chars()
            .all(|character| character.is_ascii_hexdigit() && !character.is_ascii_uppercase());
    if manifest.backup_format_version != BACKUP_FORMAT_VERSION
        || manifest.manifest_created_at_unix_ms == 0
        || manifest.manifest_created_by_maestro_version.is_empty()
        || manifest.source_data_path != BACKUP_SOURCE_PATH
        || !valid_cluster_id
        || manifest.cluster_name != config.name.to_lowercase()
        || manifest.legacy_member_name != legacy_member_name
        || manifest.host_ip != host_ip
        || manifest.initial_voter_host_ips != configured_host_ips(config)
        || manifest.api_port != config.api_port
        || manifest.etcd_client_port != config.etcd_client_port
        || manifest.etcd_peer_port != config.etcd_peer_port
        || expected_cluster_id.is_some_and(|expected| expected != manifest.cluster_id)
    {
        bail!("versioned etcd backup manifest conflicts with this migration");
    }
    let actual_digest = directory_digest(&backup_path(data_dir))?;
    if manifest.data_tree_sha256 != actual_digest {
        bail!(
            "versioned etcd backup failed manifest verification; refusing to overwrite or continue"
        );
    }
    Ok(())
}

fn configured_host_ips(config: &ClusterConfig) -> Vec<Ipv4Addr> {
    config
        .nodes
        .values()
        .filter(|node| node.role.is_voter())
        .map(|node| node.endpoint.host_ip())
        .collect()
}

fn read_backup_manifest(path: &Path) -> Result<EtcdBackupManifest> {
    serde_json::from_slice(&fs::read(path)?)
        .with_context(|| format!("failed to parse etcd recovery manifest {}", path.display()))
}

fn persist_backup_manifest(path: &Path, manifest: &EtcdBackupManifest) -> Result<()> {
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(path)?;
    file.write_all(&serde_json::to_vec_pretty(manifest)?)?;
    file.write_all(b"\n")?;
    file.sync_all()?;
    Ok(())
}

fn acquire_offline_etcd_lock(data_dir: &Path) -> Result<File> {
    let database = etcd_data_path(data_dir).join("member/snap/db");
    let file = fs::OpenOptions::new()
        .read(true)
        .write(true)
        .open(&database)
        .with_context(|| format!("failed to open legacy etcd database {}", database.display()))?;

    #[cfg(unix)]
    {
        use std::os::fd::AsRawFd;

        if unsafe { libc::flock(file.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) } != 0 {
            bail!(
                "legacy etcd database is still in use at {}; automatic migration refused to copy a live store",
                database.display()
            );
        }
    }

    Ok(file)
}

fn copy_directory(source: &Path, destination: &Path) -> Result<()> {
    let metadata =
        fs::metadata(source).with_context(|| format!("failed to inspect {}", source.display()))?;
    if !metadata.is_dir() {
        bail!("{} is not a directory", source.display());
    }
    fs::create_dir(destination)?;

    let mut entries = fs::read_dir(source)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let source_path = entry.path();
        let destination_path = destination.join(entry.file_name());
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            copy_directory(&source_path, &destination_path)?;
        } else if file_type.is_file() {
            fs::copy(&source_path, &destination_path)?;
            File::open(&destination_path)?.sync_all()?;
        } else {
            bail!(
                "unsupported entry in etcd data directory: {}",
                source_path.display()
            );
        }
    }
    fs::set_permissions(destination, metadata.permissions())?;
    sync_directory(destination)
}

fn directory_digest(root: &Path) -> Result<String> {
    let mut hasher = Sha256::new();
    hasher.update(b"maestro-etcd-tree-sha256-v1\0");
    hash_directory(root, root, &mut hasher)?;
    Ok(hex::encode(hasher.finalize()))
}

fn hash_directory(root: &Path, current: &Path, hasher: &mut Sha256) -> Result<()> {
    let mut entries = fs::read_dir(current)?.collect::<std::io::Result<Vec<_>>>()?;
    entries.sort_by_key(|entry| entry.file_name());
    for entry in entries {
        let path = entry.path();
        let relative = path
            .strip_prefix(root)
            .map_err(|error| anyhow!("failed to hash etcd backup path: {error}"))?;
        hash_length_prefixed(hasher, relative.as_os_str().as_encoded_bytes())?;
        let file_type = entry.file_type()?;
        if file_type.is_dir() {
            hasher.update(b"d");
            hash_directory(root, &path, hasher)?;
        } else if file_type.is_file() {
            hasher.update(b"f");
            let mut file = File::open(&path)?;
            hasher.update(file.metadata()?.len().to_be_bytes());
            let mut buffer = [0_u8; 64 * 1024];
            loop {
                let read = file.read(&mut buffer)?;
                if read == 0 {
                    break;
                }
                hasher.update(&buffer[..read]);
            }
        } else {
            bail!(
                "unsupported entry in etcd data directory: {}",
                path.display()
            );
        }
    }
    Ok(())
}

fn hash_length_prefixed(hasher: &mut Sha256, bytes: &[u8]) -> Result<()> {
    let length = u64::try_from(bytes.len()).context("etcd backup path length exceeds u64")?;
    hasher.update(length.to_be_bytes());
    hasher.update(bytes);
    Ok(())
}

fn install_certificates(data_dir: &Path, expected_fingerprint: &str) -> Result<()> {
    let certificates = certs_path(data_dir);
    let temporary = temporary_certs_path(data_dir);
    let backup = backup_certs_path(data_dir);

    if certificates.exists() && certificate_directory_matches(&certificates, expected_fingerprint)?
    {
        if temporary.exists() {
            fs::remove_dir_all(&temporary)?;
        }
        return Ok(());
    }
    if !temporary.exists() {
        bail!("prepared cluster certificates are missing");
    }
    if certificates.exists() {
        if backup.exists() {
            bail!("legacy certificate backup already exists before certificate installation");
        }
        fs::rename(&certificates, &backup)?;
        sync_directory(
            certificates
                .parent()
                .ok_or_else(|| anyhow!("certificate directory has no parent"))?,
        )?;
    }
    fs::rename(&temporary, &certificates)?;
    sync_directory(
        certificates
            .parent()
            .ok_or_else(|| anyhow!("certificate directory has no parent"))?,
    )?;
    if !certificate_directory_matches(&certificates, expected_fingerprint)? {
        bail!("installed cluster certificates failed fingerprint verification");
    }
    Ok(())
}

fn certificate_directory_matches(directory: &Path, expected: &str) -> Result<bool> {
    let certificate = match fs::read_to_string(directory.join("ca.pem")) {
        Ok(certificate) => certificate,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(false),
        Err(error) => return Err(error.into()),
    };
    Ok(crate::utils::certs::certificate_fingerprint(&certificate)? == expected)
}

fn sync_tree(directory: &Path) -> Result<()> {
    for entry in fs::read_dir(directory)? {
        let entry = entry?;
        if entry.file_type()?.is_dir() {
            sync_tree(&entry.path())?;
        } else {
            File::open(entry.path())?.sync_all()?;
        }
    }
    sync_directory(directory)
}

fn read_state(data_dir: &Path) -> Result<Option<AutomaticMigration>> {
    let path = state_path(data_dir);
    match fs::read(&path) {
        Ok(bytes) => serde_json::from_slice(&bytes)
            .with_context(|| format!("failed to parse {}", path.display()))
            .map(Some),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(error) => Err(error.into()),
    }
}

fn persist_state(data_dir: &Path, migration: &AutomaticMigration) -> Result<()> {
    let path = state_path(data_dir);
    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("migration state has no parent directory"))?;
    fs::create_dir_all(directory)?;
    let temporary = path.with_extension("tmp");
    if temporary.exists() {
        fs::remove_file(&temporary)?;
    }
    let mut options = fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(migration)?)?;
    file.sync_all()?;
    if path.exists() {
        let _ = fs::remove_file(&temporary);
        bail!("migration state already exists at {}", path.display());
    }
    fs::rename(&temporary, &path)?;
    sync_directory(directory)
}

fn remove_state(data_dir: &Path) -> Result<()> {
    let path = state_path(data_dir);
    if path.exists() {
        fs::remove_file(&path)?;
        sync_directory(
            path.parent()
                .ok_or_else(|| anyhow!("migration state has no parent directory"))?,
        )?;
    }
    Ok(())
}

fn persist_network_marker(data_dir: &Path) -> Result<()> {
    persist_marker(
        &network_marker_path(data_dir),
        b"recreate the container network with the configured cluster subnet\n",
    )
}

fn persist_marker(path: &Path, contents: &[u8]) -> Result<()> {
    if !path.exists() {
        let directory = path
            .parent()
            .ok_or_else(|| anyhow!("network migration marker has no parent directory"))?;
        let temporary = path.with_extension("tmp");
        if temporary.exists() {
            fs::remove_file(&temporary)?;
        }
        let mut file = fs::OpenOptions::new()
            .create_new(true)
            .write(true)
            .open(&temporary)?;
        file.write_all(contents)?;
        file.sync_all()?;
        fs::rename(&temporary, path)?;
        sync_directory(directory)?;
    }
    Ok(())
}

fn remove_marker(path: &Path) -> Result<()> {
    if path.exists() {
        fs::remove_file(path)?;
        sync_directory(
            path.parent()
                .ok_or_else(|| anyhow!("migration marker has no parent directory"))?,
        )?;
    }
    Ok(())
}

fn sync_directory(directory: &Path) -> Result<()> {
    File::open(directory)?.sync_all()?;
    Ok(())
}

fn state_path(data_dir: &Path) -> PathBuf {
    data_dir.join(format!("system/{STATE_FILE}"))
}

fn network_marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(format!("system/{NETWORK_MARKER_FILE}"))
}

fn image_marker_path(data_dir: &Path) -> PathBuf {
    data_dir.join(format!("system/{IMAGE_MARKER_FILE}"))
}

fn cluster_id_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/cluster-id")
}

fn legacy_member_path(data_dir: &Path) -> PathBuf {
    etcd_data_path(data_dir).join("member")
}

fn etcd_data_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data")
}

fn backup_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup.v1")
}

fn backup_manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup.v1.json")
}

fn partial_backup_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup.v1.partial")
}

fn partial_backup_manifest_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup.v1.json.partial")
}

fn legacy_backup_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup")
}

fn legacy_partial_backup_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd/data.legacy-backup.partial")
}

fn certs_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/certs")
}

fn temporary_certs_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/certs.cluster-enable")
}

fn backup_certs_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/certs.legacy-backup")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn config() -> ClusterConfig {
        ClusterConfig {
            name: "prod".to_string(),
            nodes: [
                (
                    "node1".to_string(),
                    crate::config::ClusterNodeConfig {
                        endpoint: crate::config::ClusterEndpointConfig::Address(
                            "10.20.0.11".parse().unwrap(),
                        ),
                        subnet: "172.22.1.0/24".to_string(),
                        role: NodeRole::Master,
                    },
                ),
                (
                    "node2".to_string(),
                    crate::config::ClusterNodeConfig {
                        endpoint: crate::config::ClusterEndpointConfig::Address(
                            "10.20.0.12".parse().unwrap(),
                        ),
                        subnet: "172.22.2.0/24".to_string(),
                        role: NodeRole::Voter,
                    },
                ),
                (
                    "node3".to_string(),
                    crate::config::ClusterNodeConfig {
                        endpoint: crate::config::ClusterEndpointConfig::Address(
                            "10.20.0.13".parse().unwrap(),
                        ),
                        subnet: "172.22.3.0/24".to_string(),
                        role: NodeRole::Voter,
                    },
                ),
            ]
            .into(),
            selected_node: Some("node1".to_string()),
            ..ClusterConfig::default()
        }
    }

    fn legacy_data(label: &str) -> PathBuf {
        let root = std::env::temp_dir().join(format!(
            "maestro-legacy-migration-{label}-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        fs::create_dir_all(root.join("system/etcd/data/member/wal")).unwrap();
        fs::create_dir_all(root.join("system/etcd/data/member/snap")).unwrap();
        fs::write(root.join("system/etcd/data/member/snap/db"), b"legacy-db").unwrap();
        fs::write(
            root.join("system/etcd/data/member/wal/0000000000000000.wal"),
            b"legacy-etcd-state",
        )
        .unwrap();
        fs::write(root.join("system/cluster-instance-id"), b"a1b2\n").unwrap();
        fs::create_dir_all(root.join("system/certs")).unwrap();
        fs::write(root.join("system/certs/legacy.pem"), b"legacy-cert").unwrap();
        root
    }

    #[test]
    fn automatic_migration_preserves_legacy_data_and_identity() {
        let root = legacy_data("complete");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();

        assert_eq!(
            legacy_etcd_container_name(&config, &root).unwrap(),
            "maestro-etcd-prod-a1b2"
        );

        let result = migrate(&config, NodeRole::Hybrid, &root, host_ip).unwrap();

        assert_eq!(
            fs::read(root.join("system/etcd/data/member/wal/0000000000000000.wal")).unwrap(),
            b"legacy-etcd-state"
        );
        assert_eq!(
            fs::read(result.etcd_backup.join("member/wal/0000000000000000.wal")).unwrap(),
            b"legacy-etcd-state"
        );
        let manifest = read_backup_manifest(&result.etcd_backup_manifest).unwrap();
        assert_eq!(manifest.backup_format_version, BACKUP_FORMAT_VERSION);
        assert_eq!(manifest.cluster_id, result.cluster_id);
        assert_eq!(manifest.cluster_name, "prod");
        assert_eq!(manifest.legacy_member_name, "maestro-prod-a1b2");
        assert_eq!(
            manifest.data_tree_sha256,
            directory_digest(&result.etcd_backup).unwrap()
        );
        assert_eq!(
            manifest.manifest_created_by_maestro_version,
            env!("CARGO_PKG_VERSION")
        );
        assert_eq!(
            fs::read(root.join("system/certs.legacy-backup/legacy.pem")).unwrap(),
            b"legacy-cert"
        );
        assert_eq!(
            crate::cluster::identity::load_cluster_id(&root).unwrap(),
            result.cluster_id
        );
        assert!(image_publication_required(&root));
        assert_eq!(
            installed_ca_fingerprint(&root).unwrap().as_deref(),
            Some(result.ca_sha256.as_str())
        );
        assert!(!is_in_progress(&root));
        assert!(network_reconfiguration_required(&root));
        complete_network_reconfiguration(&root).unwrap();
        assert!(!network_reconfiguration_required(&root));
        assert!(image_publication_required(&root));
        let marker: crate::cluster::bootstrap::LegacyMigration = serde_json::from_slice(
            &fs::read(root.join("system/cluster-enable-migration.json")).unwrap(),
        )
        .unwrap();
        assert_eq!(marker.legacy_member_name, "maestro-prod-a1b2");
        let cache = crate::cluster::join::load_voter_cache(&root, &result.cluster_id)
            .unwrap()
            .unwrap();
        assert_eq!(cache.voter_host_ips, vec![host_ip]);
        assert_eq!(cache.initial_voter_host_ips, vec![host_ip]);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn prepared_migration_resumes_without_replacing_its_ca_or_backup() {
        let root = legacy_data("resume");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let prepared = prepare(&config, NodeRole::Hybrid, &root, host_ip).unwrap();
        assert!(is_in_progress(&root));
        assert!(!cluster_id_path(&root).exists());
        assert!(backup_path(&root).exists());

        crate::cluster::identity::persist_cluster_id(&root, &prepared.cluster_id).unwrap();
        let result = migrate(&config, NodeRole::Hybrid, &root, host_ip).unwrap();

        assert_eq!(result.cluster_id, prepared.cluster_id);
        assert_eq!(result.ca_sha256, prepared.ca_sha256);
        assert!(!is_in_progress(&root));
        assert!(network_reconfiguration_required(&root));
        assert_eq!(
            fs::read(result.etcd_backup.join("member/wal/0000000000000000.wal")).unwrap(),
            b"legacy-etcd-state"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn certificate_swap_resumes_after_the_legacy_directory_is_backed_up() {
        let root = legacy_data("cert-resume");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let prepared = prepare(&config, NodeRole::Hybrid, &root, host_ip).unwrap();

        crate::cluster::identity::persist_cluster_id(&root, &prepared.cluster_id).unwrap();
        crate::cluster::bootstrap::prepare_legacy_migration(
            &root,
            &prepared.cluster_id,
            &prepared.legacy_member_name,
        )
        .unwrap();
        fs::rename(certs_path(&root), backup_certs_path(&root)).unwrap();

        let result = migrate(&config, NodeRole::Hybrid, &root, host_ip).unwrap();

        assert_eq!(result.cluster_id, prepared.cluster_id);
        assert_eq!(
            fs::read(root.join("system/certs.legacy-backup/legacy.pem")).unwrap(),
            b"legacy-cert"
        );
        assert_eq!(
            installed_ca_fingerprint(&root).unwrap().as_deref(),
            Some(prepared.ca_sha256.as_str())
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn legacy_config_does_not_trigger_automatic_migration() {
        let root = legacy_data("unchanged");
        let legacy = ClusterConfig {
            name: "prod".to_string(),
            ..ClusterConfig::default()
        };
        assert!(!is_legacy_candidate(&legacy, NodeRole::Hybrid, &root));
        assert!(!is_in_progress(&root));
        assert!(!cluster_id_path(&root).exists());
        assert!(!backup_path(&root).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn finalized_backup_is_reused_without_overwriting_when_source_changes() {
        let root = legacy_data("immutable");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&config, &root).unwrap();
        let first = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        let backup_bytes =
            fs::read(backup_path(&root).join("member/wal/0000000000000000.wal")).unwrap();
        fs::write(
            root.join("system/etcd/data/member/wal/0000000000000000.wal"),
            b"newer-live-state",
        )
        .unwrap();

        let reused = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        assert_eq!(reused, first);
        assert_eq!(
            fs::read(backup_path(&root).join("member/wal/0000000000000000.wal")).unwrap(),
            backup_bytes
        );
        let prepared = prepare(&config, NodeRole::Hybrid, &root, host_ip).unwrap();
        assert_eq!(prepared.cluster_id, first.cluster_id);
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn finalized_backup_rejects_a_conflicting_migration_without_modification() {
        let root = legacy_data("config-conflict");
        let original_config = config();
        let host_ip = original_config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&original_config, &root).unwrap();
        ensure_etcd_backup(&original_config, &root, host_ip, &member_name, None).unwrap();
        let backup_digest = directory_digest(&backup_path(&root)).unwrap();
        let manifest_bytes = fs::read(backup_manifest_path(&root)).unwrap();

        let mut changed = config();
        changed.api_port = 3005;
        let error = ensure_etcd_backup(&changed, &root, host_ip, &member_name, None).unwrap_err();

        assert!(error.to_string().contains("conflicts with this migration"));
        assert_eq!(
            directory_digest(&backup_path(&root)).unwrap(),
            backup_digest
        );
        assert_eq!(
            fs::read(backup_manifest_path(&root)).unwrap(),
            manifest_bytes
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn corrupted_finalized_backup_fails_closed_without_replacement() {
        let root = legacy_data("corrupt-backup");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&config, &root).unwrap();
        ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        let backup_file = backup_path(&root).join("member/wal/0000000000000000.wal");
        fs::write(&backup_file, b"corrupt-but-preserved").unwrap();

        let error = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap_err();
        assert!(error.to_string().contains("refusing to overwrite"));
        assert_eq!(fs::read(&backup_file).unwrap(), b"corrupt-but-preserved");
        assert_eq!(
            fs::read(root.join("system/etcd/data/member/wal/0000000000000000.wal")).unwrap(),
            b"legacy-etcd-state"
        );
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn finalized_backup_rejects_ambiguous_partial_artifacts() {
        let root = legacy_data("ambiguous-backup");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&config, &root).unwrap();
        ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        fs::create_dir(partial_backup_path(&root)).unwrap();

        let error = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap_err();

        assert!(
            error
                .to_string()
                .contains("unexpected partial recovery artifacts")
        );
        assert!(backup_path(&root).exists());
        assert!(backup_manifest_path(&root).exists());
        assert!(partial_backup_path(&root).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn pending_manifest_install_is_recovered_after_backup_rename() {
        let root = legacy_data("manifest-resume");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&config, &root).unwrap();
        let manifest = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        fs::rename(
            backup_manifest_path(&root),
            partial_backup_manifest_path(&root),
        )
        .unwrap();

        let recovered = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        assert_eq!(recovered, manifest);
        assert!(backup_manifest_path(&root).exists());
        assert!(!partial_backup_manifest_path(&root).exists());
        let _ = fs::remove_dir_all(root);
    }

    #[test]
    fn unversioned_verified_backup_is_upgraded_without_copying_it_again() {
        let root = legacy_data("upgrade-backup");
        let config = config();
        let host_ip = config.master_node().unwrap().1.endpoint.host_ip();
        let member_name = legacy_member_name(&config, &root).unwrap();
        copy_directory(&etcd_data_path(&root), &legacy_backup_path(&root)).unwrap();

        let manifest = ensure_etcd_backup(&config, &root, host_ip, &member_name, None).unwrap();
        assert!(!legacy_backup_path(&root).exists());
        assert!(backup_path(&root).exists());
        assert!(backup_manifest_path(&root).exists());
        assert_eq!(
            manifest.data_tree_sha256,
            directory_digest(&backup_path(&root)).unwrap()
        );
        let _ = fs::remove_dir_all(root);
    }

    #[cfg(unix)]
    #[test]
    fn migration_refuses_to_copy_a_live_etcd_store() {
        use std::os::fd::AsRawFd;

        let root = legacy_data("live");
        let database = File::options()
            .read(true)
            .write(true)
            .open(root.join("system/etcd/data/member/snap/db"))
            .unwrap();
        assert_eq!(
            unsafe { libc::flock(database.as_raw_fd(), libc::LOCK_EX | libc::LOCK_NB) },
            0
        );

        let config = config();
        let error = migrate(
            &config,
            NodeRole::Hybrid,
            &root,
            config.master_node().unwrap().1.endpoint.host_ip(),
        )
        .unwrap_err();
        assert!(error.to_string().contains("still in use"));
        assert!(!backup_path(&root).exists());
        assert!(!backup_manifest_path(&root).exists());
        assert!(!cluster_id_path(&root).exists());

        drop(database);
        let _ = fs::remove_dir_all(root);
    }
}
