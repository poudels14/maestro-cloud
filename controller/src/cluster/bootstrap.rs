use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::{Context, Result, anyhow, bail};
use etcd_client::{Client, ConnectOptions, Member, MemberAddOptions, TlsOptions};
use serde::{Deserialize, Serialize};

use crate::cluster::identity;
use crate::cluster::types::{ClusterMeta, ClusterRuntime};
use crate::logs::Logger;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BootstrapAction {
    Restart,
    ForceNewCluster,
    SingleNode,
    BootstrapSeed,
    WaitForAdmission,
    JoinExisting(JoinInfo),
    Worker,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinInfo {
    pub cluster_id: String,
    pub member_id: u64,
    pub member_name: String,
    pub peer_url: String,
    pub initial_cluster: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct BootstrapPermit {
    pub cluster_id: String,
    pub bootstrap_host_ip: std::net::Ipv4Addr,
    pub attempt_id: String,
    pub state: BootstrapPermitState,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct ForceNewClusterMarker {
    cluster_id: String,
    attempt_id: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LegacyMigration {
    pub cluster_id: String,
    pub legacy_member_name: String,
    pub reconciled: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum BootstrapPermitState {
    Armed,
    Starting,
    Joined,
}

pub fn arm_seed(
    data_dir: &Path,
    cluster_id: &str,
    bootstrap_host_ip: std::net::Ipv4Addr,
) -> Result<()> {
    let path = bootstrap_state_path(data_dir);
    if path.exists() || data_dir.join("system/etcd/data/member").exists() {
        bail!("refusing to replace existing etcd bootstrap or member state");
    }
    let permit = BootstrapPermit {
        cluster_id: cluster_id.to_string(),
        bootstrap_host_ip,
        attempt_id: identity::new_instance_id(),
        state: BootstrapPermitState::Armed,
    };
    persist_json(&path, &permit, true)
}

pub fn ensure_seed_armed(
    data_dir: &Path,
    cluster_id: &str,
    bootstrap_host_ip: std::net::Ipv4Addr,
) -> Result<()> {
    match load_permit(data_dir)? {
        None => arm_seed(data_dir, cluster_id, bootstrap_host_ip),
        Some(permit)
            if permit.cluster_id == cluster_id
                && permit.bootstrap_host_ip == bootstrap_host_ip
                && permit.state == BootstrapPermitState::Armed =>
        {
            Ok(())
        }
        Some(_) => bail!("existing bootstrap permit cannot authorize automatic seed recovery"),
    }
}

pub fn seed_is_armed(data_dir: &Path) -> Result<bool> {
    Ok(load_permit(data_dir)?.is_some_and(|permit| permit.state == BootstrapPermitState::Armed))
}

/// Re-arms the designated seed only after the recovery rendezvous has authenticated an empty
/// status from every configured voter. This is deliberately separate from normal provisioning:
/// an unreachable peer can never authorize a clean replacement cluster.
pub fn rearm_seed_after_empty_consensus(
    data_dir: &Path,
    cluster_id: &str,
    bootstrap_host_ip: std::net::Ipv4Addr,
) -> Result<()> {
    if data_dir.join("system/etcd/data/member").exists() {
        bail!("cannot re-arm an empty cluster while local etcd member data exists");
    }
    let path = bootstrap_state_path(data_dir);
    let permit = load_permit(data_dir)?
        .ok_or_else(|| anyhow!("missing etcd bootstrap permit {}", path.display()))?;
    if permit.cluster_id != cluster_id || permit.bootstrap_host_ip != bootstrap_host_ip {
        bail!("bootstrap permit does not match the authenticated empty-cluster consensus");
    }
    persist_json(
        &path,
        &BootstrapPermit {
            cluster_id: cluster_id.to_string(),
            bootstrap_host_ip,
            attempt_id: identity::new_instance_id(),
            state: BootstrapPermitState::Armed,
        },
        false,
    )
}

pub fn prepare_legacy_migration(
    data_dir: &Path,
    cluster_id: &str,
    legacy_member_name: &str,
) -> Result<()> {
    if !data_dir.join("system/etcd/data/member").exists() {
        bail!("legacy etcd member data is absent");
    }
    let path = legacy_migration_path(data_dir);
    if let Some(existing) = read_json::<LegacyMigration>(path.clone())? {
        if existing.cluster_id == cluster_id && existing.legacy_member_name == legacy_member_name {
            return Ok(());
        }
        bail!("legacy cluster migration marker conflicts with this migration");
    }
    persist_json(
        &path,
        &LegacyMigration {
            cluster_id: cluster_id.to_string(),
            legacy_member_name: legacy_member_name.to_string(),
            reconciled: false,
        },
        true,
    )
}

pub fn member_name_for_start(runtime: &ClusterRuntime, data_dir: &Path) -> Result<String> {
    let Some(migration) = read_json::<LegacyMigration>(legacy_migration_path(data_dir))? else {
        return Ok(runtime.member_name());
    };
    if migration.cluster_id != runtime.cluster_id {
        bail!("legacy migration marker belongs to a different cluster id");
    }
    Ok(migration.legacy_member_name)
}

pub async fn reconcile_legacy_migration(
    runtime: &ClusterRuntime,
    data_dir: &Path,
    tls: TlsOptions,
) -> Result<()> {
    let path = legacy_migration_path(data_dir);
    let Some(mut migration) = read_json::<LegacyMigration>(path.clone())? else {
        return Ok(());
    };
    if migration.cluster_id != runtime.cluster_id {
        bail!("legacy migration marker belongs to a different cluster id");
    }
    let endpoint = format!("https://{}:{}", runtime.host_ip, runtime.etcd_client_port);
    let mut delay = Duration::from_millis(250);
    let mut client = loop {
        let options = ConnectOptions::new()
            .with_tls(tls.clone())
            .with_connect_timeout(Duration::from_secs(2))
            .with_timeout(Duration::from_secs(2));
        if let Ok(mut client) = Client::connect([endpoint.as_str()], Some(options)).await
            && client.status().await.is_ok()
        {
            break client;
        }
        if delay > Duration::from_secs(8) {
            bail!("migrated legacy etcd member did not become reachable");
        }
        tokio::time::sleep(delay).await;
        delay *= 2;
    };
    let members = client.member_list().await?;
    if members.members().len() != 1 {
        bail!("legacy cluster enable supports exactly one existing etcd member");
    }
    let member = &members.members()[0];
    if member.name() != migration.legacy_member_name {
        bail!("legacy etcd member name differs from the migration marker");
    }
    let peer_url = runtime.peer_url();
    if member.peer_urls().len() != 1 || member.peer_urls().first() != Some(&peer_url) {
        client.member_update(member.id(), [peer_url]).await?;
    }
    if !migration.reconciled {
        migration.reconciled = true;
        persist_json(&path, &migration, false)?;
    }
    Ok(())
}

pub fn decide(runtime: Option<&ClusterRuntime>, data_dir: &Path) -> Result<BootstrapAction> {
    let Some(runtime) = runtime else {
        return Ok(BootstrapAction::SingleNode);
    };
    if !runtime.role.is_voter() {
        return Ok(BootstrapAction::Worker);
    }
    if data_dir.join("system/etcd/data/member").exists() {
        return match read_json::<ForceNewClusterMarker>(force_new_cluster_path(data_dir))? {
            Some(marker) if marker.cluster_id == runtime.cluster_id => {
                Ok(BootstrapAction::ForceNewCluster)
            }
            Some(_) => bail!("force-new-cluster recovery marker belongs to a different cluster"),
            None => Ok(BootstrapAction::Restart),
        };
    }
    if runtime.is_seed() {
        let permit = load_permit(data_dir)?
            .ok_or_else(|| anyhow!("bootstrap seed was not armed during automatic provisioning"))?;
        if permit.cluster_id == runtime.cluster_id
            && permit.bootstrap_host_ip == runtime.host_ip
            && permit.state == BootstrapPermitState::Armed
        {
            return Ok(BootstrapAction::BootstrapSeed);
        }
    }
    Ok(BootstrapAction::WaitForAdmission)
}

pub fn mark_force_new_cluster(data_dir: &Path, cluster_id: &str) -> Result<()> {
    if !data_dir.join("system/etcd/data/member").exists() {
        bail!("force-new-cluster recovery requires surviving etcd member data");
    }
    let path = force_new_cluster_path(data_dir);
    if let Some(existing) = read_json::<ForceNewClusterMarker>(path.clone())? {
        if existing.cluster_id == cluster_id {
            return Ok(());
        }
        bail!("existing force-new-cluster marker belongs to a different cluster");
    }
    persist_json(
        &path,
        &ForceNewClusterMarker {
            cluster_id: cluster_id.to_string(),
            attempt_id: identity::new_instance_id(),
        },
        true,
    )
}

pub fn force_new_cluster_is_pending(data_dir: &Path, cluster_id: &str) -> Result<bool> {
    match read_json::<ForceNewClusterMarker>(force_new_cluster_path(data_dir))? {
        Some(marker) if marker.cluster_id == cluster_id => Ok(true),
        Some(_) => bail!("force-new-cluster recovery marker belongs to a different cluster"),
        None => Ok(false),
    }
}

pub fn complete_force_new_cluster(data_dir: &Path) -> Result<()> {
    match std::fs::remove_file(force_new_cluster_path(data_dir)) {
        Ok(()) => Ok(()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => Ok(()),
        Err(error) => Err(error.into()),
    }
}

pub fn mark_seed_starting(data_dir: &Path) -> Result<()> {
    transition_permit(
        data_dir,
        BootstrapPermitState::Armed,
        BootstrapPermitState::Starting,
    )
}

pub fn mark_seed_joined(data_dir: &Path) -> Result<()> {
    transition_permit(
        data_dir,
        BootstrapPermitState::Starting,
        BootstrapPermitState::Joined,
    )
}

pub async fn ensure_seed_is_fresh(runtime: &ClusterRuntime, tls: TlsOptions) -> Result<()> {
    for endpoint in runtime.client_endpoints() {
        let options = ConnectOptions::new()
            .with_tls(tls.clone())
            .with_connect_timeout(Duration::from_secs(2))
            .with_timeout(Duration::from_secs(2));
        if let Ok(mut client) = Client::connect([endpoint.as_str()], Some(options)).await
            && client.status().await.is_ok()
        {
            bail!(
                "refusing fresh bootstrap because a configured etcd endpoint is already live at `{endpoint}`"
            );
        }
    }
    Ok(())
}

pub async fn wait_for_admission(
    runtime: &ClusterRuntime,
    data_dir: &Path,
    tls: TlsOptions,
    logger: &Logger,
) -> Result<JoinInfo> {
    let peer_url = runtime.peer_url();
    let member_name = runtime.member_name();
    let mut delay = Duration::from_secs(1);
    loop {
        for endpoint in remote_endpoints(runtime) {
            let connect_options = Some(ConnectOptions::new().with_tls(tls.clone()));
            let Ok(mut client) = Client::connect([endpoint.as_str()], connect_options).await else {
                continue;
            };
            let cluster_meta_ready = client
                .get("/maetro/system/cluster-meta", None)
                .await
                .is_ok_and(|response| !response.kvs().is_empty());
            let maestro_leader_ready = client
                .leader("/maetro/cluster/leader")
                .await
                .is_ok_and(|response| response.kv().is_some());
            let rbac_ready = client
                .get(crate::cluster::auth::ready_key(), None)
                .await
                .is_ok_and(|response| !response.kvs().is_empty());
            if !cluster_meta_ready || !maestro_leader_ready || !rbac_ready {
                continue;
            }
            let Ok(member_list) = client.member_list().await else {
                continue;
            };
            if let Some(member) = member_list
                .members()
                .iter()
                .find(|member| member.peer_urls().iter().any(|url| url == &peer_url))
            {
                if !member.is_learner() || !member.client_urls().is_empty() {
                    logger.emit(
                        "warn",
                        &format!(
                            "local etcd data for member {} is missing; replacing it as a learner",
                            member.id()
                        ),
                    );
                    client.member_remove(member.id()).await?;
                    let response = client
                        .member_add(
                            [peer_url.clone()],
                            Some(MemberAddOptions::new().with_is_learner()),
                        )
                        .await?;
                    let replacement = response.member().ok_or_else(|| {
                        anyhow!("etcd MemberAdd response omitted the replacement learner")
                    })?;
                    let join_info = JoinInfo {
                        cluster_id: runtime.cluster_id.clone(),
                        member_id: replacement.id(),
                        member_name: member_name.clone(),
                        peer_url: peer_url.clone(),
                        initial_cluster: format_initial_cluster(
                            response.member_list(),
                            replacement.id(),
                            &member_name,
                        )?,
                    };
                    replace_join_info(data_dir, &join_info)?;
                    return Ok(join_info);
                }
                let join_info = JoinInfo {
                    cluster_id: runtime.cluster_id.clone(),
                    member_id: member.id(),
                    member_name: member_name.clone(),
                    peer_url: peer_url.clone(),
                    initial_cluster: format_initial_cluster(
                        member_list.members(),
                        member.id(),
                        &member_name,
                    )?,
                };
                replace_join_info(data_dir, &join_info)?;
                return Ok(join_info);
            }
            let added = client
                .member_add(
                    [peer_url.clone()],
                    Some(MemberAddOptions::new().with_is_learner()),
                )
                .await;
            if let Ok(response) = added {
                let member = response
                    .member()
                    .ok_or_else(|| anyhow!("etcd MemberAdd response omitted the new member"))?;
                let join_info = JoinInfo {
                    cluster_id: runtime.cluster_id.clone(),
                    member_id: member.id(),
                    member_name: member_name.clone(),
                    peer_url: peer_url.clone(),
                    initial_cluster: format_initial_cluster(
                        response.member_list(),
                        member.id(),
                        &member_name,
                    )?,
                };
                replace_join_info(data_dir, &join_info)?;
                logger.emit("info", &format!("admitted etcd learner {}", member.id()));
                return Ok(join_info);
            }
        }
        logger.emit(
            "warn",
            &format!(
                "waiting for bootstrap seed to admit `{peer_url}`; retrying in {}s",
                delay.as_secs()
            ),
        );
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(30));
    }
}

pub async fn promote_when_ready(
    runtime: &ClusterRuntime,
    member_id: u64,
    tls: TlsOptions,
    logger: &Logger,
) -> Result<()> {
    let mut delay = Duration::from_secs(1);
    loop {
        for endpoint in remote_endpoints(runtime) {
            let connect_options = Some(ConnectOptions::new().with_tls(tls.clone()));
            let Ok(mut client) = Client::connect([endpoint.as_str()], connect_options).await else {
                continue;
            };
            if let Ok(member_list) = client.member_list().await {
                let member = member_list
                    .members()
                    .iter()
                    .find(|member| member.id() == member_id);
                match member {
                    Some(member) if !member.is_learner() => return Ok(()),
                    None => bail!("etcd learner `{member_id}` disappeared before promotion"),
                    Some(_) => {}
                }
            }
            if client.member_promote(member_id).await.is_ok() {
                logger.emit("info", &format!("promoted etcd learner {member_id}"));
                return Ok(());
            }
        }
        tokio::time::sleep(delay).await;
        delay = (delay * 2).min(Duration::from_secs(10));
    }
}

pub async fn write_cluster_meta(
    runtime: &ClusterRuntime,
    display_name: &str,
    tls: TlsOptions,
) -> Result<()> {
    let endpoint = format!("https://{}:{}", runtime.host_ip, runtime.etcd_client_port);
    let mut client = Client::connect([endpoint], Some(ConnectOptions::new().with_tls(tls))).await?;
    let meta = ClusterMeta {
        cluster_id: runtime.cluster_id.clone(),
        name: display_name.to_string(),
        bootstrap_host_ip: runtime.initial_voters[0].host_ip,
        initial_voter_host_ips: runtime
            .initial_voters
            .iter()
            .map(|node| node.host_ip)
            .collect(),
        initial_voter_endpoints: runtime.initial_voters.clone(),
    };
    let value = serde_json::to_vec(&meta)?;
    let response = client.get("/maetro/system/cluster-meta", None).await?;
    if let Some(existing) = response.kvs().first() {
        let existing: ClusterMeta = serde_json::from_slice(existing.value())?;
        if existing != meta {
            bail!("durable cluster metadata does not match local configuration");
        }
    } else {
        use etcd_client::{Compare, CompareOp, PutOptions, Txn, TxnOp};
        let transaction = Txn::new()
            .when([Compare::version(
                "/maetro/system/cluster-meta",
                CompareOp::Equal,
                0,
            )])
            .and_then([TxnOp::put(
                "/maetro/system/cluster-meta",
                value,
                Some(PutOptions::new()),
            )]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("cluster metadata was initialized concurrently; restart to validate it");
        }
    }
    seed_bootstrap_records(&mut client, runtime).await?;
    Ok(())
}

pub async fn validate_cluster_meta(
    runtime: &ClusterRuntime,
    display_name: &str,
    tls: TlsOptions,
) -> Result<()> {
    let mut client = Client::connect(
        runtime.client_endpoints(),
        Some(ConnectOptions::new().with_tls(tls)),
    )
    .await?;
    let response = client.get("/maetro/system/cluster-meta", None).await?;
    let existing = response
        .kvs()
        .first()
        .ok_or_else(|| anyhow!("cluster metadata has not been initialized"))?;
    let existing: ClusterMeta = serde_json::from_slice(existing.value())?;
    let expected = ClusterMeta {
        cluster_id: runtime.cluster_id.clone(),
        name: display_name.to_string(),
        bootstrap_host_ip: runtime.initial_voters[0].host_ip,
        initial_voter_host_ips: runtime
            .initial_voters
            .iter()
            .map(|node| node.host_ip)
            .collect(),
        initial_voter_endpoints: runtime.initial_voters.clone(),
    };
    if existing != expected {
        bail!("durable cluster metadata does not match local configuration");
    }
    Ok(())
}

pub(crate) fn format_initial_cluster(
    members: &[Member],
    self_id: u64,
    self_name: &str,
) -> Result<String> {
    let mut entries = Vec::new();
    for member in members {
        let peer_url = member
            .peer_urls()
            .first()
            .ok_or_else(|| anyhow!("etcd member `{}` has no peer URL", member.id()))?;
        let name = if member.id() == self_id {
            self_name.to_string()
        } else if !member.name().is_empty() {
            member.name().to_string()
        } else {
            member_name_for_peer(peer_url)?
        };
        entries.push(format!("{name}={peer_url}"));
    }
    entries.sort();
    Ok(entries.join(","))
}

fn member_name_for_peer(peer_url: &str) -> Result<String> {
    let host_ip = peer_ip(peer_url)?;
    let port = peer_url
        .rsplit_once(':')
        .and_then(|(_, port)| port.parse::<u16>().ok())
        .ok_or_else(|| anyhow!("invalid etcd peer URL `{peer_url}`"))?;
    let api_port = port
        .checked_sub(3)
        .ok_or_else(|| anyhow!("etcd peer URL `{peer_url}` cannot map to a node API port"))?;
    Ok(format!(
        "maestro-{}",
        crate::cluster::identity::endpoint_identity_suffix(host_ip, api_port)
    ))
}

fn peer_ip(peer_url: &str) -> Result<std::net::Ipv4Addr> {
    let without_scheme = peer_url
        .strip_prefix("https://")
        .or_else(|| peer_url.strip_prefix("http://"))
        .ok_or_else(|| anyhow!("invalid etcd peer URL `{peer_url}`"))?;
    let host = without_scheme
        .rsplit_once(':')
        .map(|(host, _)| host)
        .ok_or_else(|| anyhow!("invalid etcd peer URL `{peer_url}`"))?;
    host.parse()
        .with_context(|| format!("invalid IPv4 peer URL `{peer_url}`"))
}

pub(super) async fn seed_bootstrap_records(
    client: &mut Client,
    runtime: &ClusterRuntime,
) -> Result<()> {
    for node in &runtime.initial_voters {
        let key = crate::cluster::identity::control_reservation_key(node.host_ip, node.api_port);
        let value = serde_json::json!({
            "hostIp": node.host_ip,
            "apiPort": node.api_port,
            "gatewayPort": node.gateway_port,
            "etcdClientPort": node.etcd_client_port,
            "etcdPeerPort": node.etcd_peer_port,
            "nodeId": null,
            "state": "reserved"
        });
        create_or_validate_reservation(client, &key, &value, "apiPort").await?;
    }
    let key = format!("/maetro/cluster/subnets/{}", runtime.node_id);
    let value = serde_json::json!({
        "cidr": runtime.subnet,
        "nodeId": runtime.node_id,
        "state": "reserved"
    });
    create_or_validate_reservation(client, &key, &value, "cidr").await?;
    let members = client.member_list().await?;
    for member in members
        .members()
        .iter()
        .filter(|member| !member.is_learner())
    {
        let key = format!("/maetro/cluster/voters/{:016x}", member.id());
        let value = serde_json::json!({
            "memberId": member.id(),
            "name": member.name(),
            "peerUrls": member.peer_urls(),
            "clientUrls": member.client_urls()
        });
        create_or_validate_json(client, &key, &value).await?;
    }
    Ok(())
}

async fn create_or_validate_json(
    client: &mut Client,
    key: &str,
    value: &serde_json::Value,
) -> Result<()> {
    use etcd_client::{Compare, CompareOp, Txn, TxnOp};

    let encoded = serde_json::to_vec(value)?;
    let response = client.get(key, None).await?;
    if let Some(existing) = response.kvs().first() {
        let existing: serde_json::Value = serde_json::from_slice(existing.value())?;
        if existing != *value {
            bail!("durable cluster record `{key}` conflicts with local configuration");
        }
    } else {
        let transaction = Txn::new()
            .when([Compare::version(key, CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, encoded, None)]);
        if !client.txn(transaction).await?.succeeded() {
            let response = client.get(key, None).await?;
            let existing = response
                .kvs()
                .first()
                .ok_or_else(|| anyhow!("cluster record `{key}` disappeared"))?;
            let existing: serde_json::Value = serde_json::from_slice(existing.value())?;
            if existing != *value {
                bail!("durable cluster record `{key}` was initialized differently");
            }
        }
    }
    Ok(())
}

async fn create_or_validate_reservation(
    client: &mut Client,
    key: &str,
    value: &serde_json::Value,
    identity_field: &str,
) -> Result<()> {
    use etcd_client::{Compare, CompareOp, Txn, TxnOp};

    let encoded = serde_json::to_vec(value)?;
    let response = client.get(key, None).await?;
    if let Some(existing) = response.kvs().first() {
        let existing: serde_json::Value = serde_json::from_slice(existing.value())?;
        if existing.get(identity_field) != value.get(identity_field) {
            bail!("durable cluster reservation `{key}` conflicts with local configuration");
        }
    } else {
        let transaction = Txn::new()
            .when([Compare::version(key, CompareOp::Equal, 0)])
            .and_then([TxnOp::put(key, encoded, None)]);
        if !client.txn(transaction).await?.succeeded() {
            let response = client.get(key, None).await?;
            let existing = response
                .kvs()
                .first()
                .ok_or_else(|| anyhow!("cluster reservation `{key}` disappeared"))?;
            let existing: serde_json::Value = serde_json::from_slice(existing.value())?;
            if existing.get(identity_field) != value.get(identity_field) {
                bail!("durable cluster reservation `{key}` was initialized differently");
            }
        }
    }
    Ok(())
}

fn remote_endpoints(runtime: &ClusterRuntime) -> Vec<String> {
    runtime
        .voter_endpoints
        .iter()
        .filter(|node| **node != runtime.local_endpoint())
        .map(|node| node.client_url())
        .collect()
}

fn load_permit(data_dir: &Path) -> Result<Option<BootstrapPermit>> {
    read_json(bootstrap_state_path(data_dir))
}

pub(crate) fn persist_join_info(data_dir: &Path, join_info: &JoinInfo) -> Result<()> {
    let path = join_info_path(data_dir);
    if let Some(existing) = read_json::<JoinInfo>(path.clone())? {
        if existing != *join_info {
            bail!("persisted etcd join information conflicts with the live membership");
        }
        Ok(())
    } else {
        persist_json(&path, join_info, true)
    }
}

fn replace_join_info(data_dir: &Path, join_info: &JoinInfo) -> Result<()> {
    let path = join_info_path(data_dir);
    if let Some(existing) = read_json::<JoinInfo>(path.clone())?
        && (existing.cluster_id != join_info.cluster_id
            || existing.member_name != join_info.member_name
            || existing.peer_url != join_info.peer_url)
    {
        bail!("replacement etcd join information conflicts with the local voter identity");
    }
    persist_json(&path, join_info, false)
}

fn transition_permit(
    data_dir: &Path,
    expected: BootstrapPermitState,
    next: BootstrapPermitState,
) -> Result<()> {
    let path = bootstrap_state_path(data_dir);
    let mut permit = load_permit(data_dir)?
        .ok_or_else(|| anyhow!("missing etcd bootstrap permit {}", path.display()))?;
    if permit.state != expected {
        bail!(
            "etcd bootstrap permit is {:?}, expected {:?}",
            permit.state,
            expected
        );
    }
    permit.state = next;
    persist_json(&path, &permit, false)
}

fn force_new_cluster_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd-force-new-cluster.json")
}

fn read_json<T: serde::de::DeserializeOwned>(path: PathBuf) -> Result<Option<T>> {
    match std::fs::read(&path) {
        Ok(bytes) => {
            Ok(Some(serde_json::from_slice(&bytes).with_context(|| {
                format!("failed to parse {}", path.display())
            })?))
        }
        Err(err) if err.kind() == std::io::ErrorKind::NotFound => Ok(None),
        Err(err) => Err(err.into()),
    }
}

fn persist_json(path: &Path, value: &impl Serialize, create_new: bool) -> Result<()> {
    use std::io::Write;

    let directory = path
        .parent()
        .ok_or_else(|| anyhow!("state path has no parent: {}", path.display()))?;
    std::fs::create_dir_all(directory)?;
    let temporary = path.with_extension("tmp");
    let mut options = std::fs::OpenOptions::new();
    options
        .write(true)
        .truncate(true)
        .create(!create_new)
        .create_new(create_new);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(value)?)?;
    file.sync_all()?;
    if create_new && path.exists() {
        let _ = std::fs::remove_file(&temporary);
        bail!("refusing to replace existing {}", path.display());
    }
    std::fs::rename(&temporary, path)?;
    std::fs::File::open(directory)?.sync_all()?;
    Ok(())
}

fn bootstrap_state_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd-bootstrap-state.json")
}

fn join_info_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/etcd-join-info.json")
}

fn legacy_migration_path(data_dir: &Path) -> PathBuf {
    data_dir.join("system/cluster-enable-migration.json")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::NodeRole;

    #[test]
    fn legacy_migration_preserves_the_existing_member_name() {
        let root = std::env::temp_dir().join(format!(
            "maestro-bootstrap-migration-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        std::fs::create_dir_all(root.join("system/etcd/data/member")).unwrap();
        let cluster_id = "0123456789abcdef0123456789abcdef";
        prepare_legacy_migration(&root, cluster_id, "maestro-legacy-abcd").unwrap();
        let runtime = ClusterRuntime {
            cluster_id: cluster_id.to_string(),
            node_id: "node123abcde".to_string(),
            instance_id: "instance".to_string(),
            host_ip: "10.20.0.11".parse().unwrap(),
            role: NodeRole::Voter,
            initial_voters: vec!["10.20.0.11".parse().unwrap()],
            voter_endpoints: vec!["10.20.0.11".parse().unwrap()],
            subnet: "172.22.1.0/24".to_string(),
            control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
            labels: Default::default(),
        };
        assert_eq!(
            member_name_for_start(&runtime, &root).unwrap(),
            "maestro-legacy-abcd"
        );
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn seed_permit_is_single_use() {
        let root = std::env::temp_dir().join(format!(
            "maestro-bootstrap-{}-{}",
            std::process::id(),
            crate::utils::time::current_time_millis().unwrap_or_default()
        ));
        arm_seed(
            &root,
            "0123456789abcdef0123456789abcdef",
            std::net::Ipv4Addr::new(10, 20, 0, 11),
        )
        .expect("arm");
        assert!(
            arm_seed(
                &root,
                "0123456789abcdef0123456789abcdef",
                std::net::Ipv4Addr::new(10, 20, 0, 11)
            )
            .is_err()
        );
        mark_seed_starting(&root).expect("starting");
        assert!(mark_seed_starting(&root).is_err());
        mark_seed_joined(&root).expect("joined");
        let _ = std::fs::remove_dir_all(root);
    }

    #[test]
    fn unnamed_endpoint_members_derive_names_from_peer_ports() {
        assert_eq!(
            member_name_for_peer("https://10.20.0.11:3104").unwrap(),
            "maestro-0a14000b-0c1d"
        );
    }
}
