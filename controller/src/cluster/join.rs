use std::{
    collections::BTreeSet,
    io::Write,
    net::Ipv4Addr,
    path::{Path, PathBuf},
};

use anyhow::{Context, Result, anyhow, bail};
use base64::{Engine as _, engine::general_purpose::STANDARD as BASE64};
use chacha20poly1305::{
    ChaCha20Poly1305, Key, Nonce,
    aead::{Aead, Generate, KeyInit, Payload},
};
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, MemberAddOptions, PutOptions,
    TlsOptions, Txn, TxnOp,
};
use hkdf::Hkdf;
use hmac::{Hmac, Mac};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use x25519_dalek::{EphemeralSecret, PublicKey, StaticSecret};

use crate::{
    cluster::{
        ClusterNodeEndpoint, ClusterRuntime, NodeRole, bootstrap::JoinInfo, types::LeadershipToken,
    },
    utils::certs::{ClusterCa, EtcdCerts},
};

type HmacSha256 = Hmac<Sha256>;

const JOIN_NONCE_PREFIX: &str = "/maetro/cluster/join-nonces/";
const JOIN_INTENT_PREFIX: &str = "/maetro/cluster/join-intents/";
const ADMISSION_PREFIX: &str = "/maetro/cluster/admissions/";
const CA_DISCOVERY_CONTEXT: &[u8] = b"maestro-cluster-ca-discovery-v1";

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaDiscoveryRequest {
    pub cluster_name: String,
    pub nonce: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CaDiscoveryResponse {
    pub cluster_id: String,
    pub ca_pem: String,
    pub proof: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinRequest {
    pub node_id: String,
    pub hostname: String,
    pub role: NodeRole,
    pub cluster_host_ip: Ipv4Addr,
    pub cluster_api_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_api_port: Option<u16>,
    pub cluster_gateway_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
    pub subnet: String,
    pub tailscale_ip: Option<Ipv4Addr>,
    pub joiner_public_key: String,
    pub timestamp_ms: i64,
    pub nonce: String,
}

impl JoinRequest {
    pub fn endpoint(&self) -> ClusterNodeEndpoint {
        ClusterNodeEndpoint {
            host_ip: self.cluster_host_ip,
            api_port: self.cluster_api_port,
            gateway_port: self.cluster_gateway_port,
            etcd_client_port: self.etcd_client_port,
            etcd_peer_port: self.etcd_peer_port,
            identity_api_port: self.identity_api_port,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinEnvelope {
    pub cluster_id: String,
    pub leader_public_key: String,
    pub nonce: String,
    pub ciphertext: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinPayload {
    pub cluster_id: String,
    pub display_name: String,
    pub subnets: Vec<String>,
    pub voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default)]
    pub voter_endpoints: Vec<ClusterNodeEndpoint>,
    pub initial_voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default)]
    pub initial_voter_endpoints: Vec<ClusterNodeEndpoint>,
    pub api_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
    pub certificates: NodeCertificateBundle,
    pub voter_ca: Option<ClusterCaBundle>,
    pub join_info: Option<JoinInfo>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct JoinAdmission {
    pub node_id: String,
    pub role: NodeRole,
    pub cluster_host_ip: Ipv4Addr,
    #[serde(default)]
    pub cluster_api_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub identity_api_port: Option<u16>,
    pub subnet: String,
    pub public_key_sha256: String,
    pub created_at_ms: i64,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
struct JoinIntent {
    node_id: String,
    role: NodeRole,
    cluster_host_ip: Ipv4Addr,
    #[serde(default)]
    cluster_api_port: u16,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    identity_api_port: Option<u16>,
    #[serde(default)]
    etcd_peer_port: u16,
    subnet: String,
    public_key_sha256: String,
    member_id: Option<u64>,
}

#[derive(Clone)]
pub struct JoinCoordinator {
    runtime: ClusterRuntime,
    display_name: String,
    data_dir: PathBuf,
    join_secret: String,
    endpoints: Vec<String>,
    tls: TlsOptions,
}

impl JoinCoordinator {
    pub fn new(
        runtime: ClusterRuntime,
        display_name: String,
        data_dir: PathBuf,
        join_secret: String,
        endpoints: Vec<String>,
        tls: TlsOptions,
    ) -> Self {
        Self {
            runtime,
            display_name,
            data_dir,
            join_secret,
            endpoints,
            tls,
        }
    }

    pub fn discover_ca(&self, request: &CaDiscoveryRequest) -> Result<CaDiscoveryResponse> {
        if request.cluster_name != self.display_name {
            bail!("CA discovery requested a different cluster name");
        }
        let ca_pem = std::fs::read_to_string(self.data_dir.join("system/certs/ca.pem"))?;
        create_ca_discovery_response(
            &self.join_secret,
            &self.display_name,
            &self.runtime.cluster_id,
            &ca_pem,
            request,
        )
    }

    pub async fn approve(&self, token: &LeadershipToken, admission: JoinAdmission) -> Result<()> {
        if !admission.role.is_voter() {
            bail!("only voter joins require an approval record");
        }
        validate_admission(&admission)?;
        let key = format!("{ADMISSION_PREFIX}{}", admission.node_id);
        let value = serde_json::to_vec(&admission)?;
        let mut client = self.connect().await?;
        let response = client.get(key.as_str(), None).await?;
        if let Some(existing) = response.kvs().first() {
            if existing.value() == value {
                return Ok(());
            }
            bail!("a different voter admission already exists for this node id");
        }
        let transaction = Txn::new()
            .when([
                leadership_compare(token),
                Compare::version(key.as_str(), CompareOp::Equal, 0),
            ])
            .and_then([TxnOp::put(key.as_str(), value, None)]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("leadership changed or voter admission was created concurrently");
        }
        Ok(())
    }

    pub async fn join(
        &self,
        token: &LeadershipToken,
        request: JoinRequest,
        signature: &str,
        source_ip: Ipv4Addr,
    ) -> Result<JoinEnvelope> {
        let now_ms = i64::try_from(crate::utils::time::current_time_millis()?)
            .map_err(|_| anyhow!("current time does not fit i64"))?;
        validate_request_shape(&request, now_ms)?;
        verify_request_signature(&self.join_secret, &request, signature)?;
        if request.cluster_host_ip != source_ip {
            bail!("join source address does not match the requested cluster host IP");
        }
        if !self
            .runtime
            .control_allow_cidrs
            .iter()
            .map(|cidr| crate::cluster::network::Ipv4Cidr::parse(cidr))
            .collect::<Result<Vec<_>>>()?
            .iter()
            .any(|cidr| cidr.contains(source_ip))
        {
            bail!("join source address is outside cluster.control-allow-cidrs");
        }
        if request.identity_api_port.is_some() != self.runtime.identity_api_port.is_some() {
            bail!("join request uses a different cluster.nodes identity mode");
        }
        if request.identity_api_port.is_none()
            && (request.cluster_api_port != self.runtime.api_port
                || request.cluster_gateway_port != self.runtime.gateway_port
                || request.etcd_client_port != self.runtime.etcd_client_port
                || request.etcd_peer_port != self.runtime.etcd_peer_port)
        {
            bail!("bare-IP join request uses different shared cluster ports");
        }
        let mut client = self.connect().await?;
        if !client
            .get(format!("/maetro/cluster/removed/{}", request.node_id), None)
            .await?
            .kvs()
            .is_empty()
        {
            bail!("removed node identities cannot be reused");
        }
        let nonce_lease = client.lease_grant(10 * 60, None).await?.id();
        let nonce_key = format!("{JOIN_NONCE_PREFIX}{}", request.nonce);
        let intent_key = format!("{JOIN_INTENT_PREFIX}{}", request.node_id);
        let public_key_sha256 = public_key_sha256(&request.joiner_public_key)?;
        let requested_intent = JoinIntent {
            node_id: request.node_id.clone(),
            role: request.role,
            cluster_host_ip: request.cluster_host_ip,
            cluster_api_port: request.cluster_api_port,
            identity_api_port: request.identity_api_port,
            etcd_peer_port: request.etcd_peer_port,
            subnet: request.subnet.clone(),
            public_key_sha256,
            member_id: None,
        };
        let existing_intent = read_intent(&mut client, &intent_key).await?;
        if let Some(existing) = &existing_intent
            && !same_join_identity(existing, &requested_intent)
        {
            bail!("node id is already associated with a different join identity");
        }

        validate_reservations(&mut client, &request, existing_intent.is_some()).await?;
        let admission =
            if voter_admission_required(&self.runtime, &request, existing_intent.is_some()) {
                Some(read_matching_admission(&mut client, &request, &requested_intent).await?)
            } else {
                None
            };
        let mut comparisons = vec![
            leadership_compare(token),
            Compare::version(nonce_key.as_str(), CompareOp::Equal, 0),
        ];
        let mut operations = vec![TxnOp::put(
            nonce_key.as_str(),
            request.node_id.as_bytes(),
            Some(PutOptions::new().with_lease(nonce_lease)),
        )];
        if existing_intent.is_none() {
            comparisons.push(Compare::version(intent_key.as_str(), CompareOp::Equal, 0));
            operations.push(TxnOp::put(
                intent_key.as_str(),
                serde_json::to_vec(&requested_intent)?,
                None,
            ));
            if let Some((key, value)) = admission {
                comparisons.push(Compare::value(key.as_str(), CompareOp::Equal, value));
                operations.push(TxnOp::delete(key, None));
            }
        } else if let Some(existing_intent) = &existing_intent {
            comparisons.push(Compare::value(
                intent_key.as_str(),
                CompareOp::Equal,
                serde_json::to_vec(existing_intent)?,
            ));
        }
        let transaction = Txn::new().when(comparisons).and_then(operations);
        if !client.txn(transaction).await?.succeeded() {
            bail!("join request was replayed or cluster admission changed");
        }

        reserve_join_resources(&mut client, token, &request).await?;
        crate::cluster::auth::provision_node_users(
            &self.endpoints,
            self.tls.clone(),
            request.cluster_host_ip,
            request.identity_api_port,
            &request.node_id,
            &request.subnet,
            request.role,
        )
        .await?;

        let join_info = if request.role.is_voter() {
            Some(
                self.ensure_voter_member(
                    &mut client,
                    token,
                    &intent_key,
                    existing_intent.unwrap_or(requested_intent),
                    request.endpoint(),
                )
                .await?,
            )
        } else {
            None
        };
        let ca =
            crate::utils::certs::load_cluster_ca(&self.data_dir.join("system/certs/cluster-ca"))?;
        let certificates = crate::utils::certs::generate_cluster_node_certs_for_endpoint(
            &ca,
            request.cluster_host_ip,
            request.identity_api_port,
            request.role,
        )?;
        let meta = read_cluster_meta(&mut client).await?;
        let (voter_endpoints, subnets) = authoritative_topology(&mut client, &self.runtime).await?;
        let voter_host_ips = voter_endpoints
            .iter()
            .map(|node| node.host_ip)
            .collect::<Vec<_>>();
        persist_voter_cache(
            &self.data_dir,
            &ClusterVoterCache {
                cluster_id: meta.cluster_id.clone(),
                voter_host_ips: voter_host_ips.clone(),
                voter_endpoints: voter_endpoints.clone(),
                subnets: subnets.clone(),
                initial_voter_host_ips: meta.initial_voter_host_ips.clone(),
                initial_voter_endpoints: meta.initial_voter_endpoints.clone(),
                api_port: self.runtime.api_port,
                etcd_client_port: self.runtime.etcd_client_port,
                etcd_peer_port: self.runtime.etcd_peer_port,
            },
        )?;
        let payload = JoinPayload {
            cluster_id: meta.cluster_id,
            display_name: self.display_name.clone(),
            subnets,
            voter_host_ips,
            voter_endpoints,
            initial_voter_host_ips: meta.initial_voter_host_ips,
            initial_voter_endpoints: meta.initial_voter_endpoints,
            api_port: self.runtime.api_port,
            etcd_client_port: self.runtime.etcd_client_port,
            etcd_peer_port: self.runtime.etcd_peer_port,
            certificates: NodeCertificateBundle::from(&certificates),
            voter_ca: request.role.is_voter().then(|| ClusterCaBundle::from(&ca)),
            join_info,
        };
        encrypt_response(&self.join_secret, &request, &payload, 200)
    }

    pub async fn remove_node(
        &self,
        token: &LeadershipToken,
        node_id: &str,
    ) -> Result<RemoveNodeOutcome> {
        let mut client = self.connect().await?;
        let record_key = format!("/maetro/cluster/node-records/{node_id}");
        let record_response = client.get(record_key.as_str(), None).await?;
        let record_entry = record_response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("cluster node `{node_id}` is unknown"))?;
        let record: crate::cluster::NodeRecord = serde_json::from_slice(record_entry.value())?;
        let manifest_key = format!("/maetro/cluster/assignments/{node_id}");
        let manifest = client.get(manifest_key.as_str(), None).await?;
        if manifest.kvs().first().is_some_and(|entry| {
            serde_json::from_slice::<crate::cluster::AssignmentManifest>(entry.value())
                .is_ok_and(|manifest| !manifest.assignments.is_empty())
        }) {
            return Ok(RemoveNodeOutcome::Draining);
        }
        if node_id == self.runtime.node_id {
            return Ok(RemoveNodeOutcome::LeadershipTransferRequired);
        }

        let removal_key = format!("/maetro/cluster/removals/{node_id}");
        let removal_value = serde_json::to_vec(&serde_json::json!({
            "nodeId": node_id,
            "hostIp": record.last_info.cluster_host_ip,
            "role": record.last_info.role,
            "requestedAtMs": crate::cluster_stats::now_ms(),
        }))?;
        let existing_removal = client.get(removal_key.as_str(), None).await?;
        if existing_removal.kvs().is_empty() {
            let transaction = Txn::new()
                .when([
                    leadership_compare(token),
                    Compare::version(removal_key.as_str(), CompareOp::Equal, 0),
                ])
                .and_then([TxnOp::put(
                    removal_key.as_str(),
                    removal_value.clone(),
                    None,
                )]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while recording node removal intent");
            }
        }

        let members = client.member_list().await?;
        let peer_url = format!(
            "https://{}:{}",
            record.last_info.cluster_host_ip,
            if self.runtime.identity_api_port.is_none() {
                self.runtime.etcd_peer_port
            } else {
                record
                    .last_info
                    .cluster_api_port
                    .checked_add(3)
                    .ok_or_else(|| anyhow!("node API port cannot map to an etcd peer port"))?
            }
        );
        if let Some(member) = members
            .members()
            .iter()
            .find(|member| member.peer_urls().iter().any(|url| url == &peer_url))
        {
            let voters = members
                .members()
                .iter()
                .filter(|member| !member.is_learner())
                .count();
            if !member.is_learner() && voters <= 1 {
                bail!("refusing to remove the cluster's last voter");
            }
            client.member_remove(member.id()).await?;
        } else if record.last_info.role.is_voter() && existing_removal.kvs().is_empty() {
            bail!("voter node has no matching etcd member");
        }

        let history_key = format!("/maetro/cluster/membership-history/{node_id}");
        let tombstone_key = format!("/maetro/cluster/removed/{node_id}");
        let operations = [
            TxnOp::delete(format!("/maetro/cluster/nodes/{node_id}"), None),
            TxnOp::delete(record_key, None),
            TxnOp::delete(format!("/maetro/cluster/node-state/{node_id}"), None),
            TxnOp::delete(manifest_key, None),
            TxnOp::delete(format!("/maetro/cluster/stats/{node_id}"), None),
            TxnOp::delete(format!("/maetro/cluster/disks/{node_id}"), None),
            TxnOp::delete(
                format!("/maetro/cluster/replica-states/{node_id}/"),
                Some(etcd_client::DeleteOptions::new().with_prefix()),
            ),
            TxnOp::delete(
                control_reservation_key(
                    record.last_info.cluster_host_ip,
                    self.runtime
                        .identity_api_port
                        .map(|_| record.last_info.cluster_api_port),
                ),
                None,
            ),
            TxnOp::delete(subnet_reservation_key(&record.last_info.subnet), None),
            TxnOp::delete(format!("{ADMISSION_PREFIX}{node_id}"), None),
            TxnOp::delete(format!("{JOIN_INTENT_PREFIX}{node_id}"), None),
            TxnOp::put(
                history_key,
                serde_json::to_vec(&serde_json::json!({
                    "nodeId": node_id,
                    "hostIp": record.last_info.cluster_host_ip,
                    "role": record.last_info.role,
                    "removedAtMs": crate::cluster_stats::now_ms(),
                }))?,
                None,
            ),
            TxnOp::put(tombstone_key, removal_value, None),
            TxnOp::delete(removal_key, None),
        ];
        let transaction = Txn::new()
            .when([leadership_compare(token)])
            .and_then(operations);
        if !client.txn(transaction).await?.succeeded() {
            bail!("leadership changed while cleaning removed node state");
        }
        let meta = read_cluster_meta(&mut client).await?;
        let (voter_endpoints, subnets) = authoritative_topology(&mut client, &self.runtime).await?;
        let voter_host_ips = voter_endpoints.iter().map(|node| node.host_ip).collect();
        persist_voter_cache(
            &self.data_dir,
            &ClusterVoterCache {
                cluster_id: meta.cluster_id,
                voter_host_ips,
                voter_endpoints,
                subnets,
                initial_voter_host_ips: meta.initial_voter_host_ips,
                initial_voter_endpoints: meta.initial_voter_endpoints,
                api_port: self.runtime.api_port,
                etcd_client_port: self.runtime.etcd_client_port,
                etcd_peer_port: self.runtime.etcd_peer_port,
            },
        )?;
        Ok(RemoveNodeOutcome::Removed)
    }

    async fn ensure_voter_member(
        &self,
        client: &mut Client,
        token: &LeadershipToken,
        intent_key: &str,
        mut intent: JoinIntent,
        endpoint: ClusterNodeEndpoint,
    ) -> Result<JoinInfo> {
        let peer_url = endpoint.peer_url();
        let member_name = endpoint.member_name();
        let members = client.member_list().await?;
        let member = if let Some(member) = members
            .members()
            .iter()
            .find(|member| member.peer_urls().iter().any(|value| value == &peer_url))
        {
            member.clone()
        } else {
            client
                .member_add(
                    [peer_url.clone()],
                    Some(MemberAddOptions::new().with_is_learner()),
                )
                .await?
                .member()
                .cloned()
                .ok_or_else(|| anyhow!("etcd MemberAdd response omitted the learner"))?
        };
        if intent
            .member_id
            .is_some_and(|member_id| member_id != member.id())
        {
            bail!("membership intent conflicts with the existing etcd member");
        }
        if intent.member_id.is_none() {
            let previous = serde_json::to_vec(&intent)?;
            intent.member_id = Some(member.id());
            let transaction = Txn::new()
                .when([
                    leadership_compare(token),
                    Compare::value(intent_key, CompareOp::Equal, previous),
                ])
                .and_then([TxnOp::put(intent_key, serde_json::to_vec(&intent)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership changed while recording the learner membership");
            }
        }
        let members = client.member_list().await?;
        Ok(JoinInfo {
            cluster_id: self.runtime.cluster_id.clone(),
            member_id: member.id(),
            member_name: member_name.clone(),
            peer_url,
            initial_cluster: crate::cluster::bootstrap::format_initial_cluster(
                members.members(),
                member.id(),
                &member_name,
                endpoint.identity_api_port.is_some(),
            )?,
        })
    }

    async fn connect(&self) -> Result<Client> {
        Ok(Client::connect(
            &self.endpoints,
            Some(ConnectOptions::new().with_tls(self.tls.clone())),
        )
        .await?)
    }
}

fn voter_admission_required(
    runtime: &ClusterRuntime,
    request: &JoinRequest,
    existing_intent: bool,
) -> bool {
    request.role.is_voter()
        && !existing_intent
        && !runtime.initial_voters.contains(&request.endpoint())
}

pub fn create_ca_discovery_response(
    secret: &str,
    cluster_name: &str,
    cluster_id: &str,
    ca_pem: &str,
    request: &CaDiscoveryRequest,
) -> Result<CaDiscoveryResponse> {
    let message = ca_discovery_message(cluster_name, cluster_id, ca_pem, request)?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    Ok(CaDiscoveryResponse {
        cluster_id: cluster_id.to_string(),
        ca_pem: ca_pem.to_string(),
        proof: hex::encode(mac.finalize().into_bytes()),
    })
}

pub fn verify_ca_discovery_response(
    secret: &str,
    cluster_name: &str,
    request: &CaDiscoveryRequest,
    response: &CaDiscoveryResponse,
) -> Result<()> {
    let message = ca_discovery_message(
        cluster_name,
        &response.cluster_id,
        &response.ca_pem,
        request,
    )?;
    let proof = hex::decode(&response.proof).context("invalid CA discovery proof")?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&message);
    mac.verify_slice(&proof)
        .map_err(|_| anyhow!("cluster CA discovery authentication failed"))
}

fn ca_discovery_message(
    cluster_name: &str,
    cluster_id: &str,
    ca_pem: &str,
    request: &CaDiscoveryRequest,
) -> Result<Vec<u8>> {
    if request.cluster_name != cluster_name {
        bail!("CA discovery response belongs to a different cluster name");
    }
    let nonce = decode_32(&request.nonce, "CA discovery nonce")?;
    if cluster_id.len() != 32
        || !cluster_id
            .chars()
            .all(|character| character.is_ascii_hexdigit())
    {
        bail!("invalid cluster id in CA discovery response");
    }
    let ca_sha256 = crate::utils::certs::certificate_fingerprint(ca_pem)?;
    let mut message = Vec::with_capacity(
        CA_DISCOVERY_CONTEXT.len()
            + cluster_name.len()
            + cluster_id.len()
            + nonce.len()
            + ca_sha256.len()
            + 5,
    );
    for field in [
        CA_DISCOVERY_CONTEXT,
        cluster_name.as_bytes(),
        cluster_id.as_bytes(),
        nonce.as_slice(),
        ca_sha256.as_bytes(),
    ] {
        message.extend_from_slice(field);
        message.push(0);
    }
    Ok(message)
}

fn validate_admission(admission: &JoinAdmission) -> Result<()> {
    let key = StaticSecret::random();
    let api_port = admission.cluster_api_port.max(1);
    let request = JoinRequest {
        node_id: admission.node_id.clone(),
        hostname: "admission-validation".to_string(),
        role: admission.role,
        cluster_host_ip: admission.cluster_host_ip,
        cluster_api_port: api_port,
        identity_api_port: admission.identity_api_port,
        cluster_gateway_port: api_port.saturating_add(1),
        etcd_client_port: api_port.saturating_add(2),
        etcd_peer_port: api_port.saturating_add(3),
        subnet: admission.subnet.clone(),
        tailscale_ip: None,
        joiner_public_key: hex::encode(PublicKey::from(&key).as_bytes()),
        timestamp_ms: admission.created_at_ms,
        nonce: "00".repeat(16),
    };
    validate_request_shape(&request, admission.created_at_ms)?;
    decode_32(&admission.public_key_sha256, "public key SHA-256")?;
    Ok(())
}

async fn read_intent(client: &mut Client, key: &str) -> Result<Option<JoinIntent>> {
    client
        .get(key, None)
        .await?
        .kvs()
        .first()
        .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
        .transpose()
}

fn same_join_identity(left: &JoinIntent, right: &JoinIntent) -> bool {
    let same_endpoint_identity = match (left.identity_api_port, right.identity_api_port) {
        (Some(left_port), Some(right_port)) => {
            left_port == right_port
                && left.cluster_api_port == right.cluster_api_port
                && left.etcd_peer_port == right.etcd_peer_port
        }
        (None, None) => true,
        _ => false,
    };
    left.node_id == right.node_id
        && left.role == right.role
        && left.cluster_host_ip == right.cluster_host_ip
        && same_endpoint_identity
        && left.subnet == right.subnet
        && left.public_key_sha256 == right.public_key_sha256
}

async fn read_matching_admission(
    client: &mut Client,
    request: &JoinRequest,
    intent: &JoinIntent,
) -> Result<(String, Vec<u8>)> {
    let key = format!("{ADMISSION_PREFIX}{}", request.node_id);
    let response = client.get(key.as_str(), None).await?;
    let entry = response
        .kvs()
        .first()
        .ok_or_else(|| anyhow!("voter join has no matching one-time admission"))?;
    let admission: JoinAdmission = serde_json::from_slice(entry.value())?;
    if admission.node_id != intent.node_id
        || admission.role != intent.role
        || admission.cluster_host_ip != intent.cluster_host_ip
        || (admission.cluster_api_port != 0
            && admission.cluster_api_port != intent.cluster_api_port)
        || (admission.identity_api_port.is_some()
            && admission.identity_api_port != intent.identity_api_port)
        || admission.subnet != intent.subnet
        || admission.public_key_sha256 != intent.public_key_sha256
    {
        bail!("voter admission does not match the signed join identity");
    }
    Ok((key, entry.value().to_vec()))
}

async fn validate_reservations(
    client: &mut Client,
    request: &JoinRequest,
    retry: bool,
) -> Result<()> {
    let control_key = control_reservation_key(request.cluster_host_ip, request.identity_api_port);
    let control = client.get(control_key.as_str(), None).await?;
    if let Some(existing) = control.kvs().first() {
        let value: serde_json::Value = serde_json::from_slice(existing.value())?;
        if value.get("hostIp").and_then(serde_json::Value::as_str)
            != Some(request.cluster_host_ip.to_string().as_str())
            || (request.identity_api_port.is_some()
                && value.get("apiPort").and_then(serde_json::Value::as_u64)
                    != Some(u64::from(request.cluster_api_port)))
            || value
                .get("nodeId")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|node_id| node_id != request.node_id)
        {
            bail!("cluster host IP is already reserved by another node");
        }
        if !retry
            && value
                .get("nodeId")
                .and_then(serde_json::Value::as_str)
                .is_some()
        {
            bail!("cluster host IP is already claimed");
        }
    }
    let requested_subnet = crate::cluster::network::Ipv4Cidr::parse(&request.subnet)?;
    let reservations = client
        .get(
            "/maetro/cluster/subnets/",
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    for existing in reservations.kvs() {
        let value: serde_json::Value = serde_json::from_slice(existing.value())?;
        let Some(cidr) = value.get("cidr").and_then(serde_json::Value::as_str) else {
            bail!("invalid durable subnet reservation");
        };
        let parsed = crate::cluster::network::Ipv4Cidr::parse(cidr)?;
        if parsed == requested_subnet {
            if value
                .get("nodeId")
                .and_then(serde_json::Value::as_str)
                .is_some_and(|node_id| node_id != request.node_id)
            {
                bail!("Docker subnet is already reserved by another node");
            }
        } else if parsed.overlaps(requested_subnet) {
            bail!("Docker subnet overlaps an existing cluster reservation");
        }
    }
    Ok(())
}

async fn reserve_join_resources(
    client: &mut Client,
    token: &LeadershipToken,
    request: &JoinRequest,
) -> Result<()> {
    let control_reservation = request.identity_api_port.map_or_else(
        || {
            serde_json::json!({
                "hostIp": request.cluster_host_ip,
                "nodeId": request.node_id,
                "state": "reserved"
            })
        },
        |_| {
            serde_json::json!({
                "hostIp": request.cluster_host_ip,
                "apiPort": request.cluster_api_port,
                "gatewayPort": request.cluster_gateway_port,
                "etcdClientPort": request.etcd_client_port,
                "etcdPeerPort": request.etcd_peer_port,
                "nodeId": request.node_id,
                "state": "reserved"
            })
        },
    );
    let reservations = [
        (
            control_reservation_key(request.cluster_host_ip, request.identity_api_port),
            control_reservation,
        ),
        (
            subnet_reservation_key(&request.subnet),
            serde_json::json!({
                "cidr": request.subnet,
                "nodeId": request.node_id,
                "state": "reserved"
            }),
        ),
    ];
    for (key, value) in reservations {
        let response = client.get(key.as_str(), None).await?;
        if let Some(existing) = response.kvs().first() {
            let existing_value: serde_json::Value = serde_json::from_slice(existing.value())?;
            let expected_identity = if key.contains("control-addresses") {
                if request.identity_api_port.is_some() {
                    value.get("apiPort")
                } else {
                    value.get("hostIp")
                }
            } else {
                value.get("cidr")
            };
            let actual_identity = if key.contains("control-addresses") {
                if request.identity_api_port.is_some() {
                    existing_value.get("apiPort")
                } else {
                    existing_value.get("hostIp")
                }
            } else {
                existing_value.get("cidr")
            };
            if expected_identity != actual_identity
                || existing_value
                    .get("nodeId")
                    .and_then(serde_json::Value::as_str)
                    .is_some_and(|node_id| node_id != request.node_id)
            {
                bail!("cluster resource reservation changed during join");
            }
            if existing_value
                .get("nodeId")
                .and_then(serde_json::Value::as_str)
                == Some(request.node_id.as_str())
            {
                continue;
            }
            let transaction = Txn::new()
                .when([
                    leadership_compare(token),
                    Compare::value(key.as_str(), CompareOp::Equal, existing.value()),
                ])
                .and_then([TxnOp::put(key.as_str(), serde_json::to_vec(&value)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership or reservation changed during join");
            }
        } else {
            let transaction = Txn::new()
                .when([
                    leadership_compare(token),
                    Compare::version(key.as_str(), CompareOp::Equal, 0),
                ])
                .and_then([TxnOp::put(key.as_str(), serde_json::to_vec(&value)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership or reservation changed during join");
            }
        }
    }
    Ok(())
}

async fn read_cluster_meta(client: &mut Client) -> Result<crate::cluster::ClusterMeta> {
    let response = client.get("/maetro/system/cluster-meta", None).await?;
    let entry = response
        .kvs()
        .first()
        .ok_or_else(|| anyhow!("cluster metadata is unavailable"))?;
    Ok(serde_json::from_slice(entry.value())?)
}

async fn authoritative_topology(
    client: &mut Client,
    runtime: &ClusterRuntime,
) -> Result<(Vec<ClusterNodeEndpoint>, Vec<String>)> {
    let members = client.member_list().await?;
    let mut voter_endpoints = BTreeSet::new();
    for member in members.members() {
        for peer_url in member.peer_urls() {
            let (host_ip, peer_port) = peer_address(peer_url)?;
            let configured = runtime
                .initial_voters
                .iter()
                .find(|node| node.host_ip == host_ip && node.etcd_peer_port == peer_port)
                .copied();
            let endpoint = if let Some(configured) = configured {
                configured
            } else if runtime.identity_api_port.is_some() {
                let api_port = peer_port.checked_sub(3).ok_or_else(|| {
                    anyhow!("etcd peer port `{peer_port}` cannot map to a node API port")
                })?;
                ClusterNodeEndpoint {
                    host_ip,
                    api_port,
                    gateway_port: api_port + 1,
                    etcd_client_port: api_port + 2,
                    etcd_peer_port: peer_port,
                    identity_api_port: Some(api_port),
                }
            } else {
                ClusterNodeEndpoint {
                    host_ip,
                    api_port: runtime.api_port,
                    gateway_port: runtime.gateway_port,
                    etcd_client_port: runtime.etcd_client_port,
                    etcd_peer_port: peer_port,
                    identity_api_port: None,
                }
            };
            voter_endpoints.insert(endpoint);
        }
    }
    let response = client
        .get(
            "/maetro/cluster/subnets/",
            Some(GetOptions::new().with_prefix()),
        )
        .await?;
    let mut subnets = BTreeSet::new();
    for entry in response.kvs() {
        let value: serde_json::Value = serde_json::from_slice(entry.value())?;
        if let Some(cidr) = value.get("cidr").and_then(serde_json::Value::as_str) {
            subnets.insert(cidr.to_string());
        }
    }
    Ok((
        voter_endpoints.into_iter().collect(),
        subnets.into_iter().collect(),
    ))
}

fn peer_address(peer_url: &str) -> Result<(Ipv4Addr, u16)> {
    let address = peer_url
        .strip_prefix("https://")
        .or_else(|| peer_url.strip_prefix("http://"))
        .ok_or_else(|| anyhow!("invalid etcd peer URL"))?;
    let (host, port) = address
        .rsplit_once(':')
        .ok_or_else(|| anyhow!("invalid etcd peer URL"))?;
    Ok((
        host.parse().context("invalid etcd peer IPv4 address")?,
        port.parse().context("invalid etcd peer port")?,
    ))
}

fn control_reservation_key(host_ip: Ipv4Addr, identity_api_port: Option<u16>) -> String {
    let suffix = identity_api_port.map_or_else(
        || format!("{:08x}", u32::from(host_ip)),
        |port| format!("{:08x}-{port:04x}", u32::from(host_ip)),
    );
    format!("/maetro/cluster/control-addresses/{suffix}")
}

fn subnet_reservation_key(subnet: &str) -> String {
    format!(
        "/maetro/cluster/subnets/{}",
        subnet.replace('.', "-").replace('/', "_")
    )
}

fn leadership_compare(token: &LeadershipToken) -> Compare {
    Compare::create_revision(
        token.election_key.clone(),
        CompareOp::Equal,
        token.create_revision,
    )
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCertificateBundle {
    pub ca_pem: String,
    pub server_cert_pem: String,
    pub server_key_pem: String,
    pub peer_cert_pem: String,
    pub peer_key_pem: String,
    pub client_cert_pem: String,
    pub client_key_pem: String,
    pub probe_client_cert_pem: String,
    pub probe_client_key_pem: String,
    pub traefik_client_cert_pem: String,
    pub traefik_client_key_pem: String,
    pub api_cert_pem: String,
    pub api_key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterCaBundle {
    pub cert_pem: String,
    pub key_pem: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct ClusterVoterCache {
    pub cluster_id: String,
    pub voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default)]
    pub voter_endpoints: Vec<ClusterNodeEndpoint>,
    pub subnets: Vec<String>,
    pub initial_voter_host_ips: Vec<Ipv4Addr>,
    #[serde(default)]
    pub initial_voter_endpoints: Vec<ClusterNodeEndpoint>,
    pub api_port: u16,
    pub etcd_client_port: u16,
    pub etcd_peer_port: u16,
}

impl ClusterVoterCache {
    pub fn client_endpoints(&self) -> Vec<String> {
        if !self.voter_endpoints.is_empty() {
            return self
                .voter_endpoints
                .iter()
                .copied()
                .map(ClusterNodeEndpoint::client_url)
                .collect();
        }
        self.voter_host_ips
            .iter()
            .map(|host_ip| format!("https://{host_ip}:{}", self.etcd_client_port))
            .collect()
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum RemoveNodeOutcome {
    Draining,
    LeadershipTransferRequired,
    Removed,
}

impl From<&EtcdCerts> for NodeCertificateBundle {
    fn from(value: &EtcdCerts) -> Self {
        Self {
            ca_pem: value.ca_pem.clone(),
            server_cert_pem: value.server_cert_pem.clone(),
            server_key_pem: value.server_key_pem.clone(),
            peer_cert_pem: value.peer_cert_pem.clone(),
            peer_key_pem: value.peer_key_pem.clone(),
            client_cert_pem: value.client_cert_pem.clone(),
            client_key_pem: value.client_key_pem.clone(),
            probe_client_cert_pem: value.probe_client_cert_pem.clone(),
            probe_client_key_pem: value.probe_client_key_pem.clone(),
            traefik_client_cert_pem: value.traefik_client_cert_pem.clone(),
            traefik_client_key_pem: value.traefik_client_key_pem.clone(),
            api_cert_pem: value.api_cert_pem.clone(),
            api_key_pem: value.api_key_pem.clone(),
        }
    }
}

impl From<NodeCertificateBundle> for EtcdCerts {
    fn from(value: NodeCertificateBundle) -> Self {
        Self {
            ca_pem: value.ca_pem,
            server_cert_pem: value.server_cert_pem,
            server_key_pem: value.server_key_pem,
            peer_cert_pem: value.peer_cert_pem,
            peer_key_pem: value.peer_key_pem,
            client_cert_pem: value.client_cert_pem,
            client_key_pem: value.client_key_pem,
            probe_client_cert_pem: value.probe_client_cert_pem,
            probe_client_key_pem: value.probe_client_key_pem,
            traefik_client_cert_pem: value.traefik_client_cert_pem,
            traefik_client_key_pem: value.traefik_client_key_pem,
            api_cert_pem: value.api_cert_pem,
            api_key_pem: value.api_key_pem,
        }
    }
}

impl From<&ClusterCa> for ClusterCaBundle {
    fn from(value: &ClusterCa) -> Self {
        Self {
            cert_pem: value.cert_pem.clone(),
            key_pem: value.key_pem.clone(),
        }
    }
}

impl From<ClusterCaBundle> for ClusterCa {
    fn from(value: ClusterCaBundle) -> Self {
        Self {
            cert_pem: value.cert_pem,
            key_pem: value.key_pem,
        }
    }
}

pub fn canonical_body(request: &JoinRequest) -> Result<Vec<u8>> {
    serde_json_canonicalizer::to_vec(request).map_err(Into::into)
}

pub fn sign_request(secret: &str, request: &JoinRequest) -> Result<String> {
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&canonical_body(request)?);
    Ok(hex::encode(mac.finalize().into_bytes()))
}

pub fn verify_request_signature(
    secret: &str,
    request: &JoinRequest,
    signature: &str,
) -> Result<()> {
    let signature = hex::decode(signature).context("join signature is not hexadecimal")?;
    let mut mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    mac.update(&canonical_body(request)?);
    mac.verify_slice(&signature)
        .map_err(|_| anyhow!("join signature mismatch"))
}

pub fn public_key_sha256(public_key: &str) -> Result<String> {
    let public = decode_32(public_key, "joiner public key")?;
    Ok(format!("{:x}", Sha256::digest(public)))
}

pub fn create_join_request(
    private_key: &StaticSecret,
    node_id: String,
    hostname: String,
    role: NodeRole,
    endpoint: ClusterNodeEndpoint,
    subnet: String,
    tailscale_ip: Option<Ipv4Addr>,
    timestamp_ms: i64,
) -> JoinRequest {
    let nonce_entropy = StaticSecret::random().to_bytes();
    JoinRequest {
        node_id,
        hostname,
        role,
        cluster_host_ip: endpoint.host_ip,
        cluster_api_port: endpoint.api_port,
        identity_api_port: endpoint.identity_api_port,
        cluster_gateway_port: endpoint.gateway_port,
        etcd_client_port: endpoint.etcd_client_port,
        etcd_peer_port: endpoint.etcd_peer_port,
        subnet,
        tailscale_ip,
        joiner_public_key: hex::encode(PublicKey::from(private_key).as_bytes()),
        timestamp_ms,
        nonce: hex::encode(&nonce_entropy[..16]),
    }
}

pub fn validate_request_shape(request: &JoinRequest, now_ms: i64) -> Result<()> {
    if request.node_id.len() != 12
        || !request
            .node_id
            .chars()
            .all(|character| character.is_ascii_lowercase() || character.is_ascii_digit())
    {
        bail!("invalid node id");
    }
    if request.hostname.trim().is_empty() || request.hostname.len() > 253 {
        bail!("invalid hostname");
    }
    if !request.cluster_host_ip.is_private()
        || request.cluster_host_ip.is_loopback()
        || request.cluster_host_ip.is_unspecified()
    {
        bail!("invalid private cluster host IP");
    }
    let ports = [
        request.cluster_api_port,
        request.cluster_gateway_port,
        request.etcd_client_port,
        request.etcd_peer_port,
    ];
    if ports.contains(&0) || ports.iter().copied().collect::<BTreeSet<_>>().len() != ports.len() {
        bail!("cluster API, gateway, and etcd ports must be non-zero and distinct");
    }
    if request
        .identity_api_port
        .is_some_and(|port| port != request.cluster_api_port)
    {
        bail!("join endpoint identity does not match the cluster API port");
    }
    if request.identity_api_port.is_some()
        && (request.cluster_gateway_port != request.cluster_api_port.saturating_add(1)
            || request.etcd_client_port != request.cluster_api_port.saturating_add(2)
            || request.etcd_peer_port != request.cluster_api_port.saturating_add(3))
    {
        bail!("IP:port nodes must map gateway and etcd to API port +1, +2, and +3");
    }
    let subnet = crate::cluster::network::Ipv4Cidr::parse(&request.subnet)?;
    if subnet.prefix() != 24 || !subnet.is_private() {
        bail!("invalid private Docker /24");
    }
    decode_32(&request.joiner_public_key, "joiner public key")?;
    let nonce = hex::decode(&request.nonce)?;
    if nonce.len() != 16 {
        bail!("join nonce must contain 16 bytes");
    }
    if now_ms.abs_diff(request.timestamp_ms) > 5 * 60 * 1000 {
        bail!("join timestamp is outside the allowed window");
    }
    Ok(())
}

pub fn encrypt_response(
    secret: &str,
    request: &JoinRequest,
    payload: &JoinPayload,
    status: u16,
) -> Result<JoinEnvelope> {
    let joiner_public =
        PublicKey::from(decode_32(&request.joiner_public_key, "joiner public key")?);
    let leader_secret = EphemeralSecret::random();
    let leader_public = PublicKey::from(&leader_secret);
    let shared = leader_secret.diffie_hellman(&joiner_public);
    let key = derive_response_key(secret, request, &payload.cluster_id, shared.as_bytes())?;
    let key = Key::from(key);
    let cipher = ChaCha20Poly1305::new(&key);
    let nonce = Nonce::generate();
    let plaintext = serde_json::to_vec(payload)?;
    let aad = response_aad(request, status);
    let ciphertext = cipher
        .encrypt(
            &nonce,
            Payload {
                msg: &plaintext,
                aad: &aad,
            },
        )
        .map_err(|_| anyhow!("failed to encrypt join response"))?;
    Ok(JoinEnvelope {
        cluster_id: payload.cluster_id.clone(),
        leader_public_key: hex::encode(leader_public.as_bytes()),
        nonce: BASE64.encode(nonce.as_slice()),
        ciphertext: BASE64.encode(ciphertext),
    })
}

pub fn decrypt_response(
    secret: &str,
    private_key: &StaticSecret,
    request: &JoinRequest,
    envelope: &JoinEnvelope,
    status: u16,
) -> Result<JoinPayload> {
    let leader_public =
        PublicKey::from(decode_32(&envelope.leader_public_key, "leader public key")?);
    let shared = private_key.diffie_hellman(&leader_public);
    let key = derive_response_key(secret, request, &envelope.cluster_id, shared.as_bytes())?;
    let nonce = BASE64.decode(&envelope.nonce)?;
    if nonce.len() != 12 {
        bail!("invalid join response nonce");
    }
    let ciphertext = BASE64.decode(&envelope.ciphertext)?;
    let key = Key::from(key);
    let cipher = ChaCha20Poly1305::new(&key);
    let nonce = Nonce::from(<[u8; 12]>::try_from(nonce.as_slice()).expect("length checked"));
    let plaintext = cipher
        .decrypt(
            &nonce,
            Payload {
                msg: &ciphertext,
                aad: &response_aad(request, status),
            },
        )
        .map_err(|_| anyhow!("join response authentication failed"))?;
    serde_json::from_slice(&plaintext).context("invalid decrypted join response")
}

pub fn load_or_create_join_key(data_dir: &Path) -> Result<StaticSecret> {
    let directory = data_dir.join("system");
    let path = directory.join("join-key");
    if let Ok(encoded) = std::fs::read_to_string(&path) {
        return Ok(StaticSecret::from(decode_32(
            encoded.trim(),
            "persisted join private key",
        )?));
    }
    std::fs::create_dir_all(&directory)?;
    let private_key = StaticSecret::random();
    let temporary = directory.join("join-key.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.create_new(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(hex::encode(private_key.to_bytes()).as_bytes())?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(&directory)?.sync_all()?;
    Ok(private_key)
}

pub fn join_key_fingerprint(private_key: &StaticSecret) -> String {
    format!(
        "{:x}",
        Sha256::digest(PublicKey::from(private_key).as_bytes())
    )
}

pub fn load_voter_cache(data_dir: &Path, cluster_id: &str) -> Result<Option<ClusterVoterCache>> {
    let path = data_dir.join("system/cluster-voters.json");
    let bytes = match std::fs::read(&path) {
        Ok(bytes) => bytes,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
        Err(error) => return Err(error.into()),
    };
    let cache: ClusterVoterCache = serde_json::from_slice(&bytes)?;
    if cache.cluster_id != cluster_id {
        bail!("cached cluster voter set belongs to a different cluster id");
    }
    if (cache.voter_host_ips.is_empty() && cache.voter_endpoints.is_empty())
        || (cache.initial_voter_host_ips.is_empty() && cache.initial_voter_endpoints.is_empty())
    {
        bail!("cached cluster voter set is empty");
    }
    Ok(Some(cache))
}

pub fn persist_voter_cache(data_dir: &Path, cache: &ClusterVoterCache) -> Result<()> {
    let directory = data_dir.join("system");
    std::fs::create_dir_all(&directory)?;
    let path = directory.join("cluster-voters.json");
    let temporary = directory.join("cluster-voters.json.tmp");
    let mut options = std::fs::OpenOptions::new();
    options.create(true).truncate(true).write(true);
    #[cfg(unix)]
    {
        use std::os::unix::fs::OpenOptionsExt;
        options.mode(0o600);
    }
    let mut file = options.open(&temporary)?;
    file.write_all(&serde_json::to_vec_pretty(cache)?)?;
    file.sync_all()?;
    std::fs::rename(&temporary, &path)?;
    std::fs::File::open(&directory)?.sync_all()?;
    Ok(())
}

pub async fn run_voter_cache_sync(
    data_dir: PathBuf,
    runtime: ClusterRuntime,
    endpoints: Vec<String>,
    tls: TlsOptions,
    mut shutdown: tokio::sync::broadcast::Receiver<crate::signal::ShutdownEvent>,
    logger: crate::logs::Logger,
) {
    let mut interval = tokio::time::interval(std::time::Duration::from_secs(30));
    interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            _ = shutdown.recv() => break,
            _ = interval.tick() => {}
        }
        let result = async {
            let mut client = Client::connect(
                &endpoints,
                Some(ConnectOptions::new().with_tls(tls.clone())),
            )
            .await?;
            let meta = read_cluster_meta(&mut client).await?;
            if meta.cluster_id != runtime.cluster_id {
                bail!("live cluster id differs from the local identity");
            }
            let (voter_endpoints, subnets) = authoritative_topology(&mut client, &runtime).await?;
            let voter_host_ips = voter_endpoints.iter().map(|node| node.host_ip).collect();
            persist_voter_cache(
                &data_dir,
                &ClusterVoterCache {
                    cluster_id: meta.cluster_id,
                    voter_host_ips,
                    voter_endpoints,
                    subnets,
                    initial_voter_host_ips: meta.initial_voter_host_ips,
                    initial_voter_endpoints: meta.initial_voter_endpoints,
                    api_port: runtime.api_port,
                    etcd_client_port: runtime.etcd_client_port,
                    etcd_peer_port: runtime.etcd_peer_port,
                },
            )
        }
        .await;
        if let Err(error) = result {
            logger.emit(
                "warn",
                &format!("failed to refresh authoritative voter cache: {error}"),
            );
        }
    }
}

fn derive_response_key(
    secret: &str,
    request: &JoinRequest,
    cluster_id: &str,
    shared_secret: &[u8; 32],
) -> Result<[u8; 32]> {
    let request_nonce = hex::decode(&request.nonce)?;
    let mut nonce_mac = <HmacSha256 as Mac>::new_from_slice(secret.as_bytes())?;
    nonce_mac.update(&request_nonce);
    let nonce_proof = nonce_mac.finalize().into_bytes();
    let mut ikm = Vec::with_capacity(shared_secret.len() + nonce_proof.len());
    ikm.extend_from_slice(shared_secret);
    ikm.extend_from_slice(&nonce_proof);
    let hkdf = Hkdf::<Sha256>::new(Some(cluster_id.as_bytes()), &ikm);
    let mut key = [0_u8; 32];
    hkdf.expand(b"maestro-join-response-v1", &mut key)
        .map_err(|_| anyhow!("failed to derive join response key"))?;
    Ok(key)
}

fn response_aad(request: &JoinRequest, status: u16) -> Vec<u8> {
    let mut aad = Vec::new();
    let role = request.role.to_string();
    let status = status.to_string();
    for value in [
        request.nonce.as_bytes(),
        request.node_id.as_bytes(),
        role.as_bytes(),
        status.as_bytes(),
    ] {
        aad.extend_from_slice(value);
        aad.push(0);
    }
    aad
}

fn decode_32(value: &str, name: &str) -> Result<[u8; 32]> {
    let decoded = hex::decode(value).with_context(|| format!("{name} is not hexadecimal"))?;
    decoded
        .try_into()
        .map_err(|_| anyhow!("{name} must contain exactly 32 bytes"))
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request(key: &StaticSecret) -> JoinRequest {
        create_join_request(
            key,
            "node123abcde".to_string(),
            "node-a".to_string(),
            NodeRole::Worker,
            "10.20.0.15".parse().unwrap(),
            "172.22.4.0/24".to_string(),
            None,
            1_000,
        )
    }

    fn payload() -> JoinPayload {
        JoinPayload {
            cluster_id: "0123456789abcdef0123456789abcdef".to_string(),
            display_name: "test".to_string(),
            subnets: vec!["172.22.4.0/24".to_string()],
            voter_host_ips: vec!["10.20.0.11".parse().unwrap()],
            voter_endpoints: Vec::new(),
            initial_voter_host_ips: vec!["10.20.0.11".parse().unwrap()],
            initial_voter_endpoints: Vec::new(),
            api_port: 3001,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
            certificates: NodeCertificateBundle {
                ca_pem: "ca".to_string(),
                server_cert_pem: "server".to_string(),
                server_key_pem: "server-key".to_string(),
                peer_cert_pem: "peer".to_string(),
                peer_key_pem: "peer-key".to_string(),
                client_cert_pem: "client".to_string(),
                client_key_pem: "client-key".to_string(),
                probe_client_cert_pem: "probe".to_string(),
                probe_client_key_pem: "probe-key".to_string(),
                traefik_client_cert_pem: "traefik".to_string(),
                traefik_client_key_pem: "traefik-key".to_string(),
                api_cert_pem: "api".to_string(),
                api_key_pem: "api-key".to_string(),
            },
            voter_ca: None,
            join_info: None,
        }
    }

    #[test]
    fn signature_uses_canonical_request_body() {
        let key = StaticSecret::random();
        let request = request(&key);
        let signature = sign_request("a sufficiently long shared join secret", &request).unwrap();
        verify_request_signature(
            "a sufficiently long shared join secret",
            &request,
            &signature,
        )
        .unwrap();
        assert!(
            verify_request_signature("another shared join secret value", &request, &signature)
                .is_err()
        );
    }

    #[test]
    fn shared_secret_authenticates_ca_discovery() {
        let ca = crate::utils::certs::generate_cluster_ca().expect("generate CA");
        let request = CaDiscoveryRequest {
            cluster_name: "test".to_string(),
            nonce: "ab".repeat(32),
        };
        let secret = "a sufficiently long shared join secret";
        let response = create_ca_discovery_response(
            secret,
            "test",
            "0123456789abcdef0123456789abcdef",
            &ca.cert_pem,
            &request,
        )
        .expect("create authenticated discovery response");
        verify_ca_discovery_response(secret, "test", &request, &response)
            .expect("authenticate discovered CA");

        assert!(
            verify_ca_discovery_response(
                "a different sufficiently long secret",
                "test",
                &request,
                &response,
            )
            .is_err()
        );
        let mut tampered_ca = response.clone();
        tampered_ca.ca_pem = crate::utils::certs::generate_cluster_ca()
            .expect("generate different CA")
            .cert_pem;
        assert!(verify_ca_discovery_response(secret, "test", &request, &tampered_ca).is_err());
        let mut tampered_cluster = response;
        tampered_cluster.cluster_id = "f".repeat(32);
        assert!(verify_ca_discovery_response(secret, "test", &request, &tampered_cluster).is_err());
    }

    #[test]
    fn legacy_join_intent_retry_ignores_new_defaulted_port_fields() {
        let mut old = JoinIntent {
            node_id: "node123abcde".to_string(),
            role: NodeRole::Voter,
            cluster_host_ip: "10.20.0.15".parse().unwrap(),
            cluster_api_port: 0,
            identity_api_port: None,
            etcd_peer_port: 0,
            subnet: "172.22.4.0/24".to_string(),
            public_key_sha256: "ab".repeat(32),
            member_id: None,
        };
        let mut current = old.clone();
        current.cluster_api_port = 3001;
        current.etcd_peer_port = 2380;
        assert!(same_join_identity(&old, &current));

        old.identity_api_port = Some(3101);
        current.identity_api_port = Some(3201);
        assert!(!same_join_identity(&old, &current));
    }

    #[test]
    fn endpoint_join_ports_are_fenced_to_the_controller_port() {
        let key = StaticSecret::random();
        let mut request = create_join_request(
            &key,
            "node123abcde".to_string(),
            "node-a".to_string(),
            NodeRole::Voter,
            "10.20.0.15:3101".parse().unwrap(),
            "172.22.4.0/24".to_string(),
            None,
            1_000,
        );
        validate_request_shape(&request, 1_000).expect("valid mapped ports");
        request.etcd_peer_port = 2380;
        assert!(validate_request_shape(&request, 1_000).is_err());
    }

    #[test]
    fn configured_original_voters_do_not_need_manual_admission() {
        let private = StaticSecret::random();
        let mut request = create_join_request(
            &private,
            "node123abcde".to_string(),
            "node-a".to_string(),
            NodeRole::Hybrid,
            "10.20.0.12:3101".parse().unwrap(),
            "172.22.2.0/24".to_string(),
            None,
            1_000,
        );
        let runtime = ClusterRuntime {
            cluster_id: "0123456789abcdef0123456789abcdef".to_string(),
            node_id: "seed123abcde".to_string(),
            instance_id: "instance".to_string(),
            host_ip: "10.20.0.11".parse().unwrap(),
            role: NodeRole::Hybrid,
            initial_voters: vec![
                "10.20.0.11:3001".parse().unwrap(),
                "10.20.0.12:3101".parse().unwrap(),
                "10.20.0.13:3201".parse().unwrap(),
            ],
            subnets: Vec::new(),
            control_allow_cidrs: Vec::new(),
            api_port: 3001,
            gateway_port: 3002,
            etcd_client_port: 3003,
            etcd_peer_port: 3004,
            shared_registry: None,
            labels: Default::default(),
            identity_api_port: Some(3001),
        };
        assert!(!voter_admission_required(&runtime, &request, false));

        request.cluster_api_port = 3301;
        request.identity_api_port = Some(3301);
        request.cluster_gateway_port = 3302;
        request.etcd_client_port = 3303;
        request.etcd_peer_port = 3304;
        assert!(voter_admission_required(&runtime, &request, false));
    }

    #[test]
    fn encrypted_response_is_bound_to_request_and_joiner_key() {
        let private = StaticSecret::random();
        let request = request(&private);
        let payload = payload();
        let secret = "a sufficiently long shared join secret";
        let envelope = encrypt_response(secret, &request, &payload, 200).unwrap();
        assert_eq!(
            decrypt_response(secret, &private, &request, &envelope, 200).unwrap(),
            payload
        );

        let other = StaticSecret::random();
        assert!(decrypt_response(secret, &other, &request, &envelope, 200).is_err());
        assert!(decrypt_response(secret, &private, &request, &envelope, 201).is_err());
    }

    #[test]
    fn voter_cache_is_cluster_id_bound_and_replaceable() {
        let root = std::env::temp_dir().join(format!(
            "maestro-voter-cache-{}",
            crate::utils::nanoid::unique_id(8)
        ));
        let cluster_id = "0123456789abcdef0123456789abcdef";
        let mut cache = ClusterVoterCache {
            cluster_id: cluster_id.to_string(),
            voter_host_ips: vec!["10.20.0.11".parse().unwrap()],
            voter_endpoints: Vec::new(),
            subnets: vec!["172.22.1.0/24".to_string()],
            initial_voter_host_ips: vec!["10.20.0.11".parse().unwrap()],
            initial_voter_endpoints: Vec::new(),
            api_port: 3001,
            etcd_client_port: 2379,
            etcd_peer_port: 2380,
        };
        assert_eq!(cache.client_endpoints(), vec!["https://10.20.0.11:2379"]);
        persist_voter_cache(&root, &cache).unwrap();
        assert_eq!(
            load_voter_cache(&root, cluster_id).unwrap(),
            Some(cache.clone())
        );
        assert!(load_voter_cache(&root, &"f".repeat(32)).is_err());
        cache.voter_host_ips.push("10.20.0.12".parse().unwrap());
        cache.voter_endpoints = vec!["10.20.0.11:3101".parse().unwrap()];
        assert_eq!(cache.client_endpoints(), vec!["https://10.20.0.11:3103"]);
        persist_voter_cache(&root, &cache).unwrap();
        assert_eq!(load_voter_cache(&root, cluster_id).unwrap(), Some(cache));
        let _ = std::fs::remove_dir_all(root);
    }
}
