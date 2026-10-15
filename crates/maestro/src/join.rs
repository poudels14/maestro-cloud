use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};
use std::time::Duration;

use anyhow::Context;
use clap::Args;
use hmac::{Hmac, KeyInit, Mac};
use maestro_agent::{WIREGUARD_PORT, detect_capacity, load_or_generate_key, system_reserve};
use maestro_control::allocate_subnet;
use maestro_core::{
    ClusterConfig, ClusterDiscovery, ClusterSettings, JoinRequest, JoinResponse, NodeRecord,
    NodeRole, NodeState, VoterSecrets, WireGuardInfo,
};
use maestro_proto::pb::Envelope;
use maestro_proto::pb::cluster_client::ClusterClient;
use maestro_proto::{
    CertRole, ClusterCa, NodeCredentials, connect_pinned, connect_unverified, fingerprint_of_pem,
};
use maestro_store::{Consensus, Mutation};
use sha2::Sha256;
use tonic::{Code, Status};

use crate::cluster_service::{decode, encode};
use crate::identity::{NodeIdentity, random_node_id};
use crate::server::{self, InitArgs};

const LEADER_MARKER: &str = "leader=";
const DEFAULT_GRPC_PORT: u16 = 7400;
const GRANT_TTL_SECS: u64 = 300;
const RETRY_INTERVAL: Duration = Duration::from_secs(5);
const JOIN_DEADLINE: Duration = Duration::from_secs(180);
const CA_PROOF_PREFIX: &str = "ca-";

#[derive(Args)]
pub struct JoinArgs {
    #[arg(long)]
    config: String,
    #[arg(long)]
    advertise: Option<String>,
    #[arg(long, value_parser = parse_role, default_value = "voter")]
    role: NodeRole,
    #[arg(long, default_value = "/var/lib/maestro")]
    data_dir: PathBuf,
    #[arg(long)]
    name: Option<String>,
}

pub struct JoinAuthority {
    ca: ClusterCa,
    secret: String,
}

impl JoinAuthority {
    pub fn load(data_dir: &Path, secret: &str) -> anyhow::Result<Self> {
        Ok(Self {
            ca: ClusterCa::load(&data_dir.join("pki"))?,
            secret: secret.to_string(),
        })
    }

    pub fn discovery(&self) -> Result<ClusterDiscovery, Status> {
        let fingerprint = self.ca.fingerprint().map_err(internal)?;
        Ok(ClusterDiscovery {
            ca_pem: self.ca.cert_pem().to_string(),
            proof: ca_proof(&self.secret, &fingerprint),
        })
    }

    pub async fn admit(
        &self,
        consensus: &Consensus,
        cluster: &ClusterSettings,
        request: JoinRequest,
    ) -> Result<JoinResponse, Status> {
        let leader = consensus.current_leader().map(|(_, address)| address);
        let authorized = verify_grant(&self.secret, &request.secret, request.role, unix_now());
        match (consensus.is_leader(), authorized) {
            (false, _) => Err(Status::failed_precondition(format!(
                "not the leader; {LEADER_MARKER}{}",
                leader.unwrap_or_default()
            ))),
            (true, Err(problem)) => Err(Status::permission_denied(problem)),
            (true, Ok(())) => self.register(consensus, cluster, request).await,
        }
    }

    async fn register(
        &self,
        consensus: &Consensus,
        cluster: &ClusterSettings,
        request: JoinRequest,
    ) -> Result<JoinResponse, Status> {
        let nodes = consensus.state().nodes().map_err(internal)?;
        let used = nodes
            .iter()
            .filter_map(|node| node.subnet.clone())
            .collect::<Vec<_>>();
        let subnet = allocate_subnet(&cluster.cidr, &used).ok_or_else(|| {
            Status::resource_exhausted("no free node subnets in the cluster CIDR")
        })?;
        let replaced = nodes
            .iter()
            .filter(|node| node.endpoint == request.advertise && node.state != NodeState::Removed)
            .cloned()
            .collect::<Vec<_>>();
        let node_id = std::iter::repeat_with(random_node_id)
            .find(|candidate| nodes.iter().all(|node| node.id != *candidate))
            .unwrap_or_else(random_node_id);
        let mut retirements = Vec::new();
        for previous in replaced {
            let metrics = consensus.metrics();
            if metrics.voters.contains_key(&previous.id)
                || metrics.learners.contains_key(&previous.id)
            {
                consensus
                    .remove_node(previous.id)
                    .await
                    .map_err(|error| Status::unavailable(error.to_string()))?;
            }
            let has_assignments = !consensus
                .state()
                .assignments(previous.id)
                .map_err(internal)?
                .is_empty();
            let state = match has_assignments {
                true => NodeState::Draining,
                false => NodeState::Removed,
            };
            tracing::info!(node = %previous.name, endpoint = %previous.endpoint, "a new node took over this address; retiring the old one");
            retirements.push(Mutation::PutNode {
                node: NodeRecord { state, ..previous },
            });
        }
        let record = NodeRecord {
            id: node_id,
            name: match request.name.trim().is_empty() {
                true => node_id.short(),
                false => request.name.clone(),
            },
            role: request.role,
            endpoint: request.advertise.clone(),
            subnet: Some(subnet.clone()),
            runs_workloads: true,
            capacity: request.capacity,
            reserved: request.reserved,
            labels: request.labels.clone(),
            state: NodeState::Active,
            wireguard: request.wireguard.clone(),
        };
        retirements.push(Mutation::PutNode { node: record });
        consensus.write(retirements).await.map_err(internal)?;
        let addresses = advertise_ip(&request.advertise)
            .into_iter()
            .collect::<Vec<_>>();
        let credentials = self
            .ca
            .issue_node(node_id.0, cert_role(request.role), &addresses)
            .map_err(internal)?;
        let voter_secrets = match request.role {
            NodeRole::Voter => Some(VoterSecrets {
                ca_key_pem: self.ca.key_pem().to_string(),
            }),
            NodeRole::Worker => None,
        };
        let voters = consensus.metrics().voters.into_values().collect();
        Ok(JoinResponse {
            node_id,
            subnet,
            cluster: cluster.clone(),
            voters,
            ca_pem: credentials.ca_pem,
            cert_pem: credentials.cert_pem,
            key_pem: credentials.key_pem,
            voter_secrets,
        })
    }
}

pub async fn provision(args: JoinArgs) -> anyhow::Result<()> {
    match NodeIdentity::exists(&args.data_dir) {
        true => Ok(()),
        false => {
            let config = load_config(&args.config)?;
            let secret = config.join_secret()?.to_string();
            let voters = voter_addresses(&config)?;
            let advertise = resolve_advertise(args.advertise.as_deref(), &voters)?;
            let own_address = grpc_address(&advertise);
            let founder = args.role == NodeRole::Voter && voters.first() == Some(&own_address);
            let founded = founder && !cluster_exists(&voters, &own_address, &secret).await;
            match founded {
                true => {
                    tracing::info!("no listed voter has a cluster yet; creating it here");
                    server::init(InitArgs::new(
                        args.data_dir.clone(),
                        advertise,
                        args.config.clone(),
                    ))
                    .await
                }
                false => join_until_admitted(&args, &advertise, &config, &secret, &voters).await,
            }
        }
    }
}

pub async fn join(args: JoinArgs) -> anyhow::Result<()> {
    let config = load_config(&args.config)?;
    let secret = config.join_secret()?.to_string();
    let voters = voter_addresses(&config)?;
    let advertise = resolve_advertise(args.advertise.as_deref(), &voters)?;
    join_until_admitted(&args, &advertise, &config, &secret, &voters).await
}

async fn join_until_admitted(
    args: &JoinArgs,
    advertise: &str,
    config: &ClusterConfig,
    secret: &str,
    voters: &[String],
) -> anyhow::Result<()> {
    if args.role == NodeRole::Voter {
        config.encryption_key()?;
        config.jwt_secret_key()?;
    }
    let own_address = grpc_address(advertise);
    let candidates = voters
        .iter()
        .filter(|voter| **voter != own_address)
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !candidates.is_empty(),
        "cluster.voters lists no voter other than this node to join through"
    );
    let capacity = detect_capacity()?;
    let wireguard_key = load_or_generate_key(&args.data_dir.join("keys")).await?;
    let wireguard_endpoint = advertise_ip(advertise)
        .map(|ip| SocketAddr::new(ip, WIREGUARD_PORT))
        .context("--advertise must be an IP:port the other nodes can reach")?;
    let mut request = JoinRequest {
        secret: String::new(),
        name: args.name.clone().unwrap_or_default(),
        role: args.role,
        advertise: advertise.to_string(),
        capacity,
        reserved: system_reserve(&capacity, &config.node.reserve),
        labels: config.node.labels.clone(),
        wireguard: Some(WireGuardInfo {
            public_key: wireguard_key.public_key,
            endpoint: wireguard_endpoint,
        }),
    };
    let started = std::time::Instant::now();
    let response = loop {
        request.secret = sign_grant(secret, args.role, unix_now() + GRANT_TTL_SECS);
        let mut failures = Vec::new();
        let mut admitted = None;
        for voter in &candidates {
            match join_through(voter, secret, &request).await {
                Ok(response) => {
                    admitted = Some(response);
                    break;
                }
                Err(error) => failures.push(format!("{voter}: {error:#}")),
            }
        }
        match (admitted, started.elapsed() < JOIN_DEADLINE) {
            (Some(response), _) => break response,
            (None, true) => {
                tracing::warn!(failures = %failures.join("; "), "could not join the cluster yet; retrying");
                tokio::time::sleep(RETRY_INTERVAL).await;
            }
            (None, false) => anyhow::bail!("could not join the cluster: {}", failures.join("; ")),
        }
    };
    persist(args, advertise, response)?;
    println!("joined the cluster as a {}", role_name(args.role));
    Ok(())
}

async fn join_through(
    voter: &str,
    secret: &str,
    request: &JoinRequest,
) -> anyhow::Result<JoinResponse> {
    let fingerprint = discover(voter, secret).await?;
    let response = match request_join(voter, &fingerprint, request).await {
        Err(status) if status.code() == Code::FailedPrecondition => {
            let leader = status
                .message()
                .split_once(LEADER_MARKER)
                .map(|(_, leader)| leader.trim().to_string())
                .filter(|leader| !leader.is_empty())
                .context("the voter is not the leader and did not name one")?;
            request_join(&leader, &fingerprint, request).await?
        }
        other => other?,
    };
    Ok(response)
}

async fn discover(voter: &str, secret: &str) -> anyhow::Result<String> {
    let channel = connect_unverified(voter).await?;
    let reply = ClusterClient::new(channel)
        .discover(Envelope {
            json: b"{}".to_vec(),
        })
        .await?;
    let discovery: ClusterDiscovery = decode(&reply.into_inner())?;
    let fingerprint = fingerprint_of_pem(&discovery.ca_pem)?;
    anyhow::ensure!(
        constant_time_eq(
            discovery.proof.as_bytes(),
            ca_proof(secret, &fingerprint).as_bytes()
        ),
        "{voter} does not know this cluster's join secret"
    );
    Ok(fingerprint)
}

async fn cluster_exists(voters: &[String], own_address: &str, secret: &str) -> bool {
    let checks = voters
        .iter()
        .filter(|voter| *voter != own_address)
        .map(|voter| discover(voter, secret));
    futures_util::future::join_all(checks)
        .await
        .iter()
        .any(Result::is_ok)
}

async fn request_join(
    server: &str,
    fingerprint: &str,
    request: &JoinRequest,
) -> Result<JoinResponse, Status> {
    let channel = connect_pinned(server, fingerprint)
        .await
        .map_err(|error| Status::unavailable(error.to_string()))?;
    let reply = ClusterClient::new(channel).join(encode(request)?).await?;
    decode(&reply.into_inner())
}

fn persist(args: &JoinArgs, advertise: &str, response: JoinResponse) -> anyhow::Result<()> {
    let pki = args.data_dir.join("pki");
    let credentials = NodeCredentials {
        cert_pem: response.cert_pem,
        key_pem: response.key_pem,
        ca_pem: response.ca_pem.clone(),
    };
    credentials.save(&pki.join("node"))?;
    if let Some(secrets) = response.voter_secrets {
        ClusterCa::from_pem(response.ca_pem, secrets.ca_key_pem).save(&pki)?;
    }
    NodeIdentity {
        id: response.node_id,
        name: args
            .name
            .clone()
            .unwrap_or_else(|| response.node_id.short()),
        role: args.role,
        subnet: response.subnet,
        advertise: advertise.to_string(),
        seeds: response.voters,
        cluster: response.cluster,
    }
    .save(&args.data_dir)
}

fn load_config(source: &str) -> anyhow::Result<ClusterConfig> {
    Ok(ClusterConfig::load_with(
        source,
        &crate::secret_sources::read_config_source,
    )?)
}

fn voter_addresses(config: &ClusterConfig) -> anyhow::Result<Vec<String>> {
    let voters = config
        .cluster
        .voters
        .iter()
        .map(|voter| grpc_address(voter))
        .collect::<Vec<_>>();
    anyhow::ensure!(
        !voters.is_empty(),
        "cluster.voters must list the voter addresses in the cluster config"
    );
    Ok(voters)
}

fn resolve_advertise(advertise: Option<&str>, voters: &[String]) -> anyhow::Result<String> {
    match advertise {
        Some(advertise) => Ok(grpc_address(advertise)),
        None => {
            let probe = std::net::UdpSocket::bind("0.0.0.0:0")?;
            probe
                .connect(voters[0].as_str())
                .with_context(|| format!("cannot route to {}; pass --advertise", voters[0]))?;
            let local = probe.local_addr()?.ip();
            tracing::info!(%local, "advertising the address that routes to the voters");
            Ok(format!("{local}:{DEFAULT_GRPC_PORT}"))
        }
    }
}

pub fn grpc_address(endpoint: &str) -> String {
    let endpoint = endpoint.trim();
    match endpoint.contains(':') {
        true => endpoint.to_string(),
        false => format!("{endpoint}:{DEFAULT_GRPC_PORT}"),
    }
}

fn ca_proof(secret: &str, ca_fingerprint: &str) -> String {
    grant_signature(secret, &format!("{CA_PROOF_PREFIX}{ca_fingerprint}"))
}

fn sign_grant(secret: &str, role: NodeRole, expires_at: u64) -> String {
    let claims = format!("{}-{expires_at}", role_name(role));
    format!("{claims}-{}", grant_signature(secret, &claims))
}

fn verify_grant(secret: &str, grant: &str, requested: NodeRole, now: u64) -> Result<(), String> {
    let (claims, signature) = grant
        .rsplit_once('-')
        .ok_or_else(|| "invalid join token".to_string())?;
    let expires_at = claims
        .split_once('-')
        .and_then(|(_, expires_at)| expires_at.parse::<u64>().ok())
        .ok_or_else(|| "invalid join token".to_string())?;
    let expected = grant_signature(secret, claims);
    match (
        constant_time_eq(signature.as_bytes(), expected.as_bytes()),
        grant_role(grant) == Some(requested),
        expires_at >= now,
    ) {
        (false, _, _) => Err("invalid join token".to_string()),
        (true, false, _) => Err(format!(
            "this join token does not admit {} nodes",
            role_name(requested)
        )),
        (true, true, false) => Err("the join token has expired; mint a new one".to_string()),
        (true, true, true) => Ok(()),
    }
}

fn grant_role(grant: &str) -> Option<NodeRole> {
    match grant.split('-').next()? {
        "voter" => Some(NodeRole::Voter),
        "worker" => Some(NodeRole::Worker),
        _ => None,
    }
}

fn grant_signature(secret: &str, claims: &str) -> String {
    let mut mac = Hmac::<Sha256>::new_from_slice(secret.as_bytes())
        .unwrap_or_else(|_| unreachable!("HMAC accepts keys of any length"));
    mac.update(claims.as_bytes());
    hex_encode(&mac.finalize().into_bytes())
}

fn role_name(role: NodeRole) -> &'static str {
    match role {
        NodeRole::Voter => "voter",
        NodeRole::Worker => "worker",
    }
}

fn unix_now() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_secs())
}

fn cert_role(role: NodeRole) -> CertRole {
    match role {
        NodeRole::Voter => CertRole::Voter,
        NodeRole::Worker => CertRole::Worker,
    }
}

pub fn parse_role(value: &str) -> Result<NodeRole, String> {
    match value {
        "voter" => Ok(NodeRole::Voter),
        "worker" | "agent" => Ok(NodeRole::Worker),
        other => Err(format!("unknown role {other}; use voter or worker")),
    }
}

fn advertise_ip(advertise: &str) -> Option<IpAddr> {
    advertise
        .parse::<SocketAddr>()
        .map(|address| address.ip())
        .or_else(|_| advertise.parse::<IpAddr>())
        .ok()
}

fn constant_time_eq(left: &[u8], right: &[u8]) -> bool {
    left.len() == right.len()
        && left
            .iter()
            .zip(right)
            .fold(0u8, |difference, (left_byte, right_byte)| {
                difference | (left_byte ^ right_byte)
            })
            == 0
}

fn hex_encode(bytes: &[u8]) -> String {
    bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

fn internal(error: impl std::fmt::Display) -> Status {
    Status::internal(error.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn join_grants_admit_only_their_role_until_they_expire() {
        let grant = sign_grant("secret", NodeRole::Worker, 1_000);
        assert_eq!(grant_role(&grant), Some(NodeRole::Worker));
        assert_eq!(
            verify_grant("secret", &grant, NodeRole::Worker, 999),
            Ok(())
        );
        assert!(verify_grant("secret", &grant, NodeRole::Voter, 999).is_err());
        assert!(verify_grant("secret", &grant, NodeRole::Worker, 1_001).is_err());
        assert!(verify_grant("other", &grant, NodeRole::Worker, 999).is_err());
        let forged = grant.replacen("worker", "voter", 1);
        assert!(verify_grant("secret", &forged, NodeRole::Voter, 999).is_err());
        let extended = grant.replacen("1000", "9999999999", 1);
        assert!(verify_grant("secret", &extended, NodeRole::Worker, 999).is_err());
    }

    #[test]
    fn advertises_the_local_address_that_reaches_the_voters() {
        let voters = vec!["127.0.0.1:7400".to_string()];
        assert_eq!(resolve_advertise(None, &voters).unwrap(), "127.0.0.1:7400");
        assert_eq!(
            resolve_advertise(Some("10.1.0.11"), &voters).unwrap(),
            "10.1.0.11:7400"
        );
    }

    #[test]
    fn normalizes_voter_addresses() {
        assert_eq!(grpc_address("10.1.0.11"), "10.1.0.11:7400");
        assert_eq!(grpc_address(" 10.1.0.11:7500 "), "10.1.0.11:7500");
    }

    #[test]
    fn ca_proofs_need_the_join_secret_and_cannot_pass_as_grants() {
        let proof = ca_proof("secret", "abc");
        assert_eq!(proof, ca_proof("secret", "abc"));
        assert_ne!(proof, ca_proof("other", "abc"));
        assert_ne!(proof, ca_proof("secret", "abd"));
        let forged = format!("{CA_PROOF_PREFIX}abc-{proof}");
        assert!(verify_grant("secret", &forged, NodeRole::Voter, 0).is_err());
    }

    #[test]
    fn compares_secrets_in_constant_time() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secreT"));
        assert!(!constant_time_eq(b"secret", b"secrets"));
    }

    #[test]
    fn extracts_the_advertised_ip() {
        assert_eq!(
            advertise_ip("10.0.0.5:7400"),
            Some("10.0.0.5".parse().unwrap())
        );
        assert_eq!(advertise_ip("10.0.0.5"), Some("10.0.0.5".parse().unwrap()));
        assert_eq!(advertise_ip("node-a:7400"), None);
    }
}
