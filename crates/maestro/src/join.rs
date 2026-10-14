use std::net::{IpAddr, SocketAddr};
use std::path::{Path, PathBuf};

use anyhow::Context;
use clap::Args;
use maestro_agent::{WIREGUARD_PORT, detect_capacity, load_or_generate_key, system_reserve};
use maestro_control::allocate_subnet;
use maestro_core::{
    ClusterConfig, ClusterSettings, JoinRequest, JoinResponse, NodeRecord, NodeRole, NodeState,
    VoterSecrets, WireGuardInfo,
};
use maestro_proto::pb::cluster_client::ClusterClient;
use maestro_proto::{ClusterCa, NodeCredentials, connect_pinned};
use maestro_store::{Consensus, Mutation};
use tonic::{Code, Status};

use crate::cluster_service::{decode, encode};
use crate::identity::{NodeIdentity, hostname, random_node_id};

const TOKEN_PREFIX: &str = "mj1";
const JOIN_SECRET_FILE: &str = "join.key";
const LEADER_MARKER: &str = "leader=";

#[derive(Args)]
pub struct JoinArgs {
    #[arg(long)]
    server: String,
    #[arg(long, env = "MAESTRO_JOIN_TOKEN", hide_env_values = true)]
    token: String,
    #[arg(long)]
    advertise: String,
    #[arg(long, value_parser = parse_role, default_value = "agent")]
    role: NodeRole,
    #[arg(long, default_value = "/var/lib/maestro")]
    data_dir: PathBuf,
    #[arg(long)]
    name: Option<String>,
    #[arg(long)]
    config: Option<String>,
}

pub struct JoinAuthority {
    ca: ClusterCa,
    secret: String,
    data_dir: PathBuf,
}

impl JoinAuthority {
    pub fn load(data_dir: &Path) -> anyhow::Result<Self> {
        Ok(Self {
            ca: ClusterCa::load(&data_dir.join("pki"))?,
            secret: join_secret(data_dir)?,
            data_dir: data_dir.to_path_buf(),
        })
    }

    pub async fn admit(
        &self,
        consensus: &Consensus,
        cluster: &ClusterSettings,
        request: JoinRequest,
    ) -> Result<JoinResponse, Status> {
        let leader = consensus.current_leader().map(|(_, address)| address);
        let authorized = constant_time_eq(request.secret.as_bytes(), self.secret.as_bytes());
        match (consensus.is_leader(), authorized) {
            (false, _) => Err(Status::failed_precondition(format!(
                "not the leader; {LEADER_MARKER}{}",
                leader.unwrap_or_default()
            ))),
            (true, false) => Err(Status::permission_denied("invalid join token")),
            (true, true) => self.register(consensus, cluster, request).await,
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
        let node_id = std::iter::repeat_with(random_node_id)
            .find(|candidate| nodes.iter().all(|node| node.id != *candidate))
            .unwrap_or_else(random_node_id);
        let record = NodeRecord {
            id: node_id,
            name: request.name.clone(),
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
        consensus
            .write(vec![Mutation::PutNode { node: record }])
            .await
            .map_err(internal)?;
        let addresses = advertise_ip(&request.advertise)
            .into_iter()
            .collect::<Vec<_>>();
        let credentials = self
            .ca
            .issue_node(node_id.0, &addresses)
            .map_err(internal)?;
        let voter_secrets = match request.role {
            NodeRole::Voter => Some(self.voter_secrets().map_err(internal)?),
            NodeRole::Agent => None,
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

    fn voter_secrets(&self) -> anyhow::Result<VoterSecrets> {
        let keys = self.data_dir.join("keys");
        Ok(VoterSecrets {
            ca_key_pem: self.ca.key_pem().to_string(),
            encryption_key: std::fs::read(keys.join("encryption.key"))?,
            api_key: std::fs::read(keys.join("api.key"))?,
            join_secret: self.secret.clone(),
        })
    }
}

pub fn create_token(data_dir: &Path) -> anyhow::Result<()> {
    let ca = ClusterCa::load(&data_dir.join("pki"))?;
    println!(
        "{TOKEN_PREFIX}.{}.{}",
        ca.fingerprint()?,
        join_secret(data_dir)?
    );
    Ok(())
}

pub fn join_secret(data_dir: &Path) -> anyhow::Result<String> {
    let path = data_dir.join("keys").join(JOIN_SECRET_FILE);
    match std::fs::read_to_string(&path) {
        Ok(secret) => Ok(secret.trim().to_string()),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let secret = hex_encode(&rand::random::<[u8; 32]>());
            write_private(&path, secret.as_bytes())?;
            Ok(secret)
        }
        Err(error) => Err(error.into()),
    }
}

pub async fn join(args: JoinArgs) -> anyhow::Result<()> {
    let (fingerprint, secret) = parse_token(&args.token)?;
    let config = match &args.config {
        Some(source) => {
            ClusterConfig::load_with(source, &crate::secret_sources::read_config_source)?
        }
        None => ClusterConfig::default(),
    };
    let capacity = detect_capacity()?;
    let wireguard_key = load_or_generate_key(&args.data_dir.join("keys")).await?;
    let wireguard_endpoint = advertise_ip(&args.advertise)
        .map(|ip| SocketAddr::new(ip, WIREGUARD_PORT))
        .context("--advertise must be an IP:port the other nodes can reach")?;
    let request = JoinRequest {
        secret,
        name: args.name.clone().unwrap_or_else(hostname),
        role: args.role,
        advertise: args.advertise.clone(),
        capacity,
        reserved: system_reserve(&capacity, &config.node.reserve),
        labels: config.node.labels.clone(),
        wireguard: Some(WireGuardInfo {
            public_key: wireguard_key.public_key,
            endpoint: wireguard_endpoint,
        }),
    };
    let response = match request_join(&args.server, &fingerprint, &request).await {
        Err(status) if status.code() == Code::FailedPrecondition => {
            let leader = status
                .message()
                .split_once(LEADER_MARKER)
                .map(|(_, leader)| leader.trim().to_string())
                .filter(|leader| !leader.is_empty())
                .context("the server is not the leader and did not name one")?;
            request_join(&leader, &fingerprint, &request).await?
        }
        other => other?,
    };
    persist(&args, response)?;
    println!(
        "joined the cluster; start the node with `maestro agent --data-dir {} --advertise {}`",
        args.data_dir.display(),
        args.advertise
    );
    Ok(())
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

fn persist(args: &JoinArgs, response: JoinResponse) -> anyhow::Result<()> {
    let pki = args.data_dir.join("pki");
    let credentials = NodeCredentials {
        cert_pem: response.cert_pem,
        key_pem: response.key_pem,
        ca_pem: response.ca_pem.clone(),
    };
    credentials.save(&pki.join("node"))?;
    if let Some(secrets) = response.voter_secrets {
        ClusterCa::from_pem(response.ca_pem, secrets.ca_key_pem).save(&pki)?;
        let keys = args.data_dir.join("keys");
        write_private(&keys.join("encryption.key"), &secrets.encryption_key)?;
        write_private(&keys.join("api.key"), &secrets.api_key)?;
        write_private(&keys.join(JOIN_SECRET_FILE), secrets.join_secret.as_bytes())?;
    }
    NodeIdentity {
        id: response.node_id,
        name: args.name.clone().unwrap_or_else(hostname),
        role: args.role,
        subnet: response.subnet,
        advertise: args.advertise.clone(),
        seeds: response.voters,
        cluster: response.cluster,
    }
    .save(&args.data_dir)
}

fn parse_token(token: &str) -> anyhow::Result<(String, String)> {
    match token.trim().splitn(3, '.').collect::<Vec<_>>().as_slice() {
        [TOKEN_PREFIX, fingerprint, secret] => Ok((fingerprint.to_string(), secret.to_string())),
        _ => anyhow::bail!("invalid join token; expected {TOKEN_PREFIX}.<ca-fingerprint>.<secret>"),
    }
}

fn parse_role(value: &str) -> Result<NodeRole, String> {
    match value {
        "voter" => Ok(NodeRole::Voter),
        "agent" => Ok(NodeRole::Agent),
        other => Err(format!("unknown role {other}; use voter or agent")),
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

fn write_private(path: &Path, contents: &[u8]) -> anyhow::Result<()> {
    use std::io::Write;
    use std::os::unix::fs::OpenOptionsExt;
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent)?;
    }
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create(true)
        .truncate(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(contents)?;
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_join_tokens() {
        assert_eq!(
            parse_token("mj1.abcdef.s3cret").unwrap(),
            ("abcdef".to_string(), "s3cret".to_string())
        );
        assert!(parse_token("mj2.abcdef.s3cret").is_err());
        assert!(parse_token("mj1.only-two").is_err());
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
