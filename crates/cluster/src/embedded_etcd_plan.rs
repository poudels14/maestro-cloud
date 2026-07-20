use std::collections::BTreeSet;
use std::path::PathBuf;

use kernel_api::{ClusterId, NodeId};
use serde::{Deserialize, Serialize};

use crate::{StoreJoinTicket, StoreProviderConfig, StoreProviderError, StoreStartMode};

pub(crate) const TICKET_FORMAT_VERSION: u8 = 1;

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EtcdJoinTicketData {
    pub format_version: u8,
    pub cluster_id: ClusterId,
    pub node_id: NodeId,
    pub member_id: u64,
    pub members: Vec<EtcdMemberPlan>,
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct EtcdMemberPlan {
    pub node_id: NodeId,
    pub peer_url: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum EtcdLaunchMode {
    Start(StoreStartMode),
    Recover,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtcdSecurityPaths {
    pub certificate_authority: PathBuf,
    pub certificate: PathBuf,
    pub private_key: PathBuf,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct EtcdStartPlan {
    pub arguments: Vec<String>,
    pub data_directory: PathBuf,
    pub local_client_url: String,
    pub readiness: EtcdReadiness,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum EtcdReadiness {
    WritableQuorum,
    JoinedLearner { member_id: u64 },
}

impl EtcdJoinTicketData {
    pub fn decode(ticket: &StoreJoinTicket) -> Result<Self, StoreProviderError> {
        let bytes = ticket.provider_data()?;
        serde_json::from_slice(&bytes).map_err(|_| StoreProviderError::InvalidJoinTicket)
    }

    pub fn encode(&self) -> Result<StoreJoinTicket, StoreProviderError> {
        let bytes = serde_json::to_vec(self).map_err(|_| StoreProviderError::InvalidJoinTicket)?;
        Ok(StoreJoinTicket::from_provider_data(
            self.node_id.clone(),
            &bytes,
        ))
    }
}

impl EtcdStartPlan {
    pub fn build(
        config: &StoreProviderConfig,
        mode: EtcdLaunchMode,
        security: &EtcdSecurityPaths,
    ) -> Result<Self, StoreProviderError> {
        let (initial_cluster, initial_state, force_new_cluster, readiness) = match mode {
            EtcdLaunchMode::Start(StoreStartMode::Bootstrap) => (
                member_entry(
                    &config.local_member().node_id,
                    &peer_url(config, config.local_member().host_address),
                ),
                "new",
                ForceNewCluster::No,
                EtcdReadiness::WritableQuorum,
            ),
            EtcdLaunchMode::Start(StoreStartMode::Restart) => (
                known_member_entries(config),
                "existing",
                ForceNewCluster::No,
                EtcdReadiness::WritableQuorum,
            ),
            EtcdLaunchMode::Start(StoreStartMode::Join(ticket)) => {
                let ticket = validate_ticket(config, &ticket)?;
                (
                    ticket_entries(&ticket.members),
                    "existing",
                    ForceNewCluster::No,
                    EtcdReadiness::JoinedLearner {
                        member_id: ticket.member_id,
                    },
                )
            }
            EtcdLaunchMode::Recover => (
                member_entry(
                    &config.local_member().node_id,
                    &peer_url(config, config.local_member().host_address),
                ),
                "existing",
                ForceNewCluster::Yes,
                EtcdReadiness::WritableQuorum,
            ),
        };
        let host = config.local_member().host_address;
        let local_client_url = client_url(config, host);
        let data_directory = config.data_directory().join("data");
        let mut arguments = vec![
            format!("--name={}", member_name(&config.local_member().node_id)),
            format!("--data-dir={}", data_directory.display()),
            format!(
                "--listen-client-urls=https://{host}:{}",
                config.client_port()
            ),
            format!(
                "--listen-peer-urls=https://{host}:{}",
                config.membership_port()
            ),
            format!("--advertise-client-urls={local_client_url}"),
            format!("--initial-advertise-peer-urls={}", peer_url(config, host)),
            format!("--initial-cluster={initial_cluster}"),
            format!("--initial-cluster-state={initial_state}"),
            format!("--initial-cluster-token=maestro-{}", config.cluster_id()),
            "--strict-reconfig-check=true".to_owned(),
            "--auto-compaction-mode=periodic".to_owned(),
            "--auto-compaction-retention=1h".to_owned(),
            "--quota-backend-bytes=8589934592".to_owned(),
            format!("--cert-file={}", security.certificate.display()),
            format!("--key-file={}", security.private_key.display()),
            format!(
                "--trusted-ca-file={}",
                security.certificate_authority.display()
            ),
            "--client-cert-auth=true".to_owned(),
            format!("--peer-cert-file={}", security.certificate.display()),
            format!("--peer-key-file={}", security.private_key.display()),
            format!(
                "--peer-trusted-ca-file={}",
                security.certificate_authority.display()
            ),
            "--peer-client-cert-auth=true".to_owned(),
        ];
        if force_new_cluster == ForceNewCluster::Yes {
            arguments.push("--force-new-cluster=true".to_owned());
        }
        Ok(Self {
            arguments,
            data_directory,
            local_client_url,
            readiness,
        })
    }
}

pub(crate) fn member_name(node_id: &NodeId) -> String {
    format!("maestro-{node_id}")
}

pub(crate) fn client_url(config: &StoreProviderConfig, host: std::net::Ipv4Addr) -> String {
    format!("https://{host}:{}", config.client_port())
}

pub(crate) fn peer_url(config: &StoreProviderConfig, host: std::net::Ipv4Addr) -> String {
    format!("https://{host}:{}", config.membership_port())
}

fn validate_ticket(
    config: &StoreProviderConfig,
    ticket: &StoreJoinTicket,
) -> Result<EtcdJoinTicketData, StoreProviderError> {
    let ticket_data = EtcdJoinTicketData::decode(ticket)?;
    if ticket_data.format_version != TICKET_FORMAT_VERSION
        || ticket_data.cluster_id != *config.cluster_id()
        || ticket_data.node_id != config.local_member().node_id
        || ticket.node_id() != &ticket_data.node_id
        || ticket_data.member_id == 0
    {
        return Err(StoreProviderError::InvalidJoinTicket);
    }
    let mut identities = BTreeSet::new();
    for member in &ticket_data.members {
        let Some(known) = config.known_members().get(&member.node_id) else {
            return Err(StoreProviderError::InvalidJoinTicket);
        };
        if member.peer_url != peer_url(config, known.host_address)
            || !identities.insert(member.node_id.clone())
        {
            return Err(StoreProviderError::InvalidJoinTicket);
        }
    }
    if !identities.contains(&config.local_member().node_id) {
        return Err(StoreProviderError::InvalidJoinTicket);
    }
    Ok(ticket_data)
}

fn known_member_entries(config: &StoreProviderConfig) -> String {
    config
        .known_members()
        .values()
        .map(|member| member_entry(&member.node_id, &peer_url(config, member.host_address)))
        .collect::<Vec<_>>()
        .join(",")
}

fn ticket_entries(members: &[EtcdMemberPlan]) -> String {
    let mut members = members.to_vec();
    members.sort_by(|left, right| left.node_id.cmp(&right.node_id));
    members
        .iter()
        .map(|member| member_entry(&member.node_id, &member.peer_url))
        .collect::<Vec<_>>()
        .join(",")
}

fn member_entry(node_id: &NodeId, peer_url: &str) -> String {
    format!("{}={peer_url}", member_name(node_id))
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ForceNewCluster {
    No,
    Yes,
}
