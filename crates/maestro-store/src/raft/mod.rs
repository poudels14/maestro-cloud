mod log_store;
mod network;
mod service;
mod state_machine;

use std::collections::{BTreeMap, BTreeSet};
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::NodeId;
use maestro_proto::NodeCredentials;
use maestro_proto::pb::raft_server::RaftServer;
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{BasicNode, ChangeMembers, Config, Raft, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::StoreError;
use crate::mutation::Mutation;
use crate::state::StateMachine;
use log_store::LogStore;
use network::GrpcNetwork;
pub use service::RaftService;
use state_machine::StateMachineStore;

const HEARTBEAT_INTERVAL_MS: u64 = 250;
const ELECTION_TIMEOUT_MIN_MS: u64 = 1000;
const ELECTION_TIMEOUT_MAX_MS: u64 = 2000;
const INSTALL_SNAPSHOT_TIMEOUT_MS: u64 = 10_000;
const SNAPSHOT_EVERY_LOGS: u64 = 5000;
const LOGS_KEPT_AFTER_SNAPSHOT: u64 = 1000;
pub(crate) const MAX_MESSAGE_BYTES: usize = 64 * 1024 * 1024;

openraft::declare_raft_types!(
    pub TypeConfig:
        D = Request,
        R = Response,
        NodeId = u64,
        Node = BasicNode,
);

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct Request {
    pub mutations: Vec<Mutation>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", rename_all = "camelCase")]
pub enum Response {
    Applied { version: u64 },
    Rejected { reason: String },
    Empty,
}

#[derive(Clone, Debug)]
pub struct ConsensusSettings {
    pub node_id: NodeId,
    pub address: String,
    pub data_dir: PathBuf,
    pub snapshot_every_logs: u64,
    pub logs_kept_after_snapshot: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ConsensusMetrics {
    pub node_id: NodeId,
    pub term: u64,
    pub leader: Option<NodeId>,
    pub last_log_index: Option<u64>,
    pub last_applied_index: Option<u64>,
    pub snapshot_index: Option<u64>,
    pub voters: BTreeMap<NodeId, String>,
    pub learners: BTreeMap<NodeId, String>,
}

pub struct Consensus {
    raft: Raft<TypeConfig>,
    state: Arc<StateMachine>,
    settings: ConsensusSettings,
}

#[derive(Debug, Error)]
pub enum ConsensusError {
    #[error(transparent)]
    Store(#[from] StoreError),
    #[error("write rejected: {0}")]
    Rejected(String),
    #[error("this node is not the leader (leader {leader_id:?} at {leader_address:?})")]
    ForwardToLeader {
        leader_id: Option<NodeId>,
        leader_address: Option<String>,
    },
    #[error("raft error: {0}")]
    Raft(String),
}

impl ConsensusSettings {
    pub fn new(node_id: NodeId, address: impl Into<String>, data_dir: impl Into<PathBuf>) -> Self {
        Self {
            node_id,
            address: address.into(),
            data_dir: data_dir.into(),
            snapshot_every_logs: SNAPSHOT_EVERY_LOGS,
            logs_kept_after_snapshot: LOGS_KEPT_AFTER_SNAPSHOT,
        }
    }
}

impl Consensus {
    pub async fn open(
        settings: ConsensusSettings,
        credentials: Option<NodeCredentials>,
    ) -> Result<Self, ConsensusError> {
        std::fs::create_dir_all(&settings.data_dir).map_err(StoreError::from)?;
        let state = Arc::new(StateMachine::open(settings.data_dir.join("state.redb"))?);
        let log_store = LogStore::open(settings.data_dir.join("raft-log.redb"))?;
        let state_machine =
            StateMachineStore::new(state.clone(), settings.data_dir.join("snapshot.json"));
        let config = Config {
            cluster_name: "maestro".to_string(),
            heartbeat_interval: HEARTBEAT_INTERVAL_MS,
            election_timeout_min: ELECTION_TIMEOUT_MIN_MS,
            election_timeout_max: ELECTION_TIMEOUT_MAX_MS,
            install_snapshot_timeout: INSTALL_SNAPSHOT_TIMEOUT_MS,
            snapshot_policy: SnapshotPolicy::LogsSinceLast(settings.snapshot_every_logs),
            max_in_snapshot_log_to_keep: settings.logs_kept_after_snapshot,
            ..Default::default()
        }
        .validate()
        .map_err(raft_error)?;
        let raft = Raft::new(
            settings.node_id.0,
            Arc::new(config),
            GrpcNetwork::new(
                settings.node_id.0,
                credentials.map(|credentials| credentials.voter_client_tls()),
            ),
            log_store,
            state_machine,
        )
        .await
        .map_err(raft_error)?;
        Ok(Self {
            raft,
            state,
            settings,
        })
    }

    pub async fn bootstrap_single_voter(&self) -> Result<(), ConsensusError> {
        let members = BTreeMap::from([(
            self.settings.node_id.0,
            BasicNode::new(&self.settings.address),
        )]);
        match self.raft.initialize(members).await {
            Ok(()) | Err(RaftError::APIError(InitializeError::NotAllowed(_))) => Ok(()),
            Err(error) => Err(raft_error(error)),
        }
    }

    pub async fn wait_until_leader(&self, timeout: Duration) -> Result<(), ConsensusError> {
        self.raft
            .wait(Some(timeout))
            .state(ServerState::Leader, "wait for leadership")
            .await
            .map(|_| ())
            .map_err(raft_error)
    }

    pub async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
        let response = self
            .raft
            .client_write(Request { mutations })
            .await
            .map_err(client_write_error)?;
        match response.data {
            Response::Applied { version } => Ok(version),
            Response::Rejected { reason } => Err(ConsensusError::Rejected(reason)),
            Response::Empty => Err(ConsensusError::Raft(
                "write produced no state machine response".to_string(),
            )),
        }
    }

    pub async fn add_learner(&self, node_id: NodeId, address: &str) -> Result<(), ConsensusError> {
        self.raft
            .add_learner(node_id.0, BasicNode::new(address), true)
            .await
            .map(|_| ())
            .map_err(client_write_error)
    }

    pub async fn promote(&self, node_ids: BTreeSet<NodeId>) -> Result<(), ConsensusError> {
        self.change(ChangeMembers::AddVoterIds(raw_ids(node_ids)))
            .await
    }

    pub async fn change_membership(&self, voters: BTreeSet<NodeId>) -> Result<(), ConsensusError> {
        self.change(ChangeMembers::ReplaceAllVoters(raw_ids(voters)))
            .await
    }

    pub async fn remove_node(&self, node_id: NodeId) -> Result<(), ConsensusError> {
        let removed = BTreeSet::from([node_id.0]);
        match self.metrics().voters.contains_key(&node_id) {
            true => self.change(ChangeMembers::RemoveVoters(removed)).await,
            false => self.change(ChangeMembers::RemoveNodes(removed)).await,
        }
    }

    pub fn current_leader(&self) -> Option<(NodeId, String)> {
        let metrics = self.raft.metrics().borrow().clone();
        metrics.current_leader.and_then(|leader| {
            metrics
                .membership_config
                .membership()
                .get_node(&leader)
                .map(|node| (NodeId(leader), node.addr.clone()))
        })
    }

    pub fn metrics(&self) -> ConsensusMetrics {
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let addresses_of = |node_ids: Vec<u64>| {
            node_ids
                .into_iter()
                .filter_map(|node_id| {
                    membership
                        .get_node(&node_id)
                        .map(|node| (NodeId(node_id), node.addr.clone()))
                })
                .collect::<BTreeMap<_, _>>()
        };
        ConsensusMetrics {
            node_id: NodeId(metrics.id),
            term: metrics.current_term,
            leader: metrics.current_leader.map(NodeId),
            last_log_index: metrics.last_log_index,
            last_applied_index: metrics.last_applied.map(|log_id| log_id.index),
            snapshot_index: metrics.snapshot.map(|log_id| log_id.index),
            voters: addresses_of(membership.voter_ids().collect()),
            learners: addresses_of(membership.learner_ids().collect()),
        }
    }

    pub fn raft_service(&self) -> RaftServer<RaftService> {
        RaftServer::new(RaftService::new(self.raft.clone()))
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES)
    }

    pub async fn trigger_snapshot(&self) -> Result<(), ConsensusError> {
        self.raft.trigger().snapshot().await.map_err(raft_error)
    }

    pub fn state(&self) -> &StateMachine {
        &self.state
    }

    pub fn state_handle(&self) -> Arc<StateMachine> {
        self.state.clone()
    }

    pub fn is_leader(&self) -> bool {
        self.raft.metrics().borrow().current_leader == Some(self.settings.node_id.0)
    }

    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft.shutdown().await.map_err(raft_error)
    }

    async fn change(&self, changes: ChangeMembers<u64, BasicNode>) -> Result<(), ConsensusError> {
        self.raft
            .change_membership(changes, false)
            .await
            .map(|_| ())
            .map_err(client_write_error)
    }
}

fn client_write_error(error: RaftError<u64, ClientWriteError<u64, BasicNode>>) -> ConsensusError {
    match error {
        RaftError::APIError(ClientWriteError::ForwardToLeader(forward)) => {
            ConsensusError::ForwardToLeader {
                leader_id: forward.leader_id.map(NodeId),
                leader_address: forward.leader_node.map(|node| node.addr),
            }
        }
        other => raft_error(other),
    }
}

fn raw_ids(node_ids: BTreeSet<NodeId>) -> BTreeSet<u64> {
    node_ids.into_iter().map(|node_id| node_id.0).collect()
}

fn raft_error(error: impl std::fmt::Display) -> ConsensusError {
    ConsensusError::Raft(error.to_string())
}
