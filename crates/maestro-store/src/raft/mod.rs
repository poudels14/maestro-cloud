mod log_store;
mod network;
mod state_machine;

use std::collections::BTreeMap;
use std::io::Cursor;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::NodeId;
use openraft::error::{ClientWriteError, InitializeError, RaftError};
use openraft::{BasicNode, Config, Raft, ServerState, SnapshotPolicy};
use serde::{Deserialize, Serialize};
use thiserror::Error;

use crate::error::StoreError;
use crate::mutation::Mutation;
use crate::state::StateMachine;
use log_store::LogStore;
use network::LocalOnlyNetwork;
use state_machine::StateMachineStore;

const HEARTBEAT_INTERVAL_MS: u64 = 250;
const ELECTION_TIMEOUT_MIN_MS: u64 = 1000;
const ELECTION_TIMEOUT_MAX_MS: u64 = 2000;
const SNAPSHOT_EVERY_LOGS: u64 = 5000;
const LOGS_KEPT_AFTER_SNAPSHOT: u64 = 1000;

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
    #[error("raft error: {0}")]
    Raft(String),
}

impl Consensus {
    pub async fn open(settings: ConsensusSettings) -> Result<Self, ConsensusError> {
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
            snapshot_policy: SnapshotPolicy::LogsSinceLast(SNAPSHOT_EVERY_LOGS),
            max_in_snapshot_log_to_keep: LOGS_KEPT_AFTER_SNAPSHOT,
            ..Default::default()
        }
        .validate()
        .map_err(raft_error)?;
        let raft = Raft::new(
            settings.node_id.0,
            Arc::new(config),
            LocalOnlyNetwork,
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
            .map_err(|error: RaftError<u64, ClientWriteError<u64, BasicNode>>| raft_error(error))?;
        match response.data {
            Response::Applied { version } => Ok(version),
            Response::Rejected { reason } => Err(ConsensusError::Rejected(reason)),
            Response::Empty => Err(ConsensusError::Raft(
                "write produced no state machine response".to_string(),
            )),
        }
    }

    pub async fn trigger_snapshot(&self) -> Result<(), ConsensusError> {
        self.raft.trigger().snapshot().await.map_err(raft_error)
    }

    pub fn state(&self) -> &StateMachine {
        &self.state
    }

    pub async fn shutdown(&self) -> Result<(), ConsensusError> {
        self.raft.shutdown().await.map_err(raft_error)
    }
}

fn raft_error(error: impl std::fmt::Display) -> ConsensusError {
    ConsensusError::Raft(error.to_string())
}
