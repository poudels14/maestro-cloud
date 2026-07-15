use std::{collections::BTreeMap, sync::Arc, time::Duration};

use anyhow::{Result, bail};
use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, EventType, GetOptions, TlsOptions, Txn, TxnOp,
};
use tokio::sync::{Mutex, watch};

use crate::{
    cluster::types::{
        AssignmentManifest, LeadershipToken, NodeId, PlacementHistory, UnschedulableReplica,
    },
    deployment::types::ReplicaState,
};

const ASSIGNMENTS_PREFIX: &str = "/maetro/cluster/assignments/";
const REPLICA_STATES_PREFIX: &str = "/maetro/cluster/replica-states/";
const UNSCHEDULABLE_KEY: &str = "/maetro/cluster/unschedulable";

pub type AssignmentWatcher = watch::Receiver<Option<AssignmentManifest>>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ReplaceOutcome {
    Applied,
    GenerationConflict,
    LeadershipLost,
}

#[async_trait]
pub trait AssignmentStore: Send + Sync {
    async fn list_all(&self) -> Result<Vec<AssignmentManifest>>;
    async fn get_for_node(&self, node_id: &NodeId) -> Result<Option<AssignmentManifest>>;
    async fn replace_for_node(
        &self,
        token: &LeadershipToken,
        expected_generation: u64,
        manifest: AssignmentManifest,
    ) -> Result<ReplaceOutcome>;
    async fn watch_node(&self, node_id: &NodeId) -> Result<AssignmentWatcher>;
    async fn upsert_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
        state: ReplicaState,
        placement: Option<&PlacementHistory>,
    ) -> Result<bool>;
    async fn delete_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
    ) -> Result<bool>;
    async fn list_replica_states(&self) -> Result<Vec<ReplicaState>>;
    async fn write_unschedulable(
        &self,
        token: &LeadershipToken,
        entries: &[UnschedulableReplica],
    ) -> Result<ReplaceOutcome>;
}

pub struct EtcdAssignmentStore {
    client: Arc<Mutex<Client>>,
}

impl EtcdAssignmentStore {
    pub async fn connect(endpoints: &[String], tls: Option<TlsOptions>) -> Result<Self> {
        let options = tls.map(|tls| ConnectOptions::new().with_tls(tls));
        let client = Client::connect(endpoints, options).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
        })
    }

    async fn current_manifest(
        &self,
        node_id: &NodeId,
    ) -> Result<Option<(AssignmentManifest, i64)>> {
        let response = self
            .client
            .lock()
            .await
            .get(manifest_key(node_id), None)
            .await?;
        response
            .kvs()
            .first()
            .map(|entry| Ok((serde_json::from_slice(entry.value())?, entry.mod_revision())))
            .transpose()
    }

    fn validate_manifest(manifest: &AssignmentManifest) -> Result<()> {
        if manifest
            .assignments
            .iter()
            .any(|assignment| assignment.node_id != manifest.node_id)
        {
            bail!("assignment manifest contains an assignment for another node");
        }
        let mut identities = BTreeMap::new();
        let mut addresses = std::collections::BTreeSet::new();
        for assignment in &manifest.assignments {
            let identity = (
                assignment.service_id.as_str(),
                assignment.deployment_id.as_str(),
                assignment.replica_index,
                assignment.node_id.as_str(),
            );
            if identities
                .insert(assignment.assignment_id.as_str(), identity)
                .is_some()
            {
                bail!("assignment manifest contains a duplicate assignment id");
            }
            if let Some(address) = assignment.container_ip
                && !addresses.insert(address)
            {
                bail!("assignment manifest contains duplicate container address `{address}`");
            }
        }
        Ok(())
    }
}

#[async_trait]
impl AssignmentStore for EtcdAssignmentStore {
    async fn list_all(&self) -> Result<Vec<AssignmentManifest>> {
        let response = self
            .client
            .lock()
            .await
            .get(ASSIGNMENTS_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?;
        let mut manifests = response
            .kvs()
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect::<Result<Vec<AssignmentManifest>>>()?;
        manifests.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(manifests)
    }

    async fn get_for_node(&self, node_id: &NodeId) -> Result<Option<AssignmentManifest>> {
        Ok(self
            .current_manifest(node_id)
            .await?
            .map(|(manifest, _)| manifest))
    }

    async fn replace_for_node(
        &self,
        token: &LeadershipToken,
        expected_generation: u64,
        mut manifest: AssignmentManifest,
    ) -> Result<ReplaceOutcome> {
        Self::validate_manifest(&manifest)?;
        let key = manifest_key(&manifest.node_id);
        let current = self.current_manifest(&manifest.node_id).await?;
        if current
            .as_ref()
            .map_or(expected_generation != 0, |(value, _)| {
                value.generation != expected_generation
            })
        {
            return Ok(ReplaceOutcome::GenerationConflict);
        }
        manifest.generation = expected_generation.saturating_add(1);
        let manifest_compare = current.as_ref().map_or_else(
            || Compare::version(key.clone(), CompareOp::Equal, 0),
            |(_, revision)| Compare::mod_revision(key.clone(), CompareOp::Equal, *revision),
        );
        let transaction = Txn::new()
            .when([
                Compare::create_revision(
                    token.election_key.clone(),
                    CompareOp::Equal,
                    token.create_revision,
                ),
                manifest_compare,
            ])
            .and_then([TxnOp::put(key, serde_json::to_vec(&manifest)?, None)]);
        if self.client.lock().await.txn(transaction).await?.succeeded() {
            Ok(ReplaceOutcome::Applied)
        } else if leadership_matches(token, &self.client).await? {
            Ok(ReplaceOutcome::GenerationConflict)
        } else {
            Ok(ReplaceOutcome::LeadershipLost)
        }
    }

    async fn watch_node(&self, node_id: &NodeId) -> Result<AssignmentWatcher> {
        let initial = self.get_for_node(node_id).await?;
        let (sender, receiver) = watch::channel(initial);
        let key = manifest_key(node_id);
        let client = self.client.clone();
        tokio::spawn(async move {
            loop {
                let watch = client.lock().await.watch(key.clone(), None).await;
                let Ok(mut stream) = watch else {
                    tokio::time::sleep(Duration::from_secs(1)).await;
                    continue;
                };
                loop {
                    let response = match stream.message().await {
                        Ok(Some(response)) => response,
                        Ok(None) | Err(_) => break,
                    };
                    for event in response.events() {
                        let value = match event.event_type() {
                            EventType::Delete => Some(None),
                            EventType::Put => event.kv().and_then(|entry| {
                                serde_json::from_slice(entry.value()).ok().map(Some)
                            }),
                        };
                        if let Some(value) = value {
                            let _ = sender.send(value);
                        }
                    }
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
        Ok(receiver)
    }

    async fn upsert_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
        mut state: ReplicaState,
        placement: Option<&PlacementHistory>,
    ) -> Result<bool> {
        let Some((manifest, revision)) = self.current_manifest(node_id).await? else {
            return Ok(false);
        };
        if !manifest
            .assignments
            .iter()
            .any(|assignment| assignment.assignment_id == assignment_id)
        {
            return Ok(false);
        }
        state.node_id = Some(node_id.clone());
        state.assignment_id = Some(assignment_id.to_string());
        let manifest_key = manifest_key(node_id);
        let mut comparisons = vec![Compare::mod_revision(
            manifest_key,
            CompareOp::Equal,
            revision,
        )];
        let mut operations = vec![TxnOp::put(
            replica_state_key(node_id, assignment_id),
            serde_json::to_vec(&state)?,
            None,
        )];
        if let Some(placement) = placement {
            if placement.node_id != *node_id || placement.assignment_id != assignment_id {
                bail!("placement identity does not match assignment");
            }
            let key = placement_key(node_id, assignment_id);
            let encoded = serde_json::to_vec(placement)?;
            let existing = self.client.lock().await.get(key.clone(), None).await?;
            if let Some(entry) = existing.kvs().first() {
                let existing_placement: PlacementHistory = serde_json::from_slice(entry.value())?;
                if !same_placement_identity(&existing_placement, placement) {
                    bail!("placement history conflicts with an existing assignment record");
                }
                comparisons.push(Compare::value(key.clone(), CompareOp::Equal, entry.value()));
            } else {
                comparisons.push(Compare::version(key.clone(), CompareOp::Equal, 0));
                operations.push(TxnOp::put(key.clone(), encoded, None));
            }
            operations.push(TxnOp::put(placement_index_key(placement), key, None));
        }
        let transaction = Txn::new().when(comparisons).and_then(operations);
        Ok(self.client.lock().await.txn(transaction).await?.succeeded())
    }

    async fn delete_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
    ) -> Result<bool> {
        let key = replica_state_key(node_id, assignment_id);
        let mut client = self.client.lock().await;
        let response = client.get(key.clone(), None).await?;
        let Some(entry) = response.kvs().first() else {
            return Ok(true);
        };
        let state: ReplicaState = serde_json::from_slice(entry.value())?;
        if state.assignment_id.as_deref() != Some(assignment_id)
            || state.node_id.as_ref() != Some(node_id)
        {
            return Ok(false);
        }
        let state_value = entry.value().to_vec();
        let history_key = placement_key(node_id, assignment_id);
        let history = client.get(history_key.clone(), None).await?;
        let transaction = if let Some(history_entry) = history.kvs().first() {
            let history_value = history_entry.value().to_vec();
            let mut placement: PlacementHistory = serde_json::from_slice(&history_value)?;
            placement.ended_at_ms.get_or_insert_with(now_millis);
            Txn::new()
                .when([
                    Compare::value(key.clone(), CompareOp::Equal, state_value),
                    Compare::value(history_key.clone(), CompareOp::Equal, history_value),
                ])
                .and_then([
                    TxnOp::put(history_key, serde_json::to_vec(&placement)?, None),
                    TxnOp::delete(key, None),
                ])
        } else {
            Txn::new()
                .when([Compare::value(key.clone(), CompareOp::Equal, state_value)])
                .and_then([TxnOp::delete(key, None)])
        };
        Ok(client.txn(transaction).await?.succeeded())
    }

    async fn list_replica_states(&self) -> Result<Vec<ReplicaState>> {
        let response = self
            .client
            .lock()
            .await
            .get(REPLICA_STATES_PREFIX, Some(GetOptions::new().with_prefix()))
            .await?;
        response
            .kvs()
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn write_unschedulable(
        &self,
        token: &LeadershipToken,
        entries: &[UnschedulableReplica],
    ) -> Result<ReplaceOutcome> {
        let transaction = Txn::new()
            .when([Compare::create_revision(
                token.election_key.clone(),
                CompareOp::Equal,
                token.create_revision,
            )])
            .and_then([TxnOp::put(
                UNSCHEDULABLE_KEY,
                serde_json::to_vec(entries)?,
                None,
            )]);
        if self.client.lock().await.txn(transaction).await?.succeeded() {
            Ok(ReplaceOutcome::Applied)
        } else {
            Ok(ReplaceOutcome::LeadershipLost)
        }
    }
}

async fn leadership_matches(token: &LeadershipToken, client: &Arc<Mutex<Client>>) -> Result<bool> {
    let response = client
        .lock()
        .await
        .get(token.election_key.clone(), None)
        .await?;
    Ok(response
        .kvs()
        .first()
        .is_some_and(|entry| entry.create_revision() == token.create_revision))
}

fn manifest_key(node_id: &str) -> String {
    format!("{ASSIGNMENTS_PREFIX}{node_id}")
}

fn replica_state_key(node_id: &str, assignment_id: &str) -> String {
    format!("{REPLICA_STATES_PREFIX}{node_id}/{assignment_id}")
}

fn placement_key(node_id: &str, assignment_id: &str) -> String {
    format!("/maetro/cluster/placements/{node_id}/{assignment_id}")
}

fn placement_index_key(placement: &PlacementHistory) -> String {
    format!(
        "/maetro/cluster/placement-index/{}/{}/{}/{}",
        placement.service_id,
        placement.deployment_id,
        placement.replica_index,
        placement.assignment_id
    )
}

fn now_millis() -> i64 {
    crate::utils::time::current_time_millis()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_default()
}

fn same_placement_identity(left: &PlacementHistory, right: &PlacementHistory) -> bool {
    left.assignment_id == right.assignment_id
        && left.service_id == right.service_id
        && left.deployment_id == right.deployment_id
        && left.replica_index == right.replica_index
        && left.node_id == right.node_id
        && left.cluster_host_ip == right.cluster_host_ip
        && left.cluster_api_port == right.cluster_api_port
        && left.container_hostname == right.container_hostname
}

#[cfg(test)]
#[derive(Default)]
pub struct InMemoryAssignmentStore {
    manifests: std::sync::RwLock<BTreeMap<NodeId, AssignmentManifest>>,
    states: std::sync::RwLock<BTreeMap<(NodeId, String), ReplicaState>>,
    watchers: std::sync::Mutex<BTreeMap<NodeId, watch::Sender<Option<AssignmentManifest>>>>,
}

#[cfg(test)]
#[async_trait]
impl AssignmentStore for InMemoryAssignmentStore {
    async fn list_all(&self) -> Result<Vec<AssignmentManifest>> {
        Ok(self
            .manifests
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect())
    }

    async fn get_for_node(&self, node_id: &NodeId) -> Result<Option<AssignmentManifest>> {
        Ok(self
            .manifests
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(node_id)
            .cloned())
    }

    async fn replace_for_node(
        &self,
        _token: &LeadershipToken,
        expected_generation: u64,
        mut manifest: AssignmentManifest,
    ) -> Result<ReplaceOutcome> {
        EtcdAssignmentStore::validate_manifest(&manifest)?;
        let mut manifests = self
            .manifests
            .write()
            .unwrap_or_else(|error| error.into_inner());
        if manifests
            .get(&manifest.node_id)
            .map_or(expected_generation != 0, |value| {
                value.generation != expected_generation
            })
        {
            return Ok(ReplaceOutcome::GenerationConflict);
        }
        manifest.generation = expected_generation.saturating_add(1);
        manifests.insert(manifest.node_id.clone(), manifest.clone());
        drop(manifests);
        if let Some(sender) = self
            .watchers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .get(&manifest.node_id)
        {
            let _ = sender.send(Some(manifest));
        }
        Ok(ReplaceOutcome::Applied)
    }

    async fn watch_node(&self, node_id: &NodeId) -> Result<AssignmentWatcher> {
        let initial = self.get_for_node(node_id).await?;
        let (sender, receiver) = watch::channel(initial);
        self.watchers
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .insert(node_id.clone(), sender);
        Ok(receiver)
    }

    async fn upsert_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
        mut state: ReplicaState,
        _placement: Option<&PlacementHistory>,
    ) -> Result<bool> {
        let valid = self
            .manifests
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .get(node_id)
            .is_some_and(|manifest| {
                manifest
                    .assignments
                    .iter()
                    .any(|assignment| assignment.assignment_id == assignment_id)
            });
        if !valid {
            return Ok(false);
        }
        state.node_id = Some(node_id.clone());
        state.assignment_id = Some(assignment_id.to_string());
        self.states
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert((node_id.clone(), assignment_id.to_string()), state);
        Ok(true)
    }

    async fn delete_replica_state_if_assignment(
        &self,
        node_id: &NodeId,
        assignment_id: &str,
    ) -> Result<bool> {
        self.states
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .remove(&(node_id.clone(), assignment_id.to_string()));
        Ok(true)
    }

    async fn list_replica_states(&self) -> Result<Vec<ReplicaState>> {
        Ok(self
            .states
            .read()
            .unwrap_or_else(|error| error.into_inner())
            .values()
            .cloned()
            .collect())
    }

    async fn write_unschedulable(
        &self,
        _token: &LeadershipToken,
        _entries: &[UnschedulableReplica],
    ) -> Result<ReplaceOutcome> {
        Ok(ReplaceOutcome::Applied)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        cluster::types::{Assignment, LeaderInfo, LeadershipToken},
        deployment::types::DeploymentStatus,
    };

    fn token() -> LeadershipToken {
        LeadershipToken {
            info: LeaderInfo {
                node_id: "leader-node1".to_string(),
            },
            election_key: b"leader".to_vec(),
            create_revision: 1,
            lease_id: 1,
        }
    }

    fn assignment() -> Assignment {
        Assignment {
            assignment_id: "assignment-1".to_string(),
            placement_epoch: 1,
            service_id: "web".to_string(),
            deployment_id: "dep".to_string(),
            replica_index: 0,
            node_id: "worker-node1".to_string(),
            container_ip: None,
            replaces_assignment_id: None,
            created_at_ms: 1,
        }
    }

    #[tokio::test]
    async fn generation_cas_and_watcher_are_atomic() {
        let store = InMemoryAssignmentStore::default();
        let mut watcher = store.watch_node(&"worker-node1".to_string()).await.unwrap();
        let manifest = AssignmentManifest {
            node_id: "worker-node1".to_string(),
            generation: 0,
            assignments: vec![assignment()],
        };
        assert_eq!(
            store.replace_for_node(&token(), 0, manifest).await.unwrap(),
            ReplaceOutcome::Applied
        );
        watcher.changed().await.unwrap();
        assert_eq!(watcher.borrow().as_ref().unwrap().generation, 1);
        assert_eq!(
            store
                .replace_for_node(
                    &token(),
                    0,
                    AssignmentManifest {
                        node_id: "worker-node1".to_string(),
                        generation: 0,
                        assignments: Vec::new(),
                    },
                )
                .await
                .unwrap(),
            ReplaceOutcome::GenerationConflict
        );
    }

    #[tokio::test]
    async fn state_write_is_rejected_after_assignment_is_removed() {
        let store = InMemoryAssignmentStore::default();
        store
            .replace_for_node(
                &token(),
                0,
                AssignmentManifest {
                    node_id: "worker-node1".to_string(),
                    generation: 0,
                    assignments: vec![assignment()],
                },
            )
            .await
            .unwrap();
        let state = ReplicaState {
            service_id: Some("web".to_string()),
            deployment_id: Some("dep".to_string()),
            replica_index: 0,
            status: DeploymentStatus::Ready,
            healthcheck_failures: 0,
            restart_attempts: 0,
            node_id: None,
            assignment_id: None,
            endpoint: None,
            error: None,
        };
        assert!(
            store
                .upsert_replica_state_if_assignment(
                    &"worker-node1".to_string(),
                    "assignment-1",
                    state.clone(),
                    None,
                )
                .await
                .unwrap()
        );
        store
            .replace_for_node(
                &token(),
                1,
                AssignmentManifest {
                    node_id: "worker-node1".to_string(),
                    generation: 1,
                    assignments: Vec::new(),
                },
            )
            .await
            .unwrap();
        assert!(
            !store
                .upsert_replica_state_if_assignment(
                    &"worker-node1".to_string(),
                    "assignment-1",
                    state,
                    None,
                )
                .await
                .unwrap()
        );
    }
}
