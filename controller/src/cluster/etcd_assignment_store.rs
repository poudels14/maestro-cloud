use std::sync::Arc;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use etcd_client::{Client as EtcdClient, Compare, CompareOp, GetOptions, Txn, TxnOp, WatchOptions};
use tokio::sync::{Mutex, mpsc};

use super::assignment_store::{ASSIGNMENTS_PREFIX, AssignmentStore};
use super::scheduling::Assignment;
use super::types::NodeId;

pub struct EtcdAssignmentStore {
    client: Arc<Mutex<EtcdClient>>,
}

impl EtcdAssignmentStore {
    pub fn new(client: Arc<Mutex<EtcdClient>>) -> Self {
        Self { client }
    }

    /// Watch the per-node prefix for changes. Sends a notification (the unit
    /// value) whenever an assignment is added or removed under
    /// `cluster/assignments/{node_id}/` so the reconciler can react instantly
    /// without waiting for the next poll tick. The watcher reconnects on
    /// disconnect with a short backoff.
    pub async fn watch_for_node(&self, node_id: &NodeId) -> mpsc::Receiver<()> {
        let (tx, rx) = mpsc::channel(8);
        let prefix = Self::node_prefix(node_id);
        let client = Arc::clone(&self.client);
        tokio::spawn(async move {
            loop {
                let watch_result = {
                    let mut etcd = client.lock().await;
                    etcd.watch(prefix.as_bytes(), Some(WatchOptions::new().with_prefix()))
                        .await
                };
                let mut stream = match watch_result {
                    Ok((_watcher, stream)) => stream,
                    Err(err) => {
                        eprintln!("assignment watcher: failed to start: {err}");
                        tokio::time::sleep(std::time::Duration::from_secs(2)).await;
                        continue;
                    }
                };
                loop {
                    match stream.message().await {
                        Ok(Some(_resp)) => {
                            if tx.send(()).await.is_err() {
                                return;
                            }
                        }
                        Ok(None) | Err(_) => break,
                    }
                }
                tokio::time::sleep(std::time::Duration::from_secs(1)).await;
            }
        });
        rx
    }

    fn node_prefix(node_id: &NodeId) -> String {
        format!("{ASSIGNMENTS_PREFIX}{node_id}/")
    }

    fn assignment_key(assignment: &Assignment) -> String {
        format!(
            "{}{}/{}",
            Self::node_prefix(&assignment.node_id),
            assignment.service_id,
            assignment.replica_index
        )
    }
}

#[async_trait]
impl AssignmentStore for EtcdAssignmentStore {
    async fn replace_for_node(&self, node_id: &NodeId, assignments: &[Assignment]) -> Result<()> {
        let prefix = Self::node_prefix(node_id);
        let mut client = self.client.lock().await;
        let existing = client
            .get(prefix.as_str(), Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| anyhow!("failed to read assignments for {node_id}: {err}"))?;
        // Etcd rejects a txn that has both a DELETE and a PUT for the same
        // key, so we only DELETE keys that aren't being replaced by a PUT.
        // The PUTs themselves overwrite anything already at that key.
        let new_keys: std::collections::HashSet<Vec<u8>> = assignments
            .iter()
            .map(|assignment| Self::assignment_key(assignment).into_bytes())
            .collect();
        let mut ops: Vec<TxnOp> = Vec::new();
        for kv in existing.kvs() {
            let key_bytes = kv.key().to_vec();
            if !new_keys.contains(&key_bytes) {
                ops.push(TxnOp::delete(key_bytes, None));
            }
        }
        for assignment in assignments {
            let key = Self::assignment_key(assignment);
            let value = serde_json::to_vec(assignment)
                .map_err(|err| anyhow!("failed to serialize assignment: {err}"))?;
            ops.push(TxnOp::put(key, value, None));
        }
        let txn = Txn::new()
            .when([Compare::version(prefix.as_str(), CompareOp::Greater, -1)])
            .and_then(ops);
        client
            .txn(txn)
            .await
            .map_err(|err| anyhow!("failed to replace assignments for {node_id}: {err}"))?;
        Ok(())
    }

    async fn list_for_node(&self, node_id: &NodeId) -> Result<Vec<Assignment>> {
        let prefix = Self::node_prefix(node_id);
        let mut client = self.client.lock().await;
        let response = client
            .get(prefix.as_str(), Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| anyhow!("failed to list assignments for {node_id}: {err}"))?;
        decode_assignments(response.kvs())
    }

    async fn list_all(&self) -> Result<Vec<Assignment>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(ASSIGNMENTS_PREFIX, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| anyhow!("failed to list all assignments: {err}"))?;
        decode_assignments(response.kvs())
    }
}

fn decode_assignments(kvs: &[etcd_client::KeyValue]) -> Result<Vec<Assignment>> {
    let mut assignments = Vec::with_capacity(kvs.len());
    for kv in kvs {
        match serde_json::from_slice::<Assignment>(kv.value()) {
            Ok(assignment) => assignments.push(assignment),
            Err(err) => eprintln!(
                "cluster: skipping malformed assignment {:?}: {err}",
                kv.key_str().unwrap_or("<unprintable>")
            ),
        }
    }
    assignments.sort_by(|left, right| {
        left.service_id
            .cmp(&right.service_id)
            .then_with(|| left.replica_index.cmp(&right.replica_index))
    });
    Ok(assignments)
}
