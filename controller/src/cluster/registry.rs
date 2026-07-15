use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use std::{collections::BTreeMap, sync::RwLock};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use etcd_client::{
    Client, Compare, CompareOp, ConnectOptions, GetOptions, PutOptions, TlsOptions, Txn, TxnOp,
};
use tokio::sync::{Mutex, broadcast};

use crate::cluster::types::{LeadershipToken, NodeId, NodeInfo, NodeRecord, NodeState};
use crate::logs::Logger;

#[async_trait]
pub trait NodeRegistry: Send + Sync {
    async fn register(&self, info: NodeInfo) -> Result<()>;
    async fn deregister(&self) -> Result<()>;
    async fn list_nodes(&self) -> Result<Vec<NodeInfo>>;
    async fn get_node_state(&self, node_id: &NodeId) -> Result<NodeState>;
    async fn set_node_state(
        &self,
        token: &LeadershipToken,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<()>;
    async fn reconcile_liveness(
        &self,
        token: &LeadershipToken,
        live_node_ids: &[NodeId],
        now_ms: i64,
    ) -> Result<()>;
    async fn update_data_plane_status(
        &self,
        instance_id: &str,
        ready: bool,
        checked_at_ms: i64,
        error: Option<String>,
    ) -> Result<()>;
}

pub struct EtcdNodeRegistry {
    client: Arc<Mutex<Client>>,
    node_id: NodeId,
    lease_id: Mutex<Option<i64>>,
}

impl EtcdNodeRegistry {
    pub async fn connect(
        endpoints: &[String],
        tls: Option<TlsOptions>,
        node_id: NodeId,
    ) -> Result<Self> {
        let options = tls.map(|tls| ConnectOptions::new().with_tls(tls));
        let client = Client::connect(endpoints, options).await?;
        Ok(Self {
            client: Arc::new(Mutex::new(client)),
            node_id,
            lease_id: Mutex::new(None),
        })
    }

    pub fn spawn(
        self: Arc<Self>,
        info: NodeInfo,
        mut shutdown: broadcast::Receiver<crate::signal::ShutdownEvent>,
        logger: Logger,
    ) -> tokio::task::JoinHandle<()> {
        tokio::spawn(async move {
            let mut delay = Duration::from_secs(1);
            loop {
                if let Err(err) = self.register(info.clone()).await {
                    logger.emit("error", &format!("cluster node registration failed: {err}"));
                    tokio::select! {
                        _ = tokio::time::sleep(delay) => {}
                        _ = shutdown.recv() => break,
                    }
                    delay = (delay * 2).min(Duration::from_secs(15));
                    continue;
                }
                delay = Duration::from_secs(1);
                let lease_id = self.lease_id.lock().await.expect("registered lease");
                let keep_alive = {
                    let mut client = self.client.lock().await;
                    client.lease_keep_alive(lease_id).await
                };
                let Ok((mut keeper, mut stream)) = keep_alive else {
                    continue;
                };
                let heartbeat = async {
                    loop {
                        tokio::time::sleep(Duration::from_secs(5)).await;
                        keeper.keep_alive().await?;
                        let response =
                            tokio::time::timeout(Duration::from_secs(5), stream.message())
                                .await
                                .map_err(|_| anyhow!("node lease response timed out"))??;
                        if response.is_none_or(|response| response.ttl() <= 0) {
                            bail!("node lease expired");
                        }
                    }
                    #[allow(unreachable_code)]
                    Ok::<(), anyhow::Error>(())
                };
                tokio::select! {
                    result = heartbeat => {
                        if let Err(err) = result {
                            logger.emit("warn", &format!("cluster heartbeat interrupted: {err}"));
                        }
                    }
                    _ = shutdown.recv() => {
                        let _ = self.deregister().await;
                        break;
                    }
                }
            }
        })
    }

    async fn put_registration(&self, info: &NodeInfo, lease_id: i64) -> Result<bool> {
        let key = node_key(&self.node_id);
        let record_key = node_record_key(&self.node_id);
        let control_key = format!(
            "/maetro/cluster/control-addresses/{:08x}",
            u32::from(info.cluster_host_ip)
        );
        let subnet_key = format!(
            "/maetro/cluster/subnets/{}",
            info.subnet.replace('.', "-").replace('/', "_")
        );
        let value = serde_json::to_vec(info)?;
        let record = serde_json::to_vec(&NodeRecord {
            last_info: info.clone(),
            last_seen_at_ms: now_millis(),
            lost_at_ms: None,
            data_plane_lost_at_ms: None,
        })?;
        let mut client = self.client.lock().await;
        let control = reservation(&mut client, &control_key, &self.node_id).await?;
        let subnet = reservation(&mut client, &subnet_key, &self.node_id).await?;
        let active_control = serde_json::to_vec(&serde_json::json!({
            "hostIp": info.cluster_host_ip,
            "nodeId": self.node_id,
            "state": "active"
        }))?;
        let active_subnet = serde_json::to_vec(&serde_json::json!({
            "cidr": info.subnet,
            "nodeId": self.node_id,
            "state": "active"
        }))?;
        let transaction = Txn::new()
            .when([
                Compare::version(key.clone(), CompareOp::Equal, 0),
                Compare::value(control_key.clone(), CompareOp::Equal, control),
                Compare::value(subnet_key.clone(), CompareOp::Equal, subnet),
            ])
            .and_then([
                TxnOp::put(key, value, Some(PutOptions::new().with_lease(lease_id))),
                TxnOp::put(record_key, record, None),
                TxnOp::put(control_key, active_control, None),
                TxnOp::put(subnet_key, active_subnet, None),
            ]);
        Ok(client.txn(transaction).await?.succeeded())
    }

    async fn replace_own_registration(&self, info: &NodeInfo, lease_id: i64) -> Result<()> {
        let key = node_key(&self.node_id);
        let mut client = self.client.lock().await;
        let response = client.get(key.clone(), None).await?;
        let existing = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("node registration disappeared during retry"))?;
        let existing_info: NodeInfo = serde_json::from_slice(existing.value())?;
        if existing_info.instance_id != info.instance_id {
            bail!(
                "node id `{}` is already registered by another live daemon instance",
                self.node_id
            );
        }
        let value = serde_json::to_vec(info)?;
        let transaction = Txn::new()
            .when([Compare::value(
                key.clone(),
                CompareOp::Equal,
                existing.value(),
            )])
            .and_then([TxnOp::put(
                key,
                value,
                Some(PutOptions::new().with_lease(lease_id)),
            )]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("node registration changed concurrently");
        }
        Ok(())
    }
}

#[async_trait]
impl NodeRegistry for EtcdNodeRegistry {
    async fn register(&self, info: NodeInfo) -> Result<()> {
        if info.node_id != self.node_id {
            bail!("registry node identity does not match NodeInfo");
        }
        if !self
            .client
            .lock()
            .await
            .get(format!("/maetro/cluster/removed/{}", self.node_id), None)
            .await?
            .kvs()
            .is_empty()
        {
            bail!(
                "node `{}` was removed from the cluster; wipe its cluster identity before rejoining",
                self.node_id
            );
        }
        let lease_id = self.client.lock().await.lease_grant(15, None).await?.id();
        if !self.put_registration(&info, lease_id).await? {
            self.replace_own_registration(&info, lease_id).await?;
        }
        *self.lease_id.lock().await = Some(lease_id);
        Ok(())
    }

    async fn deregister(&self) -> Result<()> {
        if let Some(lease_id) = self.lease_id.lock().await.take() {
            self.client.lock().await.lease_revoke(lease_id).await?;
        }
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let response = self
            .client
            .lock()
            .await
            .get(
                "/maetro/cluster/nodes/",
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
        response
            .kvs()
            .iter()
            .map(|entry| serde_json::from_slice(entry.value()).map_err(Into::into))
            .collect()
    }

    async fn get_node_state(&self, node_id: &NodeId) -> Result<NodeState> {
        let response = self
            .client
            .lock()
            .await
            .get(node_state_key(node_id), None)
            .await?;
        match response.kvs().first() {
            Some(entry) => Ok(serde_json::from_slice(entry.value())?),
            None => Ok(NodeState::default()),
        }
    }

    async fn set_node_state(
        &self,
        token: &LeadershipToken,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<()> {
        let transaction = Txn::new()
            .when([Compare::create_revision(
                token.election_key.clone(),
                CompareOp::Equal,
                token.create_revision,
            )])
            .and_then([TxnOp::put(
                node_state_key(node_id),
                serde_json::to_vec(&state)?,
                None,
            )]);
        if !self.client.lock().await.txn(transaction).await?.succeeded() {
            bail!("leadership fence rejected stale node-state write");
        }
        Ok(())
    }

    async fn reconcile_liveness(
        &self,
        token: &LeadershipToken,
        live_node_ids: &[NodeId],
        now_ms: i64,
    ) -> Result<()> {
        let live = live_node_ids
            .iter()
            .collect::<std::collections::HashSet<_>>();
        let mut client = self.client.lock().await;
        let response = client
            .get(
                "/maetro/cluster/node-records/",
                Some(GetOptions::new().with_prefix()),
            )
            .await?;
        for entry in response.kvs() {
            let mut record: NodeRecord = serde_json::from_slice(entry.value())?;
            let is_live = live.contains(&record.last_info.node_id);
            let changed = if is_live {
                if record.lost_at_ms.is_some() {
                    record.lost_at_ms = None;
                    true
                } else {
                    false
                }
            } else if record.lost_at_ms.is_none() {
                record.lost_at_ms = Some(now_ms);
                true
            } else {
                false
            };
            if !changed {
                continue;
            }
            let transaction = Txn::new()
                .when([
                    Compare::create_revision(
                        token.election_key.clone(),
                        CompareOp::Equal,
                        token.create_revision,
                    ),
                    Compare::value(entry.key(), CompareOp::Equal, entry.value()),
                ])
                .and_then([TxnOp::put(entry.key(), serde_json::to_vec(&record)?, None)]);
            if !client.txn(transaction).await?.succeeded() {
                bail!("leadership or node-record CAS rejected liveness reconciliation");
            }
        }
        Ok(())
    }

    async fn update_data_plane_status(
        &self,
        instance_id: &str,
        ready: bool,
        checked_at_ms: i64,
        error: Option<String>,
    ) -> Result<()> {
        let key = node_key(&self.node_id);
        let record_key = node_record_key(&self.node_id);
        let lease_id =
            (*self.lease_id.lock().await).ok_or_else(|| anyhow!("node is not registered"))?;
        let mut client = self.client.lock().await;
        let response = client.get(key.clone(), None).await?;
        let existing = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("live node registration is absent"))?;
        let existing_value = existing.value().to_vec();
        let mut info: NodeInfo = serde_json::from_slice(existing.value())?;
        if info.instance_id != instance_id {
            bail!("node registration belongs to a different daemon instance");
        }
        info.data_plane_ready = ready;
        info.data_plane_checked_at_ms = checked_at_ms;
        info.data_plane_error = error;
        let value = serde_json::to_vec(&info)?;
        let previous_record = client.get(record_key.clone(), None).await?;
        let previous_data_plane_lost_at = previous_record
            .kvs()
            .first()
            .and_then(|entry| serde_json::from_slice::<NodeRecord>(entry.value()).ok())
            .and_then(|record| record.data_plane_lost_at_ms);
        let record = serde_json::to_vec(&NodeRecord {
            last_info: info,
            last_seen_at_ms: checked_at_ms,
            lost_at_ms: None,
            data_plane_lost_at_ms: if ready {
                None
            } else {
                previous_data_plane_lost_at.or(Some(checked_at_ms))
            },
        })?;
        let transaction = Txn::new()
            .when([
                Compare::value(key.clone(), CompareOp::Equal, existing_value),
                Compare::lease(key.clone(), CompareOp::Equal, lease_id),
            ])
            .and_then([
                TxnOp::put(key, value, Some(PutOptions::new().with_lease(lease_id))),
                TxnOp::put(record_key, record, None),
            ]);
        if !client.txn(transaction).await?.succeeded() {
            bail!("node registration changed during data-plane update");
        }
        Ok(())
    }
}

fn node_key(node_id: &str) -> String {
    format!("/maetro/cluster/nodes/{node_id}")
}

fn node_record_key(node_id: &str) -> String {
    format!("/maetro/cluster/node-records/{node_id}")
}

fn node_state_key(node_id: &str) -> String {
    format!("/maetro/cluster/node-state/{node_id}")
}

fn now_millis() -> i64 {
    crate::utils::time::current_time_millis()
        .ok()
        .and_then(|value| i64::try_from(value).ok())
        .unwrap_or_default()
}

async fn reservation(client: &mut Client, key: &str, node_id: &str) -> Result<Vec<u8>> {
    let response = client.get(key, None).await?;
    let entry = response
        .kvs()
        .first()
        .ok_or_else(|| anyhow!("cluster resource `{key}` is not reserved"))?;
    let value: serde_json::Value = serde_json::from_slice(entry.value())?;
    if value
        .get("nodeId")
        .and_then(serde_json::Value::as_str)
        .is_some_and(|claimed| claimed != node_id)
    {
        bail!("cluster resource `{key}` is already claimed by another node");
    }
    Ok(entry.value().to_vec())
}

#[cfg(test)]
pub struct InMemoryNodeRegistry {
    node_id: NodeId,
    nodes: RwLock<BTreeMap<NodeId, NodeInfo>>,
    states: RwLock<BTreeMap<NodeId, NodeState>>,
}

#[cfg(test)]
impl InMemoryNodeRegistry {
    pub fn new(node_id: NodeId) -> Self {
        Self {
            node_id,
            nodes: RwLock::new(BTreeMap::new()),
            states: RwLock::new(BTreeMap::new()),
        }
    }

    pub fn insert_for_test(&self, info: NodeInfo) {
        self.nodes
            .write()
            .unwrap_or_else(|error| error.into_inner())
            .insert(info.node_id.clone(), info);
    }
}

#[cfg(test)]
#[async_trait]
impl NodeRegistry for InMemoryNodeRegistry {
    async fn register(&self, info: NodeInfo) -> Result<()> {
        if info.node_id != self.node_id {
            bail!("registry node identity does not match NodeInfo");
        }
        let mut nodes = self.nodes.write().unwrap_or_else(|err| err.into_inner());
        if nodes
            .get(&self.node_id)
            .is_some_and(|existing| existing.instance_id != info.instance_id)
        {
            bail!("node id is already registered by another live daemon instance");
        }
        nodes.insert(self.node_id.clone(), info);
        Ok(())
    }

    async fn deregister(&self) -> Result<()> {
        self.nodes
            .write()
            .unwrap_or_else(|err| err.into_inner())
            .remove(&self.node_id);
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        Ok(self
            .nodes
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .values()
            .cloned()
            .collect())
    }

    async fn get_node_state(&self, node_id: &NodeId) -> Result<NodeState> {
        Ok(self
            .states
            .read()
            .unwrap_or_else(|err| err.into_inner())
            .get(node_id)
            .cloned()
            .unwrap_or_default())
    }

    async fn set_node_state(
        &self,
        _token: &LeadershipToken,
        node_id: &NodeId,
        state: NodeState,
    ) -> Result<()> {
        self.states
            .write()
            .unwrap_or_else(|err| err.into_inner())
            .insert(node_id.clone(), state);
        Ok(())
    }

    async fn reconcile_liveness(
        &self,
        _token: &LeadershipToken,
        _live_node_ids: &[NodeId],
        _now_ms: i64,
    ) -> Result<()> {
        Ok(())
    }

    async fn update_data_plane_status(
        &self,
        instance_id: &str,
        ready: bool,
        checked_at_ms: i64,
        error: Option<String>,
    ) -> Result<()> {
        let mut nodes = self.nodes.write().unwrap_or_else(|err| err.into_inner());
        let info = nodes
            .get_mut(&self.node_id)
            .ok_or_else(|| anyhow!("node is not registered"))?;
        if info.instance_id != instance_id {
            bail!("node registration belongs to a different daemon instance");
        }
        info.data_plane_ready = ready;
        info.data_plane_checked_at_ms = checked_at_ms;
        info.data_plane_error = error;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::net::Ipv4Addr;

    use super::*;
    use crate::cluster::types::NodeRole;

    fn node_info(instance_id: &str) -> NodeInfo {
        NodeInfo {
            node_id: "node00000001".to_string(),
            instance_id: instance_id.to_string(),
            hostname: "node-1".to_string(),
            role: NodeRole::Voter,
            scheduling: true,
            cluster_host_ip: Ipv4Addr::new(10, 20, 0, 11),
            cluster_api_port: 3001,
            cluster_gateway_port: 3002,
            subnet: "172.22.1.0/24".to_string(),
            tailscale_ip: None,
            data_plane_ready: false,
            data_plane_checked_at_ms: 0,
            data_plane_error: None,
            version: "test".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        }
    }

    #[tokio::test]
    async fn duplicate_live_instance_is_rejected() {
        let registry = InMemoryNodeRegistry::new("node00000001".to_string());
        registry
            .register(node_info("boot-1"))
            .await
            .expect("register");
        assert!(registry.register(node_info("boot-2")).await.is_err());
    }

    #[tokio::test]
    async fn stale_data_plane_monitor_is_rejected() {
        let registry = InMemoryNodeRegistry::new("node00000001".to_string());
        registry
            .register(node_info("boot-1"))
            .await
            .expect("register");
        assert!(
            registry
                .update_data_plane_status("boot-old", true, 100, None)
                .await
                .is_err()
        );
        registry
            .update_data_plane_status("boot-1", true, 100, None)
            .await
            .expect("current monitor");
        assert!(registry.list_nodes().await.expect("nodes")[0].data_plane_ready);
    }
}
