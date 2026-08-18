use std::collections::HashMap;
use std::sync::Arc;
use std::time::Duration;

#[cfg(test)]
use std::{collections::BTreeMap, sync::RwLock};

use anyhow::{Result, anyhow, bail};
use async_trait::async_trait;
use etcd_client::{Client, Compare, CompareOp, ConnectOptions, PutOptions, TlsOptions, Txn, TxnOp};
use tokio::sync::{Mutex, broadcast};

use crate::cluster::types::{LeadershipToken, NodeId, NodeInfo, NodeRecord, NodeState};
use crate::logs::Logger;

const DATA_PLANE_STALE_AFTER_MS: i64 = 15_000;
const DATA_PLANE_ALERT_AFTER_MS: i64 = 30_000;

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum NodeAvailabilityEvent {
    Down {
        node: NodeInfo,
        since_ms: i64,
    },
    Recovered {
        node: NodeInfo,
        since_ms: i64,
    },
    DataPlaneUnavailable {
        node: NodeInfo,
        since_ms: i64,
        reason: String,
    },
    DataPlaneRecovered {
        node: NodeInfo,
        since_ms: i64,
    },
}

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
        live_nodes: &[NodeInfo],
        now_ms: i64,
    ) -> Result<Vec<NodeAvailabilityEvent>>;
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
        let subnet_key = format!("/maetro/cluster/subnets/{}", self.node_id);
        let value = serde_json::to_vec(info)?;
        let mut client = self.client.lock().await;
        let previous_record_response = client.get(record_key.clone(), None).await?;
        let previous_record_compare = previous_record_response.kvs().first().map_or_else(
            || Compare::version(record_key.clone(), CompareOp::Equal, 0),
            |entry| Compare::value(record_key.clone(), CompareOp::Equal, entry.value()),
        );
        let previous_record = previous_record_response
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice::<NodeRecord>(entry.value()))
            .transpose()?;
        let registered_at_ms = now_millis();
        let record = serde_json::to_vec(&NodeRecord {
            last_info: info.clone(),
            last_seen_at_ms: registered_at_ms,
            lost_at_ms: None,
            data_plane_lost_at_ms: if info.data_plane_ready {
                None
            } else {
                previous_record
                    .as_ref()
                    .and_then(|record| record.data_plane_lost_at_ms)
                    .or(Some(registered_at_ms))
            },
            control_plane_alerted_at_ms: previous_record
                .as_ref()
                .and_then(|record| record.control_plane_alerted_at_ms),
            data_plane_alerted_at_ms: previous_record
                .as_ref()
                .and_then(|record| record.data_plane_alerted_at_ms),
        })?;
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
                previous_record_compare,
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
        let client = self.client.lock().await;
        crate::utils::etcd::get_prefix(&client, "/maetro/cluster/nodes/", false, None)
            .await?
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
        live_nodes: &[NodeInfo],
        now_ms: i64,
    ) -> Result<Vec<NodeAvailabilityEvent>> {
        let live = live_nodes
            .iter()
            .map(|node| (node.node_id.as_str(), node))
            .collect::<HashMap<_, _>>();
        let mut events = Vec::new();
        let mut client = self.client.lock().await;
        let entries =
            crate::utils::etcd::get_prefix(&client, "/maetro/cluster/node-records/", false, None)
                .await?;
        for entry in &entries {
            let mut record: NodeRecord = serde_json::from_slice(entry.value())?;
            let previous = record.clone();
            let node_id = record.last_info.node_id.clone();
            let node_events = reconcile_availability_record(
                &mut record,
                live.get(node_id.as_str()).copied(),
                now_ms,
            );
            if record == previous {
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
            events.extend(node_events);
        }
        Ok(events)
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
        let previous_record_compare = previous_record.kvs().first().map_or_else(
            || Compare::version(record_key.clone(), CompareOp::Equal, 0),
            |entry| Compare::value(record_key.clone(), CompareOp::Equal, entry.value()),
        );
        let previous_record = previous_record
            .kvs()
            .first()
            .map(|entry| serde_json::from_slice::<NodeRecord>(entry.value()))
            .transpose()?;
        let record = serde_json::to_vec(&NodeRecord {
            last_info: info,
            last_seen_at_ms: checked_at_ms,
            lost_at_ms: None,
            data_plane_lost_at_ms: if ready {
                None
            } else {
                previous_record
                    .as_ref()
                    .and_then(|record| record.data_plane_lost_at_ms)
                    .or(Some(checked_at_ms))
            },
            control_plane_alerted_at_ms: previous_record
                .as_ref()
                .and_then(|record| record.control_plane_alerted_at_ms),
            data_plane_alerted_at_ms: previous_record
                .as_ref()
                .and_then(|record| record.data_plane_alerted_at_ms),
        })?;
        let transaction = Txn::new()
            .when([
                Compare::value(key.clone(), CompareOp::Equal, existing_value),
                Compare::lease(key.clone(), CompareOp::Equal, lease_id),
                previous_record_compare,
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

fn reconcile_availability_record(
    record: &mut NodeRecord,
    live: Option<&NodeInfo>,
    now_ms: i64,
) -> Vec<NodeAvailabilityEvent> {
    let mut events = Vec::new();
    let Some(live) = live else {
        let lost_at_ms = *record.lost_at_ms.get_or_insert(now_ms);
        if record.control_plane_alerted_at_ms.is_none() {
            record.control_plane_alerted_at_ms = Some(lost_at_ms);
            events.push(NodeAvailabilityEvent::Down {
                node: record.last_info.clone(),
                since_ms: lost_at_ms,
            });
        }
        record.data_plane_lost_at_ms = None;
        record.data_plane_alerted_at_ms = None;
        return events;
    };

    record.lost_at_ms = None;
    if let Some(since_ms) = record.control_plane_alerted_at_ms.take() {
        events.push(NodeAvailabilityEvent::Recovered {
            node: live.clone(),
            since_ms,
        });
    }

    let unavailable = if !live.data_plane_ready {
        Some(
            record
                .data_plane_lost_at_ms
                .or((live.data_plane_checked_at_ms > 0).then_some(live.data_plane_checked_at_ms))
                .unwrap_or(now_ms),
        )
    } else if live.data_plane_checked_at_ms <= 0 {
        Some(record.data_plane_lost_at_ms.unwrap_or(now_ms))
    } else if now_ms.saturating_sub(live.data_plane_checked_at_ms) > DATA_PLANE_STALE_AFTER_MS {
        Some(record.data_plane_lost_at_ms.unwrap_or_else(|| {
            live.data_plane_checked_at_ms
                .saturating_add(DATA_PLANE_STALE_AFTER_MS)
        }))
    } else {
        None
    };
    record.data_plane_lost_at_ms = unavailable;

    match unavailable {
        Some(since_ms)
            if now_ms.saturating_sub(since_ms) >= DATA_PLANE_ALERT_AFTER_MS
                && record.data_plane_alerted_at_ms.is_none() =>
        {
            record.data_plane_alerted_at_ms = Some(since_ms);
            let reason = live
                .data_plane_error
                .clone()
                .filter(|reason| !reason.trim().is_empty())
                .unwrap_or_else(|| {
                    if live.data_plane_ready {
                        "data-plane health updates are stale".to_string()
                    } else {
                        "node gateway health check failed".to_string()
                    }
                });
            events.push(NodeAvailabilityEvent::DataPlaneUnavailable {
                node: live.clone(),
                since_ms,
                reason,
            });
        }
        None => {
            if let Some(since_ms) = record.data_plane_alerted_at_ms.take() {
                events.push(NodeAvailabilityEvent::DataPlaneRecovered {
                    node: live.clone(),
                    since_ms,
                });
            }
        }
        Some(_) => {}
    }
    events
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
        _live_nodes: &[NodeInfo],
        _now_ms: i64,
    ) -> Result<Vec<NodeAvailabilityEvent>> {
        Ok(Vec::new())
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

    fn node_record(info: NodeInfo) -> NodeRecord {
        NodeRecord {
            last_info: info,
            last_seen_at_ms: 1,
            lost_at_ms: None,
            data_plane_lost_at_ms: None,
            control_plane_alerted_at_ms: None,
            data_plane_alerted_at_ms: None,
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

    #[test]
    fn control_plane_loss_and_recovery_emit_once_per_transition() {
        let mut live = node_info("boot-1");
        live.data_plane_ready = true;
        live.data_plane_checked_at_ms = 100;
        let mut record = node_record(live.clone());

        let down = reconcile_availability_record(&mut record, None, 1_000);
        assert!(matches!(
            down.as_slice(),
            [NodeAvailabilityEvent::Down {
                since_ms: 1_000,
                ..
            }]
        ));
        assert!(reconcile_availability_record(&mut record, None, 2_000).is_empty());

        let recovered = reconcile_availability_record(&mut record, Some(&live), 5_000);
        assert!(matches!(
            recovered.as_slice(),
            [NodeAvailabilityEvent::Recovered {
                since_ms: 1_000,
                ..
            }]
        ));
        assert!(reconcile_availability_record(&mut record, Some(&live), 6_000).is_empty());
    }

    #[test]
    fn data_plane_alert_waits_for_grace_and_recovers_once() {
        let mut unavailable = node_info("boot-1");
        unavailable.data_plane_checked_at_ms = 1_000;
        unavailable.data_plane_error = Some("gateway timed out".to_string());
        let mut record = node_record(unavailable.clone());

        assert!(reconcile_availability_record(&mut record, Some(&unavailable), 30_999).is_empty());
        let alert = reconcile_availability_record(&mut record, Some(&unavailable), 31_000);
        assert!(matches!(
            alert.as_slice(),
            [NodeAvailabilityEvent::DataPlaneUnavailable {
                since_ms: 1_000,
                reason,
                ..
            }] if reason == "gateway timed out"
        ));
        assert!(reconcile_availability_record(&mut record, Some(&unavailable), 35_000).is_empty());

        let mut recovered = unavailable.clone();
        recovered.data_plane_ready = true;
        recovered.data_plane_checked_at_ms = 36_000;
        recovered.data_plane_error = None;
        let recovery = reconcile_availability_record(&mut record, Some(&recovered), 36_000);
        assert!(matches!(
            recovery.as_slice(),
            [NodeAvailabilityEvent::DataPlaneRecovered {
                since_ms: 1_000,
                ..
            }]
        ));
        assert!(reconcile_availability_record(&mut record, Some(&recovered), 37_000).is_empty());
    }

    #[test]
    fn stale_data_plane_updates_become_an_unavailable_incident() {
        let mut live = node_info("boot-1");
        live.data_plane_ready = true;
        live.data_plane_checked_at_ms = 1_000;
        let mut record = node_record(live.clone());

        assert!(reconcile_availability_record(&mut record, Some(&live), 45_999).is_empty());
        let alert = reconcile_availability_record(&mut record, Some(&live), 46_000);
        assert!(matches!(
            alert.as_slice(),
            [NodeAvailabilityEvent::DataPlaneUnavailable {
                since_ms: 16_000,
                reason,
                ..
            }] if reason == "data-plane health updates are stale"
        ));
    }

    #[test]
    fn legacy_node_records_default_alert_markers() {
        let value = serde_json::json!({
            "lastInfo": node_info("boot-1"),
            "lastSeenAtMs": 1,
            "lostAtMs": null,
            "dataPlaneLostAtMs": null
        });
        let record: NodeRecord = serde_json::from_value(value).expect("legacy node record");
        assert_eq!(record.control_plane_alerted_at_ms, None);
        assert_eq!(record.data_plane_alerted_at_ms, None);
    }
}
