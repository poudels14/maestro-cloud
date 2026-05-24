use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use etcd_client::{Client as EtcdClient, GetOptions, PutOptions};
use tokio::sync::Mutex;

use super::registry::NodeRegistry;
use super::types::{NodeId, NodeInfo};

pub const NODES_PREFIX: &str = "cluster/nodes/";
pub const KEEP_ALIVE_TTL_SECS: i64 = 15;

pub struct EtcdNodeRegistry {
    client: Arc<Mutex<EtcdClient>>,
    node_id: NodeId,
    lease_id: Mutex<Option<i64>>,
}

impl EtcdNodeRegistry {
    pub fn new(client: Arc<Mutex<EtcdClient>>, node_id: NodeId) -> Self {
        Self {
            client,
            node_id,
            lease_id: Mutex::new(None),
        }
    }

    fn key_for(node_id: &NodeId) -> String {
        format!("{NODES_PREFIX}{node_id}")
    }

    async fn ensure_lease(&self) -> Result<i64> {
        let mut guard = self.lease_id.lock().await;
        if let Some(id) = *guard {
            return Ok(id);
        }
        let mut client = self.client.lock().await;
        let response = client
            .lease_grant(KEEP_ALIVE_TTL_SECS, None)
            .await
            .map_err(|err| anyhow!("failed to grant lease for node registration: {err}"))?;
        let id = response.id();
        *guard = Some(id);
        Ok(id)
    }
}

#[async_trait]
impl NodeRegistry for EtcdNodeRegistry {
    async fn register(&self, info: &NodeInfo) -> Result<()> {
        if info.node_id != self.node_id {
            return Err(anyhow!(
                "registry initialized for node {} but registering {}",
                self.node_id,
                info.node_id
            ));
        }
        let lease = self.ensure_lease().await?;
        let key = Self::key_for(&info.node_id);
        let value = serde_json::to_vec(info)
            .map_err(|err| anyhow!("failed to serialize node info: {err}"))?;
        let mut client = self.client.lock().await;
        client
            .put(key, value, Some(PutOptions::new().with_lease(lease)))
            .await
            .map_err(|err| anyhow!("failed to put node registration: {err}"))?;
        Ok(())
    }

    async fn keep_alive(&self) -> Result<()> {
        let lease = match *self.lease_id.lock().await {
            Some(id) => id,
            None => return Err(anyhow!("keep_alive called before register")),
        };
        let mut client = self.client.lock().await;
        let (mut keeper, _stream) = client
            .lease_keep_alive(lease)
            .await
            .map_err(|err| anyhow!("failed to start lease keep-alive: {err}"))?;
        keeper
            .keep_alive()
            .await
            .map_err(|err| anyhow!("failed to send lease keep-alive: {err}"))?;
        Ok(())
    }

    async fn deregister(&self) -> Result<()> {
        let lease = self.lease_id.lock().await.take();
        if let Some(lease) = lease {
            let mut client = self.client.lock().await;
            let _ = tokio::time::timeout(Duration::from_secs(2), client.lease_revoke(lease)).await;
        }
        Ok(())
    }

    async fn list_nodes(&self) -> Result<Vec<NodeInfo>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(NODES_PREFIX, Some(GetOptions::new().with_prefix()))
            .await
            .map_err(|err| anyhow!("failed to list nodes: {err}"))?;
        let mut nodes = Vec::with_capacity(response.kvs().len());
        for kv in response.kvs() {
            match serde_json::from_slice::<NodeInfo>(kv.value()) {
                Ok(info) => nodes.push(info),
                Err(err) => {
                    eprintln!(
                        "cluster: skipping malformed node entry {:?}: {err}",
                        kv.key_str().unwrap_or("<unprintable>")
                    );
                }
            }
        }
        nodes.sort_by(|left, right| left.node_id.cmp(&right.node_id));
        Ok(nodes)
    }

    async fn get_node(&self, node_id: &NodeId) -> Result<Option<NodeInfo>> {
        let mut client = self.client.lock().await;
        let response = client
            .get(Self::key_for(node_id), None)
            .await
            .map_err(|err| anyhow!("failed to get node: {err}"))?;
        match response.kvs().first() {
            Some(kv) => {
                let info: NodeInfo = serde_json::from_slice(kv.value())
                    .map_err(|err| anyhow!("failed to parse node info: {err}"))?;
                Ok(Some(info))
            }
            None => Ok(None),
        }
    }

    async fn set_unschedulable(&self, unschedulable: bool) -> Result<()> {
        let lease = match *self.lease_id.lock().await {
            Some(id) => id,
            None => return Err(anyhow!("set_unschedulable called before register")),
        };
        let key = Self::key_for(&self.node_id);
        let mut client = self.client.lock().await;
        let response = client
            .get(key.as_str(), None)
            .await
            .map_err(|err| anyhow!("failed to read node info: {err}"))?;
        let kv = response
            .kvs()
            .first()
            .ok_or_else(|| anyhow!("node {} not found in registry", self.node_id))?;
        let mut info: NodeInfo = serde_json::from_slice(kv.value())
            .map_err(|err| anyhow!("failed to parse node info: {err}"))?;
        info.unschedulable = unschedulable;
        let value = serde_json::to_vec(&info)
            .map_err(|err| anyhow!("failed to serialize node info: {err}"))?;
        client
            .put(key, value, Some(PutOptions::new().with_lease(lease)))
            .await
            .map_err(|err| anyhow!("failed to update node info: {err}"))?;
        Ok(())
    }
}
