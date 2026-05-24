use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;
use etcd_client::Client as EtcdClient;
use tokio::sync::{Mutex, watch};
use tokio::task::JoinHandle;

use super::elector::LeaderElector;
use super::types::{LeaderInfo, LeadershipState, NodeId};
use crate::utils::time::current_time_millis;

pub const ELECTION_NAME: &str = "cluster/leader";
pub const LEADER_LEASE_TTL_SECS: i64 = 10;
const OBSERVE_RECONNECT_DELAY: Duration = Duration::from_secs(1);

pub struct EtcdLeaderElector {
    client: Arc<Mutex<EtcdClient>>,
    node_id: NodeId,
    election_name: String,
    state_tx: watch::Sender<LeadershipState>,
    state_rx: watch::Receiver<LeadershipState>,
    /// Holds the lease backing the current campaign + the keep-alive task that
    /// extends it. Kept here so [`resign`] (or process shutdown via drop) can
    /// revoke the lease and abort the task. Without this, the keep-alive task
    /// would be orphaned (or worse, aborted prematurely) and leadership would
    /// silently expire after [`LEADER_LEASE_TTL_SECS`].
    active_campaign: Mutex<Option<CampaignHandle>>,
}

struct CampaignHandle {
    lease_id: i64,
    keep_alive_task: JoinHandle<()>,
}

impl EtcdLeaderElector {
    pub fn new(client: Arc<Mutex<EtcdClient>>, node_id: NodeId) -> Self {
        let (state_tx, state_rx) = watch::channel(LeadershipState::Unknown);
        Self {
            client,
            node_id,
            election_name: ELECTION_NAME.to_string(),
            state_tx,
            state_rx,
            active_campaign: Mutex::new(None),
        }
    }

    pub fn with_election_name(mut self, name: impl Into<String>) -> Self {
        self.election_name = name.into();
        self
    }

    pub async fn spawn_observer(self: &Arc<Self>) {
        let this = Arc::clone(self);
        tokio::spawn(async move {
            loop {
                let mut observe_client = {
                    let guard = this.client.lock().await;
                    guard.clone()
                };
                let stream = observe_client
                    .observe(this.election_name.as_bytes().to_vec())
                    .await;
                match stream {
                    Ok(mut stream) => loop {
                        match stream.message().await {
                            Ok(Some(response)) => {
                                let leader_id = response
                                    .kv()
                                    .and_then(|kv| std::str::from_utf8(kv.value()).ok())
                                    .map(|value| value.to_string());
                                this.update_leader_observed(leader_id);
                            }
                            Ok(None) | Err(_) => break,
                        }
                    },
                    Err(_) => {}
                }
                tokio::time::sleep(OBSERVE_RECONNECT_DELAY).await;
            }
        });
    }

    fn update_leader_observed(&self, leader_id: Option<String>) {
        let now_ms = current_time_millis().unwrap_or(0);
        let next = match leader_id {
            Some(id) if id == self.node_id => LeadershipState::Leading(LeaderInfo {
                node_id: id,
                elected_at_ms: now_ms,
            }),
            Some(id) => LeadershipState::Following(Some(LeaderInfo {
                node_id: id,
                elected_at_ms: now_ms,
            })),
            None => LeadershipState::Following(None),
        };
        let _ = self.state_tx.send(next);
    }
}

impl Drop for EtcdLeaderElector {
    fn drop(&mut self) {
        if let Ok(mut guard) = self.active_campaign.try_lock() {
            if let Some(handle) = guard.take() {
                handle.keep_alive_task.abort();
            }
        }
    }
}

#[async_trait]
impl LeaderElector for EtcdLeaderElector {
    async fn campaign(&self) -> Result<()> {
        let lease_id = {
            let mut client = self.client.lock().await;
            let response = client
                .lease_grant(LEADER_LEASE_TTL_SECS, None)
                .await
                .map_err(|err| anyhow!("failed to grant leader lease: {err}"))?;
            response.id()
        };
        let (mut keeper, mut keeper_stream) = {
            let mut client = self.client.lock().await;
            client
                .lease_keep_alive(lease_id)
                .await
                .map_err(|err| anyhow!("failed to start lease keep-alive: {err}"))?
        };
        let state_tx_for_keepalive = self.state_tx.clone();
        let keep_alive_task = tokio::spawn(async move {
            let interval = Duration::from_secs(((LEADER_LEASE_TTL_SECS as u64).max(2)) / 2);
            let mut alive = true;
            while alive {
                tokio::time::sleep(interval).await;
                if keeper.keep_alive().await.is_err() {
                    alive = false;
                    break;
                }
                match keeper_stream.message().await {
                    Ok(Some(response)) if response.ttl() > 0 => {}
                    _ => {
                        alive = false;
                        break;
                    }
                }
            }
            if !alive {
                let current = state_tx_for_keepalive.borrow().clone();
                if matches!(current, LeadershipState::Leading(_)) {
                    let _ = state_tx_for_keepalive.send(LeadershipState::Following(None));
                }
            }
        });
        // The campaign call blocks until this node wins. We must NOT hold the
        // shared client mutex for that duration or the keep-alive task can't
        // pulse the lease and it expires in ~10s. Clone the client (cheap —
        // shares the gRPC connection) and run campaign on the clone with no
        // mutex held.
        let mut campaign_client = {
            let client = self.client.lock().await;
            client.clone()
        };
        let campaign_result = campaign_client
            .campaign(
                self.election_name.as_bytes().to_vec(),
                self.node_id.as_bytes().to_vec(),
                lease_id,
            )
            .await;
        match campaign_result {
            Ok(_response) => {
                let now_ms = current_time_millis().unwrap_or(0);
                let _ = self.state_tx.send(LeadershipState::Leading(LeaderInfo {
                    node_id: self.node_id.clone(),
                    elected_at_ms: now_ms,
                }));
                let mut guard = self.active_campaign.lock().await;
                if let Some(previous) = guard.take() {
                    previous.keep_alive_task.abort();
                    let mut client = self.client.lock().await;
                    let _ = client.lease_revoke(previous.lease_id).await;
                }
                *guard = Some(CampaignHandle {
                    lease_id,
                    keep_alive_task,
                });
                Ok(())
            }
            Err(err) => {
                keep_alive_task.abort();
                let mut client = self.client.lock().await;
                let _ = client.lease_revoke(lease_id).await;
                Err(anyhow!("election campaign failed: {err}"))
            }
        }
    }

    async fn resign(&self) -> Result<()> {
        let active = self.active_campaign.lock().await.take();
        if let Some(handle) = active {
            handle.keep_alive_task.abort();
            let mut client = self.client.lock().await;
            let _ = client.lease_revoke(handle.lease_id).await;
        }
        let _ = self.state_tx.send(LeadershipState::Following(None));
        Ok(())
    }

    fn state(&self) -> LeadershipState {
        self.state_rx.borrow().clone()
    }

    fn subscribe(&self) -> watch::Receiver<LeadershipState> {
        self.state_rx.clone()
    }

    fn this_node(&self) -> &NodeId {
        &self.node_id
    }
}
