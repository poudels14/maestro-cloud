//! [`ClusterService`] runs the per-node cluster lifecycle: register/heartbeat
//! into the [`NodeRegistry`] and campaign for leadership via the
//! [`LeaderElector`]. It exposes a snapshot of cluster topology + leadership
//! state for the rest of the controller to consume.

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use tokio::sync::{broadcast, watch};
use tokio::task::JoinHandle;

use super::elector::LeaderElector;
use super::registry::NodeRegistry;
use super::types::{LeaderInfo, LeadershipState, NodeId, NodeInfo, NodeRole};
use crate::signal::ShutdownEvent;

pub const DEFAULT_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Clone)]
pub struct ClusterService {
    info: NodeInfo,
    registry: Arc<dyn NodeRegistry>,
    elector: Arc<dyn LeaderElector>,
    heartbeat_interval: Duration,
}

#[derive(Debug, Clone, Default)]
pub struct ClusterSnapshot {
    pub nodes: Vec<NodeInfo>,
    pub leader: Option<LeaderInfo>,
    pub this_node: Option<NodeId>,
    pub leadership: Option<LeadershipState>,
}

impl ClusterService {
    pub fn new(
        info: NodeInfo,
        registry: Arc<dyn NodeRegistry>,
        elector: Arc<dyn LeaderElector>,
    ) -> Self {
        Self {
            info,
            registry,
            elector,
            heartbeat_interval: DEFAULT_HEARTBEAT_INTERVAL,
        }
    }

    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.heartbeat_interval = interval;
        self
    }

    pub fn info(&self) -> &NodeInfo {
        &self.info
    }

    pub fn registry(&self) -> Arc<dyn NodeRegistry> {
        Arc::clone(&self.registry)
    }

    pub fn elector(&self) -> Arc<dyn LeaderElector> {
        Arc::clone(&self.elector)
    }

    pub fn leadership(&self) -> LeadershipState {
        self.elector.state()
    }

    pub fn watch_leadership(&self) -> watch::Receiver<LeadershipState> {
        self.elector.subscribe()
    }

    pub async fn snapshot(&self) -> ClusterSnapshot {
        let nodes = self.registry.list_nodes().await.unwrap_or_default();
        let leadership = self.leadership();
        let leader = leadership.leader().cloned();
        ClusterSnapshot {
            nodes,
            leader,
            this_node: Some(self.info.node_id.clone()),
            leadership: Some(leadership),
        }
    }

    pub fn spawn(self, mut shutdown_rx: broadcast::Receiver<ShutdownEvent>) -> ClusterHandles {
        let registry_for_heartbeat = Arc::clone(&self.registry);
        let registry_for_register = Arc::clone(&self.registry);
        let info_for_heartbeat = self.info.clone();
        let interval = self.heartbeat_interval;

        let heartbeat_handle = tokio::spawn(async move {
            if let Err(err) = registry_for_register.register(&info_for_heartbeat).await {
                eprintln!("cluster: initial registration failed: {err}");
                return;
            }
            let mut ticker = tokio::time::interval(interval);
            ticker.tick().await;
            loop {
                tokio::select! {
                    _ = shutdown_rx.recv() => {
                        let _ = registry_for_heartbeat.deregister().await;
                        return;
                    }
                    _ = ticker.tick() => {
                        if let Err(err) = registry_for_heartbeat.keep_alive().await {
                            eprintln!("cluster: heartbeat failed: {err}");
                            if let Err(err) = registry_for_heartbeat.register(&info_for_heartbeat).await {
                                eprintln!("cluster: re-register failed: {err}");
                            }
                        }
                    }
                }
            }
        });

        let elector_handle = if self.info.role.can_lead() {
            let elector = Arc::clone(&self.elector);
            Some(tokio::spawn(async move {
                loop {
                    if let Err(err) = elector.campaign().await {
                        eprintln!("cluster: campaign error: {err}");
                        tokio::time::sleep(Duration::from_secs(1)).await;
                    } else {
                        return;
                    }
                }
            }))
        } else {
            None
        };

        ClusterHandles {
            heartbeat: heartbeat_handle,
            elector: elector_handle,
        }
    }
}

pub struct ClusterHandles {
    pub heartbeat: JoinHandle<()>,
    pub elector: Option<JoinHandle<()>>,
}

impl ClusterHandles {
    pub fn abort(self) {
        self.heartbeat.abort();
        if let Some(handle) = self.elector {
            handle.abort();
        }
    }
}

pub fn node_info_from(
    node_id: NodeId,
    hostname: String,
    role: NodeRole,
    api_port: u16,
    tailscale_ip: Option<String>,
    version: String,
    started_at_ms: u64,
) -> NodeInfo {
    NodeInfo {
        node_id,
        hostname,
        role,
        tailscale_ip,
        api_port,
        version,
        started_at_ms,
        labels: Default::default(),
        unschedulable: false,
    }
}

pub async fn wait_for_leadership(elector: &dyn LeaderElector, timeout: Duration) -> Result<bool> {
    let mut receiver = elector.subscribe();
    let deadline = tokio::time::Instant::now() + timeout;
    loop {
        if matches!(receiver.borrow().clone(), LeadershipState::Leading(_)) {
            return Ok(true);
        }
        let remaining = deadline.saturating_duration_since(tokio::time::Instant::now());
        if remaining.is_zero() {
            return Ok(false);
        }
        match tokio::time::timeout(remaining, receiver.changed()).await {
            Ok(Ok(())) => {}
            Ok(Err(_)) | Err(_) => return Ok(false),
        }
    }
}
