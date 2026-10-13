use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use maestro_core::ReplicaStatus;
use maestro_telemetry::{DeploymentFromReplicaId, ReplicaIdentity, ReplicaResolver};

use crate::unix_http;

const REFRESH_INTERVAL: Duration = Duration::from_secs(10);

#[derive(Default)]
pub struct AgentReplicaResolver {
    replicas: RwLock<Option<HashMap<String, ReplicaIdentity>>>,
}

impl AgentReplicaResolver {
    pub fn spawn(api_socket: PathBuf) -> Arc<Self> {
        let resolver = Arc::new(Self::default());
        let refreshing = resolver.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
            loop {
                ticker.tick().await;
                match fetch_replicas(&api_socket).await {
                    Ok(replicas) => refreshing.replace(replicas),
                    Err(error) => tracing::debug!(%error, "failed to refresh replicas from agent"),
                }
            }
        });
        resolver
    }

    fn replace(&self, replicas: Vec<ReplicaStatus>) {
        let identities = replicas
            .into_iter()
            .map(|replica| {
                let identity = ReplicaIdentity {
                    service_id: Some(replica.service_id.to_string()),
                    deployment_id: Some(replica.deployment_id.to_string()),
                };
                (replica.replica_id.to_string(), identity)
            })
            .collect();
        *self
            .replicas
            .write()
            .unwrap_or_else(|poisoned| poisoned.into_inner()) = Some(identities);
    }
}

impl ReplicaResolver for AgentReplicaResolver {
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity {
        self.replicas
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .and_then(|replicas| replicas.get(replica_id).cloned())
            .unwrap_or_else(|| DeploymentFromReplicaId.resolve(replica_id))
    }

    fn is_live(&self, replica_id: &str) -> bool {
        self.replicas
            .read()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
            .as_ref()
            .is_none_or(|replicas| replicas.contains_key(replica_id))
    }
}

async fn fetch_replicas(api_socket: &Path) -> anyhow::Result<Vec<ReplicaStatus>> {
    let (status, body) = unix_http::get(api_socket, "/api/replicas").await?;
    anyhow::ensure!(status.is_success(), "agent returned {status}");
    Ok(serde_json::from_slice(&body)?)
}
