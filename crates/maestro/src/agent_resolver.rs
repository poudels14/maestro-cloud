use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, RwLock};
use std::time::Duration;

use maestro_agent::LocalReplica;
use maestro_telemetry::{DeploymentFromReplicaId, ReplicaIdentity, ReplicaResolver};

const REFRESH_INTERVAL: Duration = Duration::from_secs(5);
const SYSTEM_CONTAINER_PREFIX: &str = "maestro-";

#[derive(Default)]
pub struct AgentReplicaResolver {
    replicas: RwLock<Option<HashMap<String, ReplicaIdentity>>>,
}

impl AgentReplicaResolver {
    pub fn spawn(manifest: PathBuf) -> Arc<Self> {
        let resolver = Arc::new(Self::default());
        let refreshing = resolver.clone();
        tokio::spawn(async move {
            let mut ticker = tokio::time::interval(REFRESH_INTERVAL);
            loop {
                ticker.tick().await;
                match read_manifest(&manifest).await {
                    Ok(replicas) => refreshing.replace(replicas),
                    Err(error) => {
                        tracing::debug!(%error, "failed to read the agent replica manifest")
                    }
                }
            }
        });
        resolver
    }

    fn replace(&self, replicas: Vec<LocalReplica>) {
        let identities = replicas
            .into_iter()
            .map(|replica| {
                let identity = ReplicaIdentity {
                    service_id: Some(replica.service_id),
                    deployment_id: Some(replica.deployment_id),
                    healthcheck_path: replica.healthcheck_path,
                };
                (replica.replica_id, identity)
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
        replica_id.starts_with(SYSTEM_CONTAINER_PREFIX)
            || self
                .replicas
                .read()
                .unwrap_or_else(|poisoned| poisoned.into_inner())
                .as_ref()
                .is_none_or(|replicas| replicas.contains_key(replica_id))
    }
}

async fn read_manifest(path: &Path) -> anyhow::Result<Vec<LocalReplica>> {
    Ok(serde_json::from_slice(&tokio::fs::read(path).await?)?)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn system_containers_stay_live_while_replicas_follow_the_agent() {
        let resolver = AgentReplicaResolver::default();
        resolver.replace(vec![LocalReplica {
            replica_id: "d1-0".to_string(),
            service_id: "web".to_string(),
            deployment_id: "d1".to_string(),
            healthcheck_path: Some("/health".to_string()),
        }]);
        assert!(resolver.is_live("maestro-traefik"));
        assert!(resolver.is_live("d1-0"));
        assert!(!resolver.is_live("d2-0"));
        assert_eq!(
            resolver.resolve("d1-0").healthcheck_path.as_deref(),
            Some("/health")
        );
    }
}
