use std::collections::HashMap;

use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
pub struct ReplicaIdentity {
    pub service_id: Option<String>,
    pub deployment_id: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub healthcheck_path: Option<String>,
}

pub trait ReplicaResolver: Send + Sync {
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity;

    fn is_live(&self, _replica_id: &str) -> bool {
        false
    }
}

#[derive(Clone, Copy, Debug, Default)]
pub struct DeploymentFromReplicaId;

impl ReplicaResolver for DeploymentFromReplicaId {
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity {
        let deployment_id = replica_id
            .rsplit_once('-')
            .filter(|(deployment, index)| {
                !deployment.is_empty()
                    && !index.is_empty()
                    && index.chars().all(|character| character.is_ascii_digit())
            })
            .map(|(deployment, _)| deployment.to_string());
        ReplicaIdentity {
            service_id: None,
            deployment_id,
            healthcheck_path: None,
        }
    }
}

impl ReplicaResolver for HashMap<String, ReplicaIdentity> {
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity {
        self.get(replica_id).cloned().unwrap_or_default()
    }
}

impl<Resolve> ReplicaResolver for Resolve
where
    Resolve: Fn(&str) -> ReplicaIdentity + Send + Sync,
{
    fn resolve(&self, replica_id: &str) -> ReplicaIdentity {
        self(replica_id)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn derives_deployment_from_indexed_replica_ids() {
        assert_eq!(
            DeploymentFromReplicaId.resolve("dmv75i2rrmi1h-0"),
            ReplicaIdentity {
                service_id: None,
                deployment_id: Some("dmv75i2rrmi1h".to_string()),
                healthcheck_path: None,
            }
        );
        assert_eq!(
            DeploymentFromReplicaId.resolve("traefik"),
            ReplicaIdentity::default()
        );
        assert_eq!(
            DeploymentFromReplicaId.resolve("edge-proxy"),
            ReplicaIdentity::default()
        );
    }
}
