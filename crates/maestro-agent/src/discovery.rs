use std::sync::Arc;

use maestro_core::{DiscoveredReplica, DiscoveryTable, ServiceId, discovery_key_hash};
use tokio::sync::watch;

#[derive(Clone)]
pub struct DiscoveryState {
    table: Arc<watch::Sender<DiscoveryTable>>,
}

#[derive(Debug, PartialEq, Eq)]
pub enum DiscoveryDenied {
    Unauthenticated,
    Forbidden,
}

impl Default for DiscoveryState {
    fn default() -> Self {
        Self {
            table: Arc::new(watch::channel(DiscoveryTable::default()).0),
        }
    }
}

impl DiscoveryState {
    pub fn replace(&self, table: DiscoveryTable) {
        self.table.send_replace(table);
    }

    pub fn subscribe(&self) -> watch::Receiver<DiscoveryTable> {
        self.table.subscribe()
    }

    /// The replicas of `target`, for a caller identified by the service owning its container IP
    /// and the API key it presented; only services `target` lets in may look it up.
    pub fn lookup(
        &self,
        caller: Option<&ServiceId>,
        key: Option<&str>,
        target: &ServiceId,
    ) -> Result<Vec<DiscoveredReplica>, DiscoveryDenied> {
        let table = self.table.borrow();
        let caller = caller.filter(|caller| {
            table
                .key_hashes
                .get(*caller)
                .is_some_and(|hash| key.is_some_and(|key| discovery_key_hash(key) == *hash))
        });
        match caller {
            None => Err(DiscoveryDenied::Unauthenticated),
            Some(caller) if !table.may_reach(caller, target) => Err(DiscoveryDenied::Forbidden),
            Some(_) => Ok(table.services.get(target).cloned().unwrap_or_default()),
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use maestro_core::{DeploymentId, NodeId, ReplicaId};

    use super::*;

    fn state() -> DiscoveryState {
        let state = DiscoveryState::default();
        state.replace(DiscoveryTable {
            services: BTreeMap::from([(
                ServiceId::from("api"),
                vec![DiscoveredReplica {
                    replica_id: ReplicaId::from("d1-0"),
                    deployment_id: DeploymentId::from("d1"),
                    version: Some("00000001".to_string()),
                    host: "d1-0.prod.maestro".to_string(),
                    ip: "10.50.1.2".parse().unwrap(),
                    port: Some(8080),
                    healthy: true,
                    draining: false,
                    node_id: NodeId(1),
                }],
            )]),
            key_hashes: BTreeMap::from([
                (ServiceId::from("web"), discovery_key_hash("web-key")),
                (ServiceId::from("api"), discovery_key_hash("api-key")),
            ]),
            access: BTreeMap::from([(ServiceId::from("api"), vec![ServiceId::from("web")])]),
            ..DiscoveryTable::default()
        });
        state
    }

    #[test]
    fn answers_allowed_callers_presenting_their_own_key() {
        let state = state();
        let web = ServiceId::from("web");
        let api = ServiceId::from("api");
        assert_eq!(
            state.lookup(Some(&web), Some("web-key"), &api).unwrap()[0].host,
            "d1-0.prod.maestro"
        );
        assert_eq!(
            state.lookup(Some(&web), Some("web-key"), &ServiceId::from("worker")),
            Err(DiscoveryDenied::Forbidden)
        );
        assert_eq!(
            state.lookup(Some(&web), Some("web-key"), &web),
            Ok(Vec::new())
        );
        assert_eq!(
            state.lookup(Some(&web), Some("api-key"), &api),
            Err(DiscoveryDenied::Unauthenticated)
        );
        assert_eq!(
            state.lookup(None, Some("web-key"), &api),
            Err(DiscoveryDenied::Unauthenticated)
        );
        assert_eq!(
            state.lookup(Some(&web), None, &api),
            Err(DiscoveryDenied::Unauthenticated)
        );
    }
}
