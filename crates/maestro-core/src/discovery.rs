use std::collections::BTreeMap;
use std::net::IpAddr;

use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};

use crate::ids::{DeploymentId, NodeId, ReplicaId, ServiceId};

pub const DISCOVERY_PORT: u16 = 7480;
pub const DISCOVERY_HOST_ENV: &str = "MAESTRO_API_HOST";
pub const DISCOVERY_KEY_ENV: &str = "MAESTRO_API_KEY";

/// What every agent needs to answer service-discovery requests from its node's containers.
#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveryTable {
    pub services: BTreeMap<ServiceId, Vec<DiscoveredReplica>>,
    /// The hash of every service's API key; a caller is the service owning its container IP and
    /// must present that service's key.
    pub key_hashes: BTreeMap<ServiceId, String>,
    /// The services allowed to reach each service (its `network.from`, previews included).
    pub access: BTreeMap<ServiceId, Vec<ServiceId>>,
    /// The addresses of every running replica, retiring ones included, by service.
    pub members: BTreeMap<ServiceId, Vec<IpAddr>>,
    /// Node gateways and system containers, which may reach every service.
    pub always_allowed: Vec<IpAddr>,
}

impl DiscoveryTable {
    pub fn may_reach(&self, caller: &ServiceId, target: &ServiceId) -> bool {
        caller == target
            || self
                .access
                .get(target)
                .is_some_and(|callers| callers.contains(caller))
    }

    /// The source addresses allowed to connect to `target`'s replicas.
    pub fn allowed_sources(&self, target: &ServiceId) -> Vec<IpAddr> {
        let mut sources = self
            .members
            .iter()
            .filter(|(service_id, _)| self.may_reach(service_id, target))
            .flat_map(|(_, addresses)| addresses.iter().copied())
            .chain(self.always_allowed.iter().copied())
            .collect::<Vec<_>>();
        sources.sort();
        sources.dedup();
        sources
    }
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DiscoveredReplica {
    pub replica_id: ReplicaId,
    pub deployment_id: DeploymentId,
    pub version: Option<String>,
    pub host: String,
    pub ip: IpAddr,
    pub port: Option<u16>,
    pub healthy: bool,
    /// A replica of a superseded deployment: it gets no new traffic and is terminated once it
    /// drains, but clients may still route work it owns to it.
    pub draining: bool,
    pub node_id: NodeId,
}

pub fn discovery_key_hash(key: &str) -> String {
    hex::encode(Sha256::digest(key.as_bytes()))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn allows_listed_callers_the_service_itself_and_system_addresses() {
        let table = DiscoveryTable {
            access: BTreeMap::from([(ServiceId::from("api"), vec![ServiceId::from("web")])]),
            members: BTreeMap::from([
                (ServiceId::from("api"), vec!["10.50.1.5".parse().unwrap()]),
                (ServiceId::from("web"), vec!["10.50.2.7".parse().unwrap()]),
                (ServiceId::from("db"), vec!["10.50.3.9".parse().unwrap()]),
            ]),
            always_allowed: vec!["10.50.1.1".parse().unwrap()],
            ..DiscoveryTable::default()
        };
        assert!(table.may_reach(&ServiceId::from("web"), &ServiceId::from("api")));
        assert!(!table.may_reach(&ServiceId::from("db"), &ServiceId::from("api")));
        assert!(!table.may_reach(&ServiceId::from("api"), &ServiceId::from("web")));
        assert_eq!(
            table.allowed_sources(&ServiceId::from("api")),
            [
                "10.50.1.1".parse::<IpAddr>().unwrap(),
                "10.50.1.5".parse().unwrap(),
                "10.50.2.7".parse().unwrap()
            ]
        );
    }
}
