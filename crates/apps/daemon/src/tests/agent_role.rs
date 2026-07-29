use std::collections::BTreeMap;
use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{ClusterId, NodeId, NodeInstanceId, NodeRole, NodeSpec, WorkloadNetworkMode};
use kernel_store::{InMemoryStore, Keyspace, Store, TokioClock};
use node_agent::{NodeRegistryAgent, NodeRegistrySettings, SystemStatusClock};
use semver::Version;

use crate::agent_role::register_node_liveness;

#[tokio::test]
async fn waits_for_predecessor_liveness_lease_to_expire() -> Result<(), Box<dyn std::error::Error>>
{
    let clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(clock.clone()));
    let predecessor = test_registry(store.clone(), clock.clone(), "instance-1")?;
    let predecessor_registration = predecessor.register().await?;
    drop(predecessor_registration);

    let successor = test_registry(store.clone(), clock.clone(), "instance-2")?;
    let successor_registration = tokio::time::timeout(
        Duration::from_secs(1),
        register_node_liveness(&successor, clock.as_ref(), Duration::from_millis(5)),
    )
    .await??;
    let key = Keyspace::new(&ClusterId::new("registration-retry")?)
        .node_liveness(&NodeId::new("node-1")?);
    let liveness = store.get(&key).await?.ok_or("liveness missing")?;
    assert_eq!(liveness.value, b"instance-2");

    successor_registration.close().await?;
    Ok(())
}

fn test_registry(
    store: Arc<InMemoryStore>,
    clock: Arc<TokioClock>,
    instance_id: &str,
) -> Result<NodeRegistryAgent, Box<dyn std::error::Error>> {
    Ok(NodeRegistryAgent::new(
        store,
        NodeRegistrySettings {
            cluster_id: ClusterId::new("registration-retry")?,
            node_id: NodeId::new("node-1")?,
            node_spec: NodeSpec {
                hostname: "node-1.internal".to_owned(),
                host_address: IpAddr::V4(Ipv4Addr::new(10, 20, 0, 11)),
                role: NodeRole::Worker,
                workload_network_mode: WorkloadNetworkMode::ClusterRouted,
                scheduling_labels: BTreeMap::new(),
            },
            instance_id: NodeInstanceId::new(instance_id)?,
            running_version: Version::new(0, 6, 0),
            session_ttl: Duration::from_millis(30),
            keepalive_interval: Duration::from_millis(10),
        },
        clock,
        Arc::new(SystemStatusClock),
    )?)
}
