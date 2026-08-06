use std::net::{IpAddr, Ipv4Addr};
use std::sync::Arc;
use std::time::Duration;

use kernel_api::{NodeId, ResourceKind};
use kernel_store::{ExpectedVersion, InMemoryStore, Keyspace, PutRequest, Store, TokioClock};
use node_agent::{AssignmentAgent, AssignmentAgentSettings, SystemStatusClock, WorkloadDns};
use runtime::{
    FakeNetworkProvider, FakeRuntime, FakeRuntimeOperation, NetworkAddressing, NetworkCidr,
    NetworkSpec, WorkloadRuntime, WorkloadState,
};

use crate::legacy_cluster_tests::running_assignment_snapshot;
use crate::plan_legacy_snapshot;

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[tokio::test]
async fn migrated_running_assignment_is_recreated_without_restart_accounting() -> TestResult {
    let plan = plan_legacy_snapshot(&running_assignment_snapshot()?, MASTER_SECRET)?;
    let cluster_id = plan.cluster_id().clone();
    let node_id = NodeId::new("node-a")?;
    let monotonic_clock = Arc::new(TokioClock::new());
    let store = Arc::new(InMemoryStore::new(monotonic_clock.clone()));
    let keyspace = Keyspace::new(&cluster_id);
    for write in plan.writes() {
        store
            .put_cas(PutRequest {
                key: keyspace.resource(&ResourceKind::new(write.kind().as_str())?, write.id()),
                value: write.value().to_vec(),
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
    }

    let runtime = Arc::new(FakeRuntime::new());
    let network = Arc::new(FakeNetworkProvider::default());
    let state_root = tempfile::tempdir()?;
    let agent = AssignmentAgent::new(
        store.clone(),
        runtime.clone(),
        network,
        AssignmentAgentSettings {
            cluster_id: cluster_id.clone(),
            node_id: node_id.clone(),
            network: NetworkSpec {
                name: "maestro-node-a".to_owned(),
                addressing: NetworkAddressing::Managed {
                    range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24)?,
                    gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                },
                mtu_bytes: 1_420,
            },
            dns: WorkloadDns::Disabled,
            system_host_ports: Default::default(),
            stop_timeout: Duration::from_secs(5),
            resync_interval: Duration::from_secs(30),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            reconcile_timeout: Duration::from_secs(10),
            secrets_root: state_root.path().join("secrets"),
            node_api_root: state_root.path().join("node-api"),
        },
        #[cfg(unix)]
        None,
        monotonic_clock,
        Arc::new(SystemStatusClock),
    )?;

    let report = agent.reconcile_once().await?;
    assert_eq!(report.desired, 1);
    assert_eq!(report.running, 1);
    assert_eq!(report.restarted, 0);
    assert_eq!(report.unresolved, 0);
    let calls = runtime.calls()?;
    assert_eq!(operation_count(&calls, FakeRuntimeOperation::Create), 1);
    assert_eq!(operation_count(&calls, FakeRuntimeOperation::Start), 1);
    let workloads = runtime.list(&cluster_id, &node_id).await?;
    assert_eq!(workloads.len(), 1);
    assert_eq!(
        workloads.first().map(|workload| workload.status.state),
        Some(WorkloadState::Running)
    );

    Ok(())
}

fn operation_count(calls: &[runtime::FakeRuntimeCall], operation: FakeRuntimeOperation) -> usize {
    calls
        .iter()
        .filter(|call| call.operation == operation)
        .count()
}
