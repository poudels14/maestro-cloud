mod support;

use std::sync::Arc;
use std::time::Duration;

use maestro_control::{Deployer, DeployerSettings, StoreWriter};
use maestro_core::{Deployment, DeploymentId, DeploymentStatus, NodeId, ReplicaPhase, ServiceId};
use maestro_store::Mutation;
use support::{FakeClock, FakeObserved, LocalWriter, clock, image_service, node};

struct Harness {
    writer: Arc<LocalWriter>,
    observed: Arc<FakeObserved>,
    clock: Arc<FakeClock>,
    deployer: Deployer<LocalWriter, FakeObserved>,
}

fn harness(nodes: Vec<maestro_core::NodeRecord>) -> Harness {
    let writer = Arc::new(LocalWriter::new());
    let observed = Arc::new(FakeObserved::default());
    let clock = clock();
    let clock_source = clock.clone();
    let settings = DeployerSettings {
        clock: Arc::new(move || clock_source.now()),
        ..DeployerSettings::default()
    };
    let mutations = nodes
        .into_iter()
        .map(|node| Mutation::PutNode { node })
        .collect::<Vec<_>>();
    writer.state().apply(&mutations).unwrap();
    let deployer = Deployer::new(writer.clone(), observed.clone(), settings);
    Harness {
        writer,
        observed,
        clock,
        deployer,
    }
}

fn queue(harness: &Harness, deployment_id: &str, replicas: u32) {
    let spec = image_service(replicas);
    harness
        .writer
        .state()
        .apply(&[
            Mutation::PutService {
                id: ServiceId::from("api"),
                spec: spec.clone(),
            },
            Mutation::CreateDeployment {
                deployment: Deployment {
                    id: DeploymentId::from(deployment_id),
                    service_id: ServiceId::from("api"),
                    spec,
                    status: DeploymentStatus::Queued,
                    image: None,
                    failure: None,
                    created_at_ms: harness.clock.now(),
                    updated_at_ms: harness.clock.now(),
                },
            },
        ])
        .unwrap();
}

fn status(harness: &Harness, deployment_id: &str) -> DeploymentStatus {
    harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from(deployment_id))
        .unwrap()
        .unwrap()
        .status
}

fn assigned_replicas(harness: &Harness) -> Vec<String> {
    let mut replicas = harness
        .writer
        .state()
        .nodes()
        .unwrap()
        .iter()
        .flat_map(|node| harness.writer.state().assignments(node.id).unwrap())
        .map(|assignment| assignment.replica_id.to_string())
        .collect::<Vec<_>>();
    replicas.sort();
    replicas
}

#[tokio::test]
async fn deploys_image_service_until_ready() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    queue(&harness, "d1", 2);

    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Deploying);
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d1-1"]);
    let node_one = harness.writer.state().assignments(NodeId(1)).unwrap();
    let node_two = harness.writer.state().assignments(NodeId(2)).unwrap();
    assert_eq!((node_one.len(), node_two.len()), (1, 1));

    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Deploying);

    harness.observed.set("d1-1", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
}

#[tokio::test]
async fn new_deployment_replaces_old_after_drain_grace() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1);
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1);
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d2-0"]);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);

    harness.observed.set("d2-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d2"), DeploymentStatus::Ready);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Draining);
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d2-0"]);

    harness
        .clock
        .advance(Duration::from_secs(6).as_millis() as u64);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Stopped);
    assert_eq!(assigned_replicas(&harness), ["d2-0"]);
}

#[tokio::test]
async fn failed_replica_fails_deployment_and_keeps_old_one() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1);
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1);
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d2-0", ReplicaPhase::Failed);
    harness.deployer.tick().await.unwrap();

    assert_eq!(status(&harness, "d2"), DeploymentStatus::Failed);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
    assert_eq!(assigned_replicas(&harness), ["d1-0"]);
}

#[tokio::test]
async fn times_out_unhealthy_deployments() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1);
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Unhealthy);

    harness
        .clock
        .advance(Duration::from_secs(601).as_millis() as u64);
    harness.deployer.tick().await.unwrap();
    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status, DeploymentStatus::Failed);
    assert!(deployment.failure.unwrap().contains("timed out"));
    assert!(assigned_replicas(&harness).is_empty());
}

#[tokio::test]
async fn unschedulable_deployment_fails_without_partial_placement() {
    let harness = harness(vec![node(1, 2, 32)]);
    queue(&harness, "d1", 5);
    harness.deployer.tick().await.unwrap();

    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status, DeploymentStatus::Failed);
    assert!(deployment.failure.unwrap().starts_with("unschedulable"));
    assert!(assigned_replicas(&harness).is_empty());
}
