mod support;

use std::sync::Arc;
use std::time::Duration;

use maestro_control::{Deployer, DeployerSettings, StoreWriter};
use maestro_core::{Deployment, DeploymentId, DeploymentStatus, NodeId, ReplicaPhase, ServiceId};
use maestro_store::Mutation;
use support::{
    FakeBuilder, FakeClock, FakeObserved, LocalWriter, build_service, clock, image_service, node,
};

struct Harness {
    writer: Arc<LocalWriter>,
    observed: Arc<FakeObserved>,
    builder: Arc<FakeBuilder>,
    clock: Arc<FakeClock>,
    deployer: Deployer<LocalWriter, FakeObserved, FakeBuilder>,
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
    let builder = Arc::new(FakeBuilder::default());
    let deployer = Deployer::new(writer.clone(), observed.clone(), builder.clone(), settings);
    Harness {
        writer,
        observed,
        builder,
        clock,
        deployer,
    }
}

async fn queue(harness: &Harness, deployment_id: &str, replicas: u32) {
    queue_spec(harness, deployment_id, image_service(replicas)).await;
}

async fn queue_spec(harness: &Harness, deployment_id: &str, spec: maestro_core::ServiceSpec) {
    let resolves_image = spec.image.is_some();
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
                    git_commit: None,
                    built_on: None,
                },
            },
        ])
        .unwrap();
    if resolves_image {
        tokio::time::timeout(Duration::from_secs(1), async {
            while status(harness, deployment_id) == DeploymentStatus::Queued {
                harness.deployer.tick().await.unwrap();
                settle().await;
            }
        })
        .await
        .unwrap();
    }
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

fn retiring_replicas(harness: &Harness) -> Vec<String> {
    let mut replicas = harness
        .writer
        .state()
        .nodes()
        .unwrap()
        .iter()
        .flat_map(|node| harness.writer.state().assignments(node.id).unwrap())
        .filter(|assignment| assignment.is_retiring())
        .map(|assignment| assignment.replica_id.to_string())
        .collect::<Vec<_>>();
    replicas.sort();
    replicas
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
    queue(&harness, "d1", 2).await;

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
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1).await;
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
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1).await;
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
    queue(&harness, "d1", 1).await;
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
    queue(&harness, "d1", 5).await;
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

async fn settle() {
    for _ in 0..20 {
        tokio::task::yield_now().await;
    }
}

#[tokio::test]
async fn builds_images_before_deploying() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue_spec(&harness, "d1", build_service(1)).await;

    harness.deployer.tick().await.unwrap();
    settle().await;
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Building);
    assert_eq!(harness.builder.started(), ["d1"]);
    let building = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(
        building.git_commit.map(|commit| commit.message),
        Some("Add things".to_string())
    );

    harness.deployer.tick().await.unwrap();
    settle().await;
    assert_eq!(harness.builder.started(), ["d1"]);

    harness.builder.release(1);
    settle().await;
    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status, DeploymentStatus::Deploying);
    assert_eq!(deployment.image.as_deref(), Some("maestro.local/api:d1"));
    assert_eq!(
        deployment.git_commit.map(|commit| commit.reference),
        Some("abc123".to_string())
    );
    assert_eq!(deployment.built_on, Some(NodeId(1)));

    harness.deployer.tick().await.unwrap();
    let node_assignments = harness.writer.state().assignments(NodeId(1)).unwrap();
    assert_eq!(node_assignments[0].image, "maestro.local/api:d1");
}

#[tokio::test]
async fn failed_builds_fail_the_deployment() {
    let harness = harness(vec![node(1, 8, 32)]);
    harness.builder.fail("d1");
    queue_spec(&harness, "d1", build_service(1)).await;
    harness.deployer.tick().await.unwrap();
    harness.builder.release(1);
    settle().await;

    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status, DeploymentStatus::Failed);
    assert_eq!(
        deployment.failure.as_deref(),
        Some("build failed: dockerfile not found")
    );
}

#[tokio::test]
async fn limits_concurrent_builds() {
    let harness = harness(vec![node(1, 8, 32)]);
    for index in 1..=6 {
        queue_spec(&harness, &format!("d{index}"), build_service(1)).await;
        harness.clock.advance(1);
    }

    harness.deployer.tick().await.unwrap();
    settle().await;
    assert_eq!(harness.builder.started(), ["d1", "d2", "d3", "d4", "d5"]);
    assert_eq!(status(&harness, "d6"), DeploymentStatus::Queued);

    harness.builder.release(1);
    settle().await;
    harness.deployer.tick().await.unwrap();
    settle().await;
    assert_eq!(harness.builder.started().len(), 6);
}

#[tokio::test]
async fn newer_ready_deployment_cancels_older_in_flight_ones() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Restarting);

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d2-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    assert_eq!(status(&harness, "d2"), DeploymentStatus::Ready);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Cancelled);
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d2-0"]);
}

#[tokio::test]
async fn cancelled_build_result_is_ignored() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue_spec(&harness, "d1", build_service(1)).await;
    harness.deployer.tick().await.unwrap();
    settle().await;

    harness.clock.advance(1_000);
    queue(&harness, "d2", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d2-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Cancelled);

    harness.builder.release(1);
    settle().await;
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Cancelled);
}

#[tokio::test]
async fn reschedules_replicas_from_lost_nodes() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    queue(&harness, "d1", 2).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.observed.set("d1-1", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
    let lost = harness.writer.state().assignments(NodeId(2)).unwrap();
    assert_eq!(lost.len(), 1);

    harness.observed.lose_node(NodeId(2));
    harness.deployer.tick().await.unwrap();

    assert!(
        harness
            .writer
            .state()
            .assignments(NodeId(2))
            .unwrap()
            .is_empty()
    );
    let survivors = harness.writer.state().assignments(NodeId(1)).unwrap();
    assert_eq!(survivors.len(), 2);
    assert!(survivors.iter().any(|assignment| assignment.index == 2));
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
}

#[tokio::test]
async fn deploying_counts_actual_assignments_after_rescheduling() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    queue(&harness, "d1", 2).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.lose_node(NodeId(2));
    harness.deployer.tick().await.unwrap();
    let replicas = assigned_replicas(&harness);
    assert_eq!(replicas.len(), 2);
    replicas
        .iter()
        .for_each(|replica| harness.observed.set(replica, ReplicaPhase::Healthy));
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
}

#[tokio::test]
async fn image_deployments_pin_the_resolved_digest() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    let assignments = harness.writer.state().assignments(NodeId(1)).unwrap();
    assert_eq!(assignments[0].image, "nginx:1.27@sha256:feed");
}

#[tokio::test]
async fn unresolvable_image_fails_the_deployment() {
    let harness = harness(vec![node(1, 8, 32)]);
    let mut spec = image_service(1);
    spec.image = Some("missing:1".to_string());
    queue_spec(&harness, "d1", spec).await;
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Failed);
    assert!(assigned_replicas(&harness).is_empty());
}

#[tokio::test]
async fn replicas_override_scales_a_ready_deployment_in_place() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);

    set_override(&harness, Some(3));
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d1-1", "d1-2"]);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);

    set_override(&harness, None);
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d1-1", "d1-2"]);
    assert_eq!(retiring_replicas(&harness), ["d1-1", "d1-2"]);

    harness
        .clock
        .advance(Duration::from_secs(6).as_millis() as u64);
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0"]);
}

#[tokio::test]
async fn new_deployments_start_with_the_override() {
    let harness = harness(vec![node(1, 8, 32)]);
    set_override(&harness, Some(2));
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d1-1"]);
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Deploying);
    harness.observed.set("d1-1", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
}

#[tokio::test]
async fn deployments_with_a_known_image_skip_the_build() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue_spec(&harness, "d1", build_service(1)).await;
    let mut deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    deployment.id = DeploymentId::from("d2");
    deployment.image = Some("maestro.local/api:d0".to_string());
    harness
        .writer
        .state()
        .apply(&[
            Mutation::SetDeploymentStatus {
                service_id: ServiceId::from("api"),
                deployment_id: DeploymentId::from("d1"),
                status: DeploymentStatus::Cancelled,
                image: None,
                failure: None,
                at_ms: harness.clock.now(),
            },
            Mutation::CreateDeployment { deployment },
        ])
        .unwrap();
    harness.deployer.tick().await.unwrap();
    assert!(harness.builder.started().is_empty());
    assert_eq!(status(&harness, "d2"), DeploymentStatus::Deploying);
    assert_eq!(assigned_replicas(&harness), ["d2-0"]);
}

fn set_override(harness: &Harness, replicas: Option<u32>) {
    harness
        .writer
        .state()
        .apply(&[Mutation::SetServiceControls {
            id: ServiceId::from("api"),
            controls: maestro_core::ServiceControls {
                frozen: false,
                replicas_override: replicas,
            },
        }])
        .unwrap();
}

#[tokio::test]
async fn draining_a_node_replaces_its_replicas_before_releasing_them() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    queue(&harness, "d1", 2).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.observed.set("d1-1", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    let on_one = harness.writer.state().assignments(NodeId(1)).unwrap()[0]
        .replica_id
        .to_string();

    let mut draining = node(1, 8, 32);
    draining.state = maestro_core::NodeState::Draining;
    harness
        .writer
        .state()
        .apply(&[Mutation::PutNode { node: draining }])
        .unwrap();
    harness.deployer.tick().await.unwrap();
    assert_eq!(assigned_replicas(&harness), ["d1-0", "d1-1", "d1-2"]);
    assert_eq!(
        harness.writer.state().assignments(NodeId(2)).unwrap().len(),
        2
    );

    harness.deployer.tick().await.unwrap();
    assert!(assigned_replicas(&harness).contains(&on_one));

    harness.observed.set("d1-2", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();
    assert_eq!(retiring_replicas(&harness), [on_one.clone()]);

    harness
        .clock
        .advance(Duration::from_secs(6).as_millis() as u64);
    harness.deployer.tick().await.unwrap();
    assert!(
        harness
            .writer
            .state()
            .assignments(NodeId(1))
            .unwrap()
            .is_empty()
    );
    assert_eq!(assigned_replicas(&harness).len(), 2);
    assert_eq!(status(&harness, "d1"), DeploymentStatus::Ready);
}

#[tokio::test]
async fn new_deployments_skip_unreachable_nodes() {
    let harness = harness(vec![node(1, 8, 32), node(2, 8, 32)]);
    harness.observed.lose_node(NodeId(2));
    queue(&harness, "d1", 2).await;
    harness.deployer.tick().await.unwrap();
    assert_eq!(
        harness.writer.state().assignments(NodeId(1)).unwrap().len(),
        2
    );
    assert!(
        harness
            .writer
            .state()
            .assignments(NodeId(2))
            .unwrap()
            .is_empty()
    );
}

#[tokio::test]
async fn reports_replicas_that_cannot_be_placed() {
    let harness = harness(vec![node(1, 8, 32)]);
    queue(&harness, "d1", 1).await;
    harness.deployer.tick().await.unwrap();
    harness.observed.set("d1-0", ReplicaPhase::Healthy);
    harness.deployer.tick().await.unwrap();

    let mut draining = node(1, 8, 32);
    draining.state = maestro_core::NodeState::Draining;
    harness
        .writer
        .state()
        .apply(&[Mutation::PutNode { node: draining }])
        .unwrap();
    harness.deployer.tick().await.unwrap();
    let report = harness.writer.state().unschedulable().unwrap();
    assert_eq!(report.len(), 1);
    assert_eq!(report[0].reason, "no schedulable nodes");
    assert_eq!(assigned_replicas(&harness), ["d1-0"]);

    harness
        .writer
        .state()
        .apply(&[Mutation::PutNode {
            node: node(2, 8, 32),
        }])
        .unwrap();
    harness.deployer.tick().await.unwrap();
    assert!(harness.writer.state().unschedulable().unwrap().is_empty());
}

#[tokio::test]
async fn resolves_secret_sources_before_deploying() {
    let harness = harness(vec![node(1, 8, 32)]);
    let mut spec = image_service(1);
    spec.deploy.env.source = Some("aws-secret://app".to_string());
    queue_spec(&harness, "d1", spec).await;
    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.spec.deploy.env.source, None);
    assert_eq!(deployment.spec.deploy.env.items["FROM_SOURCE"], "fetched");
    assert_eq!(deployment.status, DeploymentStatus::Deploying);
}

#[tokio::test]
async fn failed_secret_resolution_fails_the_deployment() {
    let harness = harness(vec![node(1, 8, 32)]);
    let mut spec = image_service(1);
    spec.deploy.env.source = Some("aws-secret://missing".to_string());
    queue_spec(&harness, "d1", spec).await;
    let deployment = harness
        .writer
        .state()
        .deployment(&ServiceId::from("api"), &DeploymentId::from("d1"))
        .unwrap()
        .unwrap();
    assert_eq!(deployment.status, DeploymentStatus::Failed);
    assert!(deployment.failure.unwrap().contains("secret not found"));
}
