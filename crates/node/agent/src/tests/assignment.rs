use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};
use std::os::unix::fs::{MetadataExt, PermissionsExt};
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    ClusterId, Condition, ConditionReason, ConditionState, ConditionType, Deployment, DeploymentId,
    DeploymentPhase, DeploymentSpec, DeploymentStatus, ExecPolicy, Generation, Node, NodeApiAccess,
    NodeId, NodeInstanceId, NodeRole, NodeSpec, NodeStatus, ObjectMeta, PlacementConstraint,
    ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName,
    ResourceRevision, SecretMountSpec, SecretValue, ServiceId, ServiceSpec, Timestamp,
    VolumeAccess, VolumeMountSpec, VolumeSource, WorkloadNetworkMode, WorkloadUserSpec,
};
use kernel_store::{
    Clock, DeleteRequest, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Store,
};
use runtime::{
    FakeNetworkProvider, FakeRuntime, FakeRuntimeOperation, NetworkAddressing, NetworkCidr,
    NetworkProvider, NetworkSpec, RuntimeError, ShutdownRequest, ValueSourceError,
    ValueSourceResolver, WorkloadIdMapping, WorkloadRuntime, WorkloadSpec, WorkloadUser,
    WorkloadUserNamespace,
};
use tokio::sync::{Notify, watch};

use crate::assignment::host_secret_owner;
use crate::{AssignmentAgent, AssignmentAgentSettings, NodeApiServices, StatusClock, WorkloadDns};

use super::store_fault::FailFirstListStore;

mod dns;
#[cfg(unix)]
mod node_api;

#[tokio::test]
async fn assignment_reconcile_runs_and_re_adopts_one_exactly_addressed_workload()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;

    let first = world.agent().reconcile_once().await?;
    assert_eq!(first.desired, 1);
    assert_eq!(first.running, 1);
    assert_eq!(world.network.lease_count(), 1);
    assert_running(&world).await?;

    let restarted = world.agent().reconcile_once().await?;
    assert_eq!(restarted.running, 1);
    assert_eq!(restarted.garbage_collected, 0);
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_publishes_a_runtime_delegated_address()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut assignment = assignment();
    assignment.spec.workload_address = None;
    world.seed(&deployment(), &assignment).await?;

    let report = world.agent_delegated().reconcile_once().await?;
    assert_eq!(report.running, 1);
    let stored = world.load_assignment().await?;
    assert_eq!(stored.spec.workload_address, None);
    assert_eq!(
        stored.status.workload_address,
        Some(IpAddr::V4(Ipv4Addr::new(192, 0, 2, 2)))
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_creates_and_reuses_missing_replica_state()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let assignment = assignment();
    world
        .seed_without_replica(&deployment(), &assignment)
        .await?;

    let first = world.agent().reconcile_once().await?;
    assert_eq!(first.replica_states_created, 1);
    let key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("ReplicaState")?,
        &ResourceName::new(assignment.meta.id.as_str())?,
    );
    let stored = world.store.get(&key).await?.ok_or("replica missing")?;
    let replica: ReplicaState = serde_json::from_slice(&stored.value)?;
    assert_eq!(replica.spec.assignment_id, assignment.meta.id);
    assert_eq!(replica.status.phase, DeploymentPhase::PendingReady);
    assert_eq!(replica.status.node_id, Some(node_id("node-1")));
    assert_eq!(
        replica.status.workload_id.as_ref().map(|id| id.as_str()),
        Some("assignment-1")
    );

    let second = world.agent().reconcile_once().await?;
    assert_eq!(second.replica_states_created, 0);
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_retries_transient_runtime_failure_from_pending_status()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    let agent = world.agent();
    world.runtime.fail_next(
        FakeRuntimeOperation::Start,
        RuntimeError::Unavailable {
            message: "injected outage".to_owned(),
        },
    )?;

    let first = agent.reconcile_once().await?;
    assert_eq!(first.unresolved, 1);
    let pending = world.load_assignment().await?;
    assert_eq!(pending.status.phase, AssignmentPhase::Pending);
    assert_eq!(
        world.load_replica().await?.status.phase,
        DeploymentPhase::Publishing
    );
    assert_eq!(
        pending
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("RetryBackoff")
    );

    let waiting = agent.reconcile_once().await?;
    assert_eq!(waiting.unresolved, 1);
    assert_eq!(world.load_assignment().await?.status, pending.status);

    world.status_clock.advance(Duration::from_secs(5));
    let resumed = agent.reconcile_once().await?;
    assert_eq!(resumed.running, 1);
    assert_running(&world).await?;
    Ok(())
}

#[tokio::test]
async fn new_user_deployment_fails_fast_while_an_older_deployment_is_ready()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    let mut previous = deployment();
    previous.meta.id = DeploymentId::new("deployment-previous")?;
    previous.spec.service_generation = Generation(0);
    previous.status.phase = DeploymentPhase::Ready;
    put_resource(
        world.store.as_ref(),
        Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::new(previous.meta.id.as_str())?,
        ),
        serde_json::to_vec(&previous)?,
    )
    .await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::Start,
        RuntimeError::Unavailable {
            message: "injected rollout failure".to_owned(),
        },
    )?;

    let report = world.agent().reconcile_once().await?;

    assert_eq!(report.requeue_at, None);
    assert_eq!(
        world.load_assignment().await?.status.phase,
        AssignmentPhase::Failed
    );
    Ok(())
}

#[tokio::test]
async fn user_convergence_failure_stops_after_ten_retries_without_a_fallback()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    let agent = world.agent();

    for expected_attempt in 1..=10 {
        world.runtime.fail_next(
            FakeRuntimeOperation::Start,
            RuntimeError::Unavailable {
                message: format!("injected convergence failure {expected_attempt}"),
            },
        )?;
        let report = agent.reconcile_once().await?;
        let retry_at = report.requeue_at.ok_or("retry deadline missing")?;
        assert_eq!(
            world.load_assignment().await?.status.phase,
            AssignmentPhase::Pending
        );
        let delay = retry_at.0.saturating_sub(world.status_clock.now().0);
        world
            .status_clock
            .advance(Duration::from_millis(u64::try_from(delay)?));
    }

    world.runtime.fail_next(
        FakeRuntimeOperation::Start,
        RuntimeError::Unavailable {
            message: "final convergence failure".to_owned(),
        },
    )?;
    let exhausted = agent.reconcile_once().await?;

    assert_eq!(exhausted.requeue_at, None);
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Failed);
    assert_eq!(
        assignment
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("RetryLimitReached")
    );
    Ok(())
}

#[tokio::test]
async fn zero_restart_attempts_makes_a_user_process_exit_terminal()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.max_restart_attempts = 0;
    world.seed(&deployment, &assignment()).await?;
    world.agent().reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;

    let report = world.agent().reconcile_once().await?;

    assert_eq!(report.unresolved, 1);
    assert_eq!(report.requeue_at, None);
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Failed);
    assert_eq!(
        assignment
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("WorkloadExited")
    );
    Ok(())
}

#[tokio::test]
async fn user_process_exit_retries_with_the_default_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    let agent = world.agent();
    agent.reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;

    let delayed = agent.reconcile_once().await?;
    assert_eq!(delayed.requeue_at, Some(Timestamp(1_750_000_005_000)));
    assert_eq!(
        world.load_assignment().await?.status.phase,
        AssignmentPhase::Pending
    );

    world.status_clock.advance(Duration::from_secs(5));
    let recovered = agent.reconcile_once().await?;
    assert_eq!(recovered.running, 1);
    assert_eq!(recovered.restarted, 1);
    Ok(())
}

#[tokio::test]
async fn user_process_exit_exhausts_the_configured_restart_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.max_restart_attempts = 2;
    world.seed(&deployment, &assignment()).await?;
    let agent = world.agent();
    agent.reconcile_once().await?;

    for _attempt in 1..=2 {
        let handle = world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .into_iter()
            .next()
            .ok_or("workload missing")?
            .handle;
        world
            .runtime
            .stop(
                &handle,
                ShutdownRequest {
                    timeout: Duration::from_secs(5),
                },
            )
            .await?;
        let delayed = agent.reconcile_once().await?;
        let retry_at = delayed.requeue_at.ok_or("retry deadline missing")?;
        let delay = retry_at.0.saturating_sub(world.status_clock.now().0);
        world
            .status_clock
            .advance(Duration::from_millis(u64::try_from(delay)?));
        assert_eq!(agent.reconcile_once().await?.restarted, 1);
    }

    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;
    let exhausted = agent.reconcile_once().await?;

    assert_eq!(exhausted.requeue_at, None);
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Failed);
    assert_eq!(
        assignment
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("RetryLimitReached")
    );
    Ok(())
}

#[tokio::test]
async fn daemon_restart_resets_the_volatile_user_restart_budget()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.max_restart_attempts = 1;
    world.seed(&deployment, &assignment()).await?;
    let first_agent = world.agent();
    first_agent.reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;

    assert_eq!(
        first_agent.reconcile_once().await?.requeue_at,
        Some(Timestamp(1_750_000_005_000))
    );
    drop(first_agent);

    let restarted_agent = world.agent();
    assert_eq!(
        restarted_agent.reconcile_once().await?.requeue_at,
        Some(Timestamp(1_750_000_005_000))
    );
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Pending);
    assert!(
        assignment
            .status
            .conditions
            .first()
            .is_some_and(|condition| condition.message.contains("retry attempt 1"))
    );
    Ok(())
}

#[tokio::test]
async fn system_process_exit_retries_with_volatile_backoff()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let (deployment, assignment) = system_workload();
    world.seed(&deployment, &assignment).await?;
    let agent = world.agent();
    agent.reconcile_once().await?;
    let handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .next()
        .ok_or("workload missing")?
        .handle;
    world
        .runtime
        .stop(
            &handle,
            ShutdownRequest {
                timeout: Duration::from_secs(5),
            },
        )
        .await?;

    let delayed = agent.reconcile_once().await?;
    assert_eq!(delayed.requeue_at, Some(Timestamp(1_750_000_005_000)));
    assert_eq!(
        world.load_assignment().await?.status.phase,
        AssignmentPhase::Pending
    );

    world.status_clock.advance(Duration::from_secs(5));
    let recovered = agent.reconcile_once().await?;
    assert_eq!(
        recovered.running,
        1,
        "assignment={:?}",
        world.load_assignment().await?
    );
    assert_eq!(recovered.restarted, 1);
    Ok(())
}

#[tokio::test]
async fn system_health_crash_restarts_and_resets_replica_health()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let (deployment, assignment) = system_workload();
    world.seed_without_replica(&deployment, &assignment).await?;
    let mut replica = replica(&assignment);
    replica.status.phase = DeploymentPhase::Crashed;
    replica.status.healthcheck_failures = 3;
    replica.status.conditions.push(Condition {
        condition_type: ConditionType::HealthReady,
        state: ConditionState::False,
        reason: ConditionReason("UnhealthyThresholdReached".to_owned()),
        message: "old workload failed its health check".to_owned(),
        observed_generation: Generation(1),
        last_transition_time: Timestamp(1_750_000_000_000),
    });
    put_resource(
        world.store.as_ref(),
        world.replica_key(),
        serde_json::to_vec(&replica)?,
    )
    .await?;
    let agent = world.agent();

    let delayed = agent.reconcile_once().await?;
    assert_eq!(delayed.requeue_at, Some(Timestamp(1_750_000_005_000)));

    world.status_clock.advance(Duration::from_secs(5));
    let recovered = agent.reconcile_once().await?;
    assert_eq!(recovered.restarted, 1);
    let replica = world.load_replica().await?;
    assert_eq!(replica.status.phase, DeploymentPhase::PendingReady);
    assert_eq!(replica.status.healthcheck_failures, 0);
    assert!(
        replica
            .status
            .conditions
            .iter()
            .all(|condition| condition.condition_type != ConditionType::HealthReady)
    );
    Ok(())
}

#[tokio::test]
async fn assignment_run_retries_a_transient_whole_snapshot_failure()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "injected list outage".to_owned(),
        },
    )?;
    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let agent = world.agent();
    let task = tokio::spawn(async move { agent.run(shutdown_rx).await });
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let lists = world
                .runtime
                .calls()
                .unwrap()
                .into_iter()
                .filter(|call| call.operation == FakeRuntimeOperation::List)
                .count();
            if lists >= 1 {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;

    world.monotonic_clock.advance(Duration::from_secs(1));
    tokio::time::timeout(Duration::from_secs(1), async {
        loop {
            let assignment = world.load_assignment().await.unwrap();
            if assignment.status.phase == AssignmentPhase::Running {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    let lists = world
        .runtime
        .calls()?
        .into_iter()
        .filter(|call| call.operation == FakeRuntimeOperation::List)
        .count();
    assert!(lists >= 2);
    assert_running(&world).await?;

    shutdown_tx.send(true)?;
    tokio::time::timeout(Duration::from_secs(1), task).await???;
    assert!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .is_empty()
    );
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Stopped);
    assert!(assignment.status.workload_id.is_none());
    let assignment_condition = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .ok_or("assignment stop condition missing")?;
    assert_eq!(assignment_condition.state, ConditionState::False);
    assert_eq!(assignment_condition.reason.0, "DaemonShutdown");
    let replica = world.load_replica().await?;
    assert_eq!(replica.status.phase, DeploymentPhase::Stopped);
    assert!(replica.status.workload_id.is_none());
    let replica_condition = replica
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .ok_or("replica stop condition missing")?;
    assert_eq!(replica_condition.state, ConditionState::False);
    assert_eq!(replica_condition.reason.0, "DaemonShutdown");

    let restarted = world.agent().reconcile_once().await?;
    assert_eq!(restarted.restarted, 1);
    assert_running(&world).await?;
    let replica = world.load_replica().await?;
    assert_eq!(replica.status.phase, DeploymentPhase::PendingReady);
    assert_eq!(replica.status.healthcheck_failures, 0);
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn planned_shutdown_records_node_maintenance_before_stopping_workloads()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    world.seed_maintenance_node().await?;

    world.agent().shutdown_local_workloads().await?;

    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Stopped);
    let condition = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .ok_or("assignment stop condition missing")?;
    assert_eq!(condition.reason.0, "NodeMaintenance");
    let replica = world.load_replica().await?;
    assert_eq!(replica.status.phase, DeploymentPhase::Stopped);
    assert_eq!(
        replica
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
            .map(|condition| condition.reason.0.as_str()),
        Some("NodeMaintenance")
    );
    Ok(())
}

#[tokio::test]
async fn planned_shutdown_records_stopping_when_runtime_inventory_is_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    world.runtime.fail_next(
        FakeRuntimeOperation::List,
        RuntimeError::Unavailable {
            message: "injected shutdown inventory outage".to_owned(),
        },
    )?;

    assert!(world.agent().shutdown_local_workloads().await.is_err());

    assert_eq!(
        world.load_assignment().await?.status.phase,
        AssignmentPhase::Stopping
    );
    assert_eq!(
        world.load_replica().await?.status.phase,
        DeploymentPhase::Stopping
    );
    assert_eq!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .len(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn planned_shutdown_still_removes_runtime_workloads_when_store_inventory_is_unavailable()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    world.seed(&deployment(), &assignment()).await?;
    world.agent().reconcile_once().await?;
    let unavailable = Arc::new(FailFirstListStore::new(world.store.clone()));

    assert!(
        world
            .agent_with_store(unavailable)
            .shutdown_local_workloads()
            .await
            .is_err()
    );

    assert!(
        world
            .runtime
            .list(&cluster_id(), &node_id("node-1"))
            .await?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn assignment_reconcile_mounts_and_cleans_private_secret_files()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: None,
        items: BTreeMap::from([("TOKEN".to_owned(), SecretValue::new("sensitive"))]),
    });
    world.seed(&deployment, &assignment()).await?;
    world.agent().reconcile_once().await?;
    let secret_path = world.secrets.path().join("assignment-1/secrets.env");
    assert_eq!(
        std::fs::read_to_string(&secret_path)?,
        "TOKEN=\"sensitive\"\n"
    );
    assert_eq!(
        std::fs::metadata(&secret_path)?.permissions().mode() & 0o777,
        0o444
    );

    let key = world.assignment_key();
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    world
        .store
        .delete_cas(DeleteRequest {
            key,
            expected: stored.version,
        })
        .await?;
    world.agent().reconcile_once().await?;
    assert!(!secret_path.exists());
    Ok(())
}

#[test]
fn assignment_maps_an_explicit_secret_owner_into_the_workload_user_namespace()
-> Result<(), Box<dyn std::error::Error>> {
    let namespace = WorkloadUserNamespace {
        uid: WorkloadIdMapping {
            container_id: 0,
            host_id: 1_000_000,
            size: 65_536,
        },
        gid: WorkloadIdMapping {
            container_id: 0,
            host_id: 2_000_000,
            size: 65_536,
        },
    };
    assert_eq!(
        host_secret_owner(
            Some(namespace),
            Some(WorkloadUser {
                user_id: 1_000,
                group_id: 1_001,
            }),
        )?,
        Some(WorkloadUser {
            user_id: 1_001_000,
            group_id: 2_001_001,
        })
    );
    assert_eq!(host_secret_owner(Some(namespace), None)?, None);
    Ok(())
}

#[tokio::test]
async fn assignment_resolves_external_values_only_at_workload_creation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.environment.clear();
    deployment.spec.service.environment_sources =
        vec!["aws-secret://runtime-environment".to_owned()];
    deployment.spec.environment_template.ingress_host =
        Some("api-pr-42.preview.example.test".to_owned());
    deployment.spec.environment_template.ingress_port = Some(8080);
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: Some("aws-secret://runtime-secrets".to_owned()),
        items: BTreeMap::new(),
    });
    world.seed(&deployment, &assignment()).await?;

    let stored_before = world.load_deployment().await?;
    let encoded_before = serde_json::to_string(&stored_before)?;
    assert!(encoded_before.contains("aws-secret://runtime-environment"));
    assert!(encoded_before.contains("aws-secret://runtime-secrets"));
    assert!(!encoded_before.contains("resolved-mode"));
    assert!(!encoded_before.contains("resolved-token"));

    let resolver = Arc::new(FakeValueSources {
        values: BTreeMap::from([
            (
                "aws-secret://runtime-environment".to_owned(),
                BTreeMap::from([
                    ("MODE".to_owned(), SecretValue::new("resolved-mode")),
                    (
                        "BATON_HOST".to_owned(),
                        SecretValue::new("https://${{ MAESTRO_INGRESS_HOST }}/"),
                    ),
                    (
                        "INGRESS_PORT".to_owned(),
                        SecretValue::new("${{ MAESTRO_INGRESS_PORT }}"),
                    ),
                ]),
            ),
            (
                "aws-secret://runtime-secrets".to_owned(),
                BTreeMap::from([
                    (
                        "BATON_HOST".to_owned(),
                        SecretValue::new("https://${{ MAESTRO_INGRESS_HOST }}/"),
                    ),
                    (
                        "INGRESS_PORT".to_owned(),
                        SecretValue::new("${{ MAESTRO_INGRESS_PORT }}"),
                    ),
                    ("TOKEN".to_owned(), SecretValue::new("resolved-token")),
                ]),
            ),
        ]),
        calls: AtomicU64::new(0),
    });
    let agent = world.agent().with_value_source_resolver(resolver.clone());
    agent.reconcile_once().await?;
    agent.reconcile_once().await?;

    assert_eq!(resolver.calls.load(Ordering::SeqCst), 2);
    assert_eq!(
        std::fs::read_to_string(world.secrets.path().join("assignment-1/secrets.env"))?,
        "BATON_HOST=\"https://api-pr-42.preview.example.test/\"\nINGRESS_PORT=\"8080\"\nTOKEN=\"resolved-token\"\n"
    );
    let spec = world
        .runtime
        .workload_spec(&kernel_api::WorkloadId::new("assignment-1")?)?
        .ok_or("workload spec missing")?;
    let WorkloadSpec::Container(workload) = spec else {
        return Err("expected container workload".into());
    };
    assert_eq!(
        workload
            .configuration
            .environment
            .get("MODE")
            .map(SecretValue::expose),
        Some("resolved-mode")
    );
    assert_eq!(
        workload
            .configuration
            .environment
            .get("BATON_HOST")
            .map(SecretValue::expose),
        Some("https://api-pr-42.preview.example.test/")
    );
    assert_eq!(
        workload
            .configuration
            .environment
            .get("INGRESS_PORT")
            .map(SecretValue::expose),
        Some("8080")
    );

    let replica = world.load_replica().await?;
    assert_eq!(
        replica
            .status
            .resolved_secrets
            .as_ref()
            .and_then(|secrets| secrets.get("TOKEN"))
            .map(kernel_api::MaskedSecret::as_str),
        Some("••••oken")
    );
    let encoded_replica = serde_json::to_string(&replica)?;
    assert!(encoded_replica.contains("••••oken"));
    assert!(!encoded_replica.contains("resolved-token"));

    let encoded_after = serde_json::to_string(&world.load_deployment().await?)?;
    assert!(encoded_after.contains("aws-secret://runtime-environment"));
    assert!(encoded_after.contains("aws-secret://runtime-secrets"));
    assert!(!encoded_after.contains("resolved-mode"));
    assert!(!encoded_after.contains("resolved-token"));
    Ok(())
}

#[tokio::test]
async fn rejected_external_secret_enters_bounded_convergence_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: Some("aws-secret://missing".to_owned()),
        items: BTreeMap::new(),
    });
    world.seed(&deployment, &assignment()).await?;
    let resolver = Arc::new(FakeValueSources {
        values: BTreeMap::new(),
        calls: AtomicU64::new(0),
    });

    world
        .agent()
        .with_value_source_resolver(resolver)
        .reconcile_once()
        .await?;

    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Pending);
    let failure = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .ok_or("runtime failure condition missing")?;
    assert_eq!(failure.reason.0, "RetryBackoff");
    assert!(failure.message.contains("aws-secret://missing"));
    Ok(())
}

#[tokio::test]
async fn unavailable_external_secret_enters_bounded_convergence_retry()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let mut deployment = deployment();
    deployment.spec.service.secrets = Some(SecretMountSpec::Dotenv {
        mount_path: "/run/secrets/maestro.env".to_owned(),
        source: Some("aws-secret://unavailable".to_owned()),
        items: BTreeMap::new(),
    });
    world.seed(&deployment, &assignment()).await?;

    world
        .agent()
        .with_value_source_resolver(Arc::new(UnavailableValueSource))
        .reconcile_once()
        .await?;

    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Pending);
    let failure = assignment
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .ok_or("runtime failure condition missing")?;
    assert_eq!(failure.reason.0, "RetryBackoff");
    assert!(
        failure
            .message
            .contains("AWS Secrets Manager is unavailable")
    );
    Ok(())
}

struct UnavailableValueSource;

#[async_trait]
impl ValueSourceResolver for UnavailableValueSource {
    async fn resolve(
        &self,
        _source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
        Err(ValueSourceError::Unavailable {
            message: "AWS Secrets Manager is unavailable".to_owned(),
        })
    }
}

struct FakeValueSources {
    values: BTreeMap<String, BTreeMap<String, SecretValue>>,
    calls: AtomicU64,
}

#[async_trait]
impl ValueSourceResolver for FakeValueSources {
    async fn resolve(
        &self,
        source: &str,
    ) -> Result<BTreeMap<String, SecretValue>, ValueSourceError> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        self.values
            .get(source)
            .cloned()
            .ok_or_else(|| ValueSourceError::Rejected {
                message: format!("missing fake source `{source}`"),
            })
    }
}

pub(crate) fn cluster_id() -> ClusterId {
    ClusterId::new("cluster-1").unwrap()
}

pub(crate) fn node_id(value: &str) -> NodeId {
    NodeId::new(value).unwrap()
}

pub(crate) fn assignment() -> Assignment {
    Assignment {
        meta: ObjectMeta {
            id: AssignmentId::new("assignment-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: AssignmentSpec {
            service_id: ServiceId::new("api").unwrap(),
            deployment_id: DeploymentId::new("deployment-1").unwrap(),
            restart_generation: Generation(1),
            replica_index: 0,
            node_id: node_id("node-1"),
            placement_epoch: 1,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 8))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase: AssignmentPhase::Pending,
            workload_id: None,
            workload_address: None,
            conditions: Vec::new(),
        },
    }
}

pub(crate) fn deployment() -> Deployment {
    Deployment {
        meta: ObjectMeta {
            id: DeploymentId::new("deployment-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: DeploymentSpec {
            service_id: ServiceId::new("api").unwrap(),
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            goal: kernel_api::DeploymentGoal::Run,
            service: ServiceSpec {
                name: "API".to_owned(),
                version: "1.0.0".to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: "registry.test/api:latest".to_owned(),
                },
                preview: None,
                command: None,
                replicas: 1,
                exposed_ports: vec![8080],
                health_check: None,
                max_restart_attempts: kernel_api::DEFAULT_MAX_RESTART_ATTEMPTS,
                environment: BTreeMap::from([("MODE".to_owned(), "production".to_owned())]),
                environment_sources: Vec::new(),
                user: None,
                node_api: kernel_api::NodeApiAccess::Disabled,
                secrets: None,
                volumes: vec![VolumeMountSpec {
                    source: VolumeSource::HostPath {
                        path: "/srv/api".to_owned(),
                        node_id: node_id("node-1"),
                    },
                    target: "/data".to_owned(),
                    access: VolumeAccess::ReadOnly,
                }],
                placement: PlacementConstraint::default(),
                exec: ExecPolicy::Allowed,
            },
            environment_template: Default::default(),
            build_id: None,
        },
        status: DeploymentStatus {
            phase: DeploymentPhase::Publishing,
            created_at: Timestamp(1_750_000_000_000),
            ready_at: None,
            draining_at: None,
            image_digest: Some(
                "registry.test/api@sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
                    .to_owned(),
            ),
            git_commit: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    }
}

fn system_workload() -> (Deployment, Assignment) {
    let service_id = ServiceId::new(kernel_api::TAILSCALE_GATEWAY_SERVICE_ID).unwrap();
    let mut deployment = deployment();
    deployment.spec.service_id = service_id.clone();
    let mut assignment = assignment();
    assignment.spec.service_id = service_id;
    (deployment, assignment)
}

async fn assert_running(world: &World) -> Result<(), Box<dyn std::error::Error>> {
    let assignment = world.load_assignment().await?;
    assert_eq!(assignment.status.phase, AssignmentPhase::Running);
    assert_eq!(
        assignment.status.workload_id.as_ref().map(|id| id.as_str()),
        Some("assignment-1")
    );
    assert_eq!(
        assignment.status.workload_address,
        assignment.spec.workload_address
    );
    assert_eq!(
        assignment
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("WorkloadRunning")
    );
    Ok(())
}

pub(super) struct World {
    pub(super) store: Arc<InMemoryStore>,
    pub(super) runtime: Arc<FakeRuntime>,
    pub(super) network: Arc<FakeNetworkProvider>,
    monotonic_clock: Arc<TestMonotonicClock>,
    status_clock: Arc<TestStatusClock>,
    secrets: tempfile::TempDir,
    node_api: tempfile::TempDir,
}

impl World {
    pub(super) fn new() -> Self {
        let monotonic_clock = Arc::new(TestMonotonicClock::default());
        Self {
            store: Arc::new(InMemoryStore::new(monotonic_clock.clone())),
            runtime: Arc::new(FakeRuntime::new()),
            network: Arc::new(FakeNetworkProvider::default()),
            monotonic_clock,
            status_clock: Arc::new(TestStatusClock::new(1_750_000_000_000)),
            secrets: tempfile::tempdir().unwrap(),
            node_api: tempfile::tempdir().unwrap(),
        }
    }

    pub(super) fn agent(&self) -> AssignmentAgent {
        self.agent_with_node_api(None)
    }

    fn agent_with_store(&self, store: Arc<dyn Store>) -> AssignmentAgent {
        self.agent_with_network_and_store(
            NetworkSpec {
                name: "maestro-node-1".to_owned(),
                addressing: NetworkAddressing::Managed {
                    range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24).unwrap(),
                    gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                },
                mtu_bytes: 1_420,
            },
            Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1))),
            None,
            store,
        )
    }

    fn agent_delegated(&self) -> AssignmentAgent {
        self.agent_with_network(
            NetworkSpec {
                name: "maestro-dev".to_owned(),
                addressing: NetworkAddressing::Delegated,
                mtu_bytes: 1_500,
            },
            None,
            None,
        )
    }

    fn agent_with_node_api(&self, services: Option<NodeApiServices>) -> AssignmentAgent {
        self.agent_with_network(
            NetworkSpec {
                name: "maestro-node-1".to_owned(),
                addressing: NetworkAddressing::Managed {
                    range: NetworkCidr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 0)), 24).unwrap(),
                    gateway: IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1)),
                },
                mtu_bytes: 1_420,
            },
            Some(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 1))),
            services,
        )
    }

    fn agent_with_network(
        &self,
        network_spec: NetworkSpec,
        dns_server: Option<IpAddr>,
        services: Option<NodeApiServices>,
    ) -> AssignmentAgent {
        self.agent_with_network_and_store(network_spec, dns_server, services, self.store.clone())
    }

    fn agent_with_network_and_store(
        &self,
        network_spec: NetworkSpec,
        dns_server: Option<IpAddr>,
        services: Option<NodeApiServices>,
        store: Arc<dyn Store>,
    ) -> AssignmentAgent {
        let runtime: Arc<dyn WorkloadRuntime> = self.runtime.clone();
        let network: Arc<dyn NetworkProvider> = self.network.clone();
        AssignmentAgent::new(
            store,
            runtime,
            network,
            AssignmentAgentSettings {
                cluster_id: cluster_id(),
                node_id: node_id("node-1"),
                network: network_spec,
                dns: dns_server.map_or(WorkloadDns::Disabled, WorkloadDns::Static),
                system_host_ports: Default::default(),
                stop_timeout: Duration::from_secs(5),
                resync_interval: Duration::from_secs(30),
                restart_backoff_base: Duration::from_secs(5),
                restart_backoff_max: Duration::from_secs(60),
                reconcile_timeout: Duration::from_secs(10),
                secrets_root: self.secrets.path().to_path_buf(),
                node_api_root: self.node_api.path().join("mounts"),
            },
            services,
            self.monotonic_clock.clone(),
            self.status_clock.clone(),
        )
        .unwrap()
    }

    pub(super) async fn seed(
        &self,
        deployment: &Deployment,
        assignment: &Assignment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        self.seed_without_replica(deployment, assignment).await?;
        put_resource(
            self.store.as_ref(),
            self.replica_key(),
            serde_json::to_vec(&replica(assignment))?,
        )
        .await
    }

    async fn seed_without_replica(
        &self,
        deployment: &Deployment,
        assignment: &Assignment,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let keyspace = Keyspace::new(&cluster_id());
        put_resource(
            self.store.as_ref(),
            keyspace.resource(
                &ResourceKind::new("Deployment")?,
                &ResourceName::new(deployment.meta.id.as_str())?,
            ),
            serde_json::to_vec(deployment)?,
        )
        .await?;
        put_resource(
            self.store.as_ref(),
            self.assignment_key(),
            serde_json::to_vec(assignment)?,
        )
        .await
    }

    async fn seed_maintenance_node(&self) -> Result<(), Box<dyn std::error::Error>> {
        let node: Node = kernel_api::Object {
            meta: ObjectMeta {
                id: node_id("node-1"),
                labels: BTreeMap::new(),
                annotations: BTreeMap::new(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: NodeSpec {
                hostname: "node-1.internal".to_owned(),
                host_address: IpAddr::V4(Ipv4Addr::new(10, 0, 0, 1)),
                role: NodeRole::Worker,
                workload_network_mode: WorkloadNetworkMode::ClusterRouted,
                scheduling_labels: BTreeMap::new(),
            },
            status: NodeStatus {
                instance_id: NodeInstanceId::new("instance-1")?,
                version: "1.0.0".to_owned(),
                last_seen: Timestamp(1_750_000_000_000),
                conditions: vec![Condition {
                    condition_type: ConditionType::Maintenance,
                    state: ConditionState::True,
                    reason: ConditionReason("UpgradeRun:upgrade-1".to_owned()),
                    message: "upgrade is in progress".to_owned(),
                    observed_generation: Generation(1),
                    last_transition_time: Timestamp(1_750_000_000_000),
                }],
            },
        };
        put_resource(
            self.store.as_ref(),
            Keyspace::new(&cluster_id())
                .resource(&ResourceKind::new("Node")?, &ResourceName::new("node-1")?),
            serde_json::to_vec(&node)?,
        )
        .await
    }

    async fn load_assignment(&self) -> Result<Assignment, Box<dyn std::error::Error>> {
        let stored = self
            .store
            .get(&self.assignment_key())
            .await?
            .ok_or("assignment missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    async fn load_deployment(&self) -> Result<Deployment, Box<dyn std::error::Error>> {
        let key = Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("Deployment")?,
            &ResourceName::new("deployment-1")?,
        );
        let stored = self.store.get(&key).await?.ok_or("deployment missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    async fn load_replica(&self) -> Result<ReplicaState, Box<dyn std::error::Error>> {
        let stored = self
            .store
            .get(&self.replica_key())
            .await?
            .ok_or("replica missing")?;
        Ok(serde_json::from_slice(&stored.value)?)
    }

    pub(super) fn assignment_key(&self) -> kernel_store::StoreKey {
        Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("Assignment").unwrap(),
            &ResourceName::new("assignment-1").unwrap(),
        )
    }

    fn replica_key(&self) -> kernel_store::StoreKey {
        Keyspace::new(&cluster_id()).resource(
            &ResourceKind::new("ReplicaState").unwrap(),
            &ResourceName::new("assignment-1").unwrap(),
        )
    }
}

fn replica(assignment: &Assignment) -> ReplicaState {
    ReplicaState {
        meta: ObjectMeta {
            id: ReplicaStateId::new("assignment-1").unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ReplicaStateSpec {
            service_id: assignment.spec.service_id.clone(),
            deployment_id: assignment.spec.deployment_id.clone(),
            assignment_id: assignment.meta.id.clone(),
            replica_index: assignment.spec.replica_index,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Publishing,
            node_id: Some(assignment.spec.node_id.clone()),
            workload_id: None,
            healthcheck_failures: 0,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    }
}

pub(super) async fn put_resource(
    store: &dyn Store,
    key: kernel_store::StoreKey,
    value: Vec<u8>,
) -> Result<(), Box<dyn std::error::Error>> {
    store
        .put_cas(PutRequest {
            key,
            value,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await?;
    Ok(())
}

#[derive(Default)]
struct TestMonotonicClock {
    milliseconds: AtomicU64,
    changed: Notify,
}

impl TestMonotonicClock {
    fn advance(&self, duration: Duration) {
        let milliseconds = u64::try_from(duration.as_millis()).unwrap();
        self.milliseconds.fetch_add(milliseconds, Ordering::SeqCst);
        self.changed.notify_waiters();
    }
}

#[async_trait]
impl Clock for TestMonotonicClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::from_duration(Duration::from_millis(
            self.milliseconds.load(Ordering::SeqCst),
        ))
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        loop {
            let changed = self.changed.notified();
            if self.now() >= deadline {
                return;
            }
            changed.await;
        }
    }
}

struct TestStatusClock(AtomicI64);

impl TestStatusClock {
    fn new(milliseconds: i64) -> Self {
        Self(AtomicI64::new(milliseconds))
    }

    fn advance(&self, duration: Duration) {
        let milliseconds = i64::try_from(duration.as_millis()).unwrap();
        self.0.fetch_add(milliseconds, Ordering::SeqCst);
    }
}

impl StatusClock for TestStatusClock {
    fn now(&self) -> Timestamp {
        Timestamp(self.0.load(Ordering::SeqCst))
    }
}
