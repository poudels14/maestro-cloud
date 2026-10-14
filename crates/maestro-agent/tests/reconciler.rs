use std::sync::Arc;
use std::time::Duration;

use maestro_agent::{LocalPlanSource, Reconciler, ReconcilerSettings, StatusBoard};
use maestro_core::{
    Assignment, ByteSize, DeploySpec, Deployment, DeploymentId, DeploymentStatus,
    EffectiveResources, Millicores, NodeId, ReplicaId, ReplicaPhase, ResolvedRequest, SecretsSpec,
    ServiceId, ServiceSpec, VolumeMount,
};
use maestro_runtime::FakeRuntime;
use maestro_store::{Mutation, StateMachine};
use tempfile::TempDir;

const NODE: NodeId = NodeId(1);
const IMAGE: &str = "nginx:1.27";

struct Harness {
    _directory: TempDir,
    state: Arc<StateMachine>,
    runtime: Arc<FakeRuntime>,
    board: StatusBoard,
    reconciler: Reconciler<FakeRuntime, LocalPlanSource>,
}

fn harness(max_restarts: Option<u32>) -> Harness {
    harness_with(
        DeploySpec {
            max_restarts,
            ..DeploySpec::default()
        },
        ReconcilerSettings::default(),
    )
}

fn harness_with(deploy: DeploySpec, settings: ReconcilerSettings) -> Harness {
    let directory = tempfile::tempdir().unwrap();
    let state = Arc::new(StateMachine::open(directory.path().join("state.redb")).unwrap());
    let spec = ServiceSpec {
        name: "API".to_string(),
        image: Some(IMAGE.to_string()),
        build: None,
        deploy,
        ingress: None,
        preview: None,
    };
    state
        .apply(&[
            Mutation::PutService {
                id: ServiceId::from("api"),
                spec: spec.clone(),
            },
            Mutation::CreateDeployment {
                deployment: Deployment {
                    id: DeploymentId::from("d1"),
                    service_id: ServiceId::from("api"),
                    spec,
                    status: DeploymentStatus::Deploying,
                    image: Some(IMAGE.to_string()),
                    failure: None,
                    created_at_ms: 1,
                    updated_at_ms: 1,
                    git_commit: None,
                    built_on: None,
                },
            },
        ])
        .unwrap();
    let runtime = Arc::new(FakeRuntime::default());
    let board = StatusBoard::new();
    let reconciler = Reconciler::new(
        runtime.clone(),
        Arc::new(LocalPlanSource::new(state.clone(), test_key(), NODE)),
        board.clone(),
        settings,
    );
    Harness {
        _directory: directory,
        state,
        runtime,
        board,
        reconciler,
    }
}

fn test_key() -> Arc<maestro_store::SecretKey> {
    Arc::new(maestro_store::SecretKey::from_bytes(&[5u8; 32]))
}

fn assignment(replica: &str, cpu: u32) -> Assignment {
    Assignment {
        replica_id: ReplicaId::from(replica),
        service_id: ServiceId::from("api"),
        deployment_id: DeploymentId::from("d1"),
        node_id: NODE,
        index: 0,
        image: IMAGE.to_string(),
        resources: EffectiveResources {
            cpu_limit: Millicores::new(cpu),
            memory_limit: ByteSize::new(4 << 30),
            request: ResolvedRequest {
                cpu: Millicores::new(250),
                memory: ByteSize::new(512 << 20),
            },
        },
        retiring_since_ms: None,
    }
}

fn assign(harness: &Harness, assignments: Vec<Assignment>) {
    harness
        .state
        .apply(&[Mutation::SetNodeAssignments {
            node_id: NODE,
            assignments,
        }])
        .unwrap();
}

fn phase(harness: &Harness, replica: &str) -> Option<ReplicaPhase> {
    harness
        .board
        .get(&ReplicaId::from(replica))
        .map(|status| status.phase)
}

#[tokio::test(start_paused = true)]
async fn starts_assigned_replicas_with_limits() {
    let mut harness = harness(None);
    assign(
        &harness,
        vec![assignment("d1-0", 2000), assignment("d1-1", 2000)],
    );

    harness.reconciler.reconcile().await.unwrap();

    let spec = harness.runtime.spec("d1-0").unwrap();
    assert_eq!(spec.limits.cpu, Millicores::new(2000));
    assert_eq!(spec.limits.memory, ByteSize::new(4 << 30));
    assert_eq!(spec.cgroup_parent, "maestro-workloads.slice");
    assert_eq!(spec.labels["maestro.service"], "api");
    assert_eq!(phase(&harness, "d1-0"), Some(ReplicaPhase::Healthy));
    assert!(
        harness
            .board
            .get(&ReplicaId::from("d1-1"))
            .unwrap()
            .ip
            .is_some()
    );
}

#[tokio::test(start_paused = true)]
async fn restarts_exited_replicas_with_backoff() {
    let mut harness = harness(None);
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    harness.runtime.exit("d1-0", 1);
    harness.reconciler.reconcile().await.unwrap();
    assert_eq!(phase(&harness, "d1-0"), Some(ReplicaPhase::Restarting));
    assert_eq!(harness.runtime.start_count("d1-0"), 1);

    harness.reconciler.reconcile().await.unwrap();
    assert_eq!(harness.runtime.start_count("d1-0"), 1);

    tokio::time::advance(Duration::from_secs(6)).await;
    harness.reconciler.reconcile().await.unwrap();
    assert_eq!(harness.runtime.start_count("d1-0"), 2);
    assert_eq!(phase(&harness, "d1-0"), Some(ReplicaPhase::Healthy));
    assert_eq!(
        harness
            .board
            .get(&ReplicaId::from("d1-0"))
            .unwrap()
            .restarts,
        1
    );
}

#[tokio::test(start_paused = true)]
async fn gives_up_after_max_restarts() {
    let mut harness = harness(Some(1));
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    harness.runtime.exit("d1-0", 1);
    harness.reconciler.reconcile().await.unwrap();
    tokio::time::advance(Duration::from_secs(6)).await;
    harness.reconciler.reconcile().await.unwrap();
    harness.runtime.exit("d1-0", 1);
    harness.reconciler.reconcile().await.unwrap();

    assert_eq!(phase(&harness, "d1-0"), Some(ReplicaPhase::Failed));
    tokio::time::advance(Duration::from_secs(600)).await;
    harness.reconciler.reconcile().await.unwrap();
    assert_eq!(harness.runtime.start_count("d1-0"), 2);
}

#[tokio::test(start_paused = true)]
async fn retires_unassigned_replicas() {
    let mut harness = harness(None);
    assign(
        &harness,
        vec![assignment("d1-0", 1000), assignment("d1-1", 1000)],
    );
    harness.reconciler.reconcile().await.unwrap();

    assign(&harness, vec![assignment("d1-1", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    assert!(harness.runtime.spec("d1-0").is_none());
    assert!(harness.board.get(&ReplicaId::from("d1-0")).is_none());
    assert!(harness.runtime.spec("d1-1").is_some());
}

#[tokio::test(start_paused = true)]
async fn recreates_replica_when_plan_changes() {
    let mut harness = harness(None);
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    assign(&harness, vec![assignment("d1-0", 3000)]);
    harness.reconciler.reconcile().await.unwrap();

    assert_eq!(harness.runtime.start_count("d1-0"), 2);
    assert_eq!(
        harness.runtime.spec("d1-0").unwrap().limits.cpu,
        Millicores::new(3000)
    );

    harness
        .reconciler
        .record_event(maestro_runtime::RuntimeEvent::Exited {
            id: "d1-0".to_string(),
            exit_code: 0,
        });
    assert_eq!(phase(&harness, "d1-0"), Some(ReplicaPhase::Healthy));
}

#[tokio::test(start_paused = true)]
async fn reports_pull_failures_and_retries() {
    let mut harness = harness(None);
    harness.runtime.fail_image(IMAGE);
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    let status = harness.board.get(&ReplicaId::from("d1-0")).unwrap();
    assert_eq!(status.phase, ReplicaPhase::Restarting);
    assert!(status.last_error.unwrap().contains("fake pull failure"));
    assert_eq!(harness.runtime.start_count("d1-0"), 0);
}

#[tokio::test(start_paused = true)]
async fn adopts_running_replicas_after_agent_restart() {
    let mut harness = harness(None);
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();
    let ip = harness.board.get(&ReplicaId::from("d1-0")).unwrap().ip;

    let restarted_board = StatusBoard::new();
    let mut restarted = Reconciler::new(
        harness.runtime.clone(),
        Arc::new(LocalPlanSource::new(
            harness.state.clone(),
            test_key(),
            NODE,
        )),
        restarted_board.clone(),
        ReconcilerSettings::default(),
    );
    restarted.reconcile().await.unwrap();

    let adopted = restarted_board.get(&ReplicaId::from("d1-0")).unwrap();
    assert_eq!(adopted.phase, ReplicaPhase::Healthy);
    assert_eq!(adopted.ip, ip);
    assert_eq!(harness.runtime.start_count("d1-0"), 1);
}

#[tokio::test(start_paused = true)]
async fn mounts_volumes_and_a_private_secrets_file() {
    let scratch = tempfile::tempdir().unwrap();
    let secrets_dir = scratch.path().join("secrets");
    let volume = scratch.path().join("data");
    let deploy = DeploySpec {
        volumes: vec![VolumeMount {
            host_path: volume.to_string_lossy().to_string(),
            mount_path: "/data".to_string(),
            read_only: false,
            owner: None,
        }],
        secrets: Some(SecretsSpec {
            mount_path: "/run/app.env".to_string(),
            source: None,
            items: [("TOKEN".to_string(), "s3cret".to_string())].into(),
        }),
        ..DeploySpec::default()
    };
    let mut harness = harness_with(
        deploy,
        ReconcilerSettings {
            secrets_dir: secrets_dir.clone(),
            ..ReconcilerSettings::default()
        },
    );
    assign(&harness, vec![assignment("d1-0", 1000)]);
    harness.reconciler.reconcile().await.unwrap();

    let mounts = harness.runtime.spec("d1-0").unwrap().mounts;
    assert!(volume.is_dir());
    assert_eq!(mounts[0].destination, "/data");
    assert!(!mounts[0].read_only);
    assert_eq!(mounts[1].destination, "/run/app.env");
    assert!(mounts[1].read_only);
    assert_eq!(
        std::fs::read_to_string(&mounts[1].source).unwrap(),
        "TOKEN=\"s3cret\"\n"
    );

    assign(&harness, vec![]);
    harness.reconciler.reconcile().await.unwrap();
    assert!(!mounts[1].source.exists());
}
