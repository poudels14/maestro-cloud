use std::collections::{BTreeMap, BTreeSet};
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    ArtifactTemplate, AssignmentId, Build, BuildId, BuildPhase, BuildSource, BuildTemplate,
    ClusterId, Deployment, DeploymentId, DeploymentPhase, ExecPolicy, Generation, NodeApiAccess,
    NodeId, NodeInstanceId, Object, ObjectMeta, PlacementConstraint, ReplicaState, ReplicaStateId,
    ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName, ResourceRevision,
    RolloutState, Service, ServiceId, ServiceSpec, ServiceStatus, Timestamp,
};
use kernel_controller::{
    Backoff, FencedStore, LeaderIdentity, LeadershipToken, RuntimeConfig, TimestampClock,
};
use kernel_store::{
    CasOutcome, Clock, ExpectedVersion, InMemoryStore, Keyspace, MonotonicTime, PutRequest,
    Session, SessionBinding, Store,
};

use crate::snapshot::ResourceSnapshot;
use crate::writer::DeploymentWriter;
use crate::{DeploymentController, DeploymentReconciler, LifecycleSettings, plan};

#[tokio::test]
async fn store_backed_controller_advances_only_from_exact_replica_state()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(image_service()).await?;
    let created = world.reconcile(Timestamp(1_000)).await?;
    assert_eq!(created.created_deployments, 1);
    let mut deployment = world.one::<Deployment>("Deployment").await?;
    assert_eq!(deployment.status.phase, DeploymentPhase::Queued);

    let building = world.reconcile(Timestamp(2_000)).await?;
    assert_eq!(building.updated_deployments, 1);
    deployment = world.one::<Deployment>("Deployment").await?;
    assert_eq!(deployment.status.phase, DeploymentPhase::Building);

    let assignment = crate::tests::plan_support::assignment(&deployment, "assignment-1", 1);
    world
        .put("Assignment", &assignment.meta.id, &assignment)
        .await?;
    world.reconcile(Timestamp(3_000)).await?;
    deployment = world.one::<Deployment>("Deployment").await?;
    assert_eq!(deployment.status.phase, DeploymentPhase::PendingReady);

    let replica = ready_replica(&deployment, &assignment.meta.id);
    world
        .put("ReplicaState", &replica.meta.id, &replica)
        .await?;
    let ready = world.reconcile(Timestamp(4_000)).await?;
    assert_eq!((ready.updated_deployments, ready.updated_services), (1, 1));
    deployment = world.one::<Deployment>("Deployment").await?;
    let service = world.one::<Service>("Service").await?;
    assert_eq!(deployment.status.phase, DeploymentPhase::Ready);
    assert_eq!(deployment.status.ready_at, Some(Timestamp(4_000)));
    assert_eq!(
        service.status.active_deployment_id,
        Some(deployment.meta.id)
    );
    Ok(())
}

#[tokio::test]
async fn atomic_writer_conflict_creates_no_partial_lifecycle_generation()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(image_service()).await?;
    let service_id = ServiceId::new("api")?;
    let snapshot = ResourceSnapshot::load_service(&world.fenced, &world.keys, &service_id).await?;
    let desired = plan(snapshot.input(world.cluster_id.clone(), Timestamp(1_000), settings()))?;

    world
        .update::<Service>("Service", "api", |service| {
            service.status.rollout = RolloutState::Frozen;
        })
        .await?;
    let writer = DeploymentWriter::new(&world.cluster_id)?;
    let report = writer.apply(&world.fenced, &snapshot, &desired).await?;
    assert!(report.conflict);
    assert!(world.list::<Deployment>("Deployment").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn service_snapshot_excludes_unrelated_replica_cardinality()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(image_service()).await?;
    let mut unrelated_service = image_service();
    unrelated_service.meta.id = ServiceId::new("unrelated")?;
    world
        .put("Service", &unrelated_service.meta.id, &unrelated_service)
        .await?;
    let unrelated_deployment =
        crate::tests::plan_support::deployment(&unrelated_service, DeploymentPhase::Removed);
    world
        .put(
            "Deployment",
            &unrelated_deployment.meta.id,
            &unrelated_deployment,
        )
        .await?;
    for index in 0..130 {
        let assignment_id = AssignmentId::new(format!("unrelated-{index}"))?;
        let replica = ready_replica(&unrelated_deployment, &assignment_id);
        world
            .put("ReplicaState", &replica.meta.id, &replica)
            .await?;
    }

    let service_id = ServiceId::new("api")?;
    let snapshot = ResourceSnapshot::load_service(&world.fenced, &world.keys, &service_id).await?;

    assert_eq!(snapshot.services.len(), 1);
    assert!(snapshot.replicas.is_empty());
    assert_eq!(snapshot.primary_compares().len(), 1);
    assert_eq!(
        world.reconcile(Timestamp(1_000)).await?.created_deployments,
        1
    );
    Ok(())
}

#[tokio::test]
async fn stale_replica_history_is_collected_in_bounded_batches()
-> Result<(), Box<dyn std::error::Error>> {
    let service = image_service();
    let removed = crate::tests::plan_support::deployment(&service, DeploymentPhase::Removed);
    let world = World::new(service).await?;
    world.put("Deployment", &removed.meta.id, &removed).await?;
    for index in 0..130 {
        let assignment_id = AssignmentId::new(format!("stale-{index}"))?;
        let replica = ready_replica(&removed, &assignment_id);
        world
            .put("ReplicaState", &replica.meta.id, &replica)
            .await?;
    }

    let report = world.reconcile(Timestamp(1_000)).await?;

    assert_eq!(report.deleted_replicas, 130);
    assert!(world.list::<ReplicaState>("ReplicaState").await?.is_empty());
    Ok(())
}

#[tokio::test]
async fn store_backed_watched_commit_advances_after_pinned_deployment_exists()
-> Result<(), Box<dyn std::error::Error>> {
    let mut watched_service = build_service();
    let ArtifactTemplate::Build { template } = &mut watched_service.spec.artifact else {
        return Ok(());
    };
    template.watch = true;
    let world = World::new(watched_service).await?;
    world.reconcile(Timestamp(1_000)).await?;
    world.reconcile(Timestamp(2_000)).await?;
    let initial_deployment = world.one::<Deployment>("Deployment").await?;
    world
        .update::<Build>(
            "Build",
            initial_deployment
                .spec
                .build_id
                .as_ref()
                .ok_or("build id")?
                .as_str(),
            |build| {
                build.status.phase = BuildPhase::Succeeded;
                build.status.source_revision = Some("initial-revision".to_string());
                build.status.image_digest = Some("example.test/api@sha256:initial".to_string());
            },
        )
        .await?;
    world
        .update::<Deployment>(
            "Deployment",
            initial_deployment.meta.id.as_str(),
            |deployment| {
                deployment.status.phase = DeploymentPhase::Ready;
                deployment.status.image_digest =
                    Some("example.test/api@sha256:initial".to_string());
            },
        )
        .await?;
    world
        .update::<Service>("Service", "api", |service| {
            service.status.active_deployment_id = Some(initial_deployment.meta.id.clone());
            service.meta.annotations.insert(
                kernel_api::AnnotationKey(kernel_api::BUILD_WATCH_REVISION_ANNOTATION.to_string()),
                "updated-revision".to_string(),
            );
        })
        .await?;

    let created = world.reconcile(Timestamp(3_000)).await?;
    assert_eq!(created.created_deployments, 1);
    let advanced = world.reconcile(Timestamp(4_000)).await?;

    assert!(!advanced.conflict);
    assert_eq!(advanced.created_builds, 1);
    assert_eq!(advanced.updated_deployments, 1);
    Ok(())
}

#[tokio::test]
async fn finalization_collects_deployment_build_and_replica_before_release()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(build_service()).await?;
    world.reconcile(Timestamp(1_000)).await?;
    world.reconcile(Timestamp(2_000)).await?;
    let deployment = world.one::<Deployment>("Deployment").await?;
    let build = world.one::<Build>("Build").await?;
    let replica = ready_replica(&deployment, &AssignmentId::new("historical-assignment")?);
    world
        .put("ReplicaState", &replica.meta.id, &replica)
        .await?;
    let orphan_deployment_id = DeploymentId::new("orphan-deployment")?;
    let mut orphan_build = build.clone();
    orphan_build.meta.id = BuildId::new("orphan-build")?;
    orphan_build.spec.deployment_id = orphan_deployment_id.clone();
    world
        .put("Build", &orphan_build.meta.id, &orphan_build)
        .await?;
    let mut orphan_replica = replica.clone();
    orphan_replica.meta.id = ReplicaStateId::new("orphan-replica")?;
    orphan_replica.spec.deployment_id = orphan_deployment_id;
    world
        .put("ReplicaState", &orphan_replica.meta.id, &orphan_replica)
        .await?;
    world
        .update::<Deployment>("Deployment", deployment.meta.id.as_str(), |resource| {
            resource.status.phase = DeploymentPhase::Removed;
        })
        .await?;
    world
        .update::<Service>("Service", "api", |service| {
            service.meta.deletion_timestamp = Some(Timestamp(3_000));
        })
        .await?;

    let collected = world.reconcile(Timestamp(3_000)).await?;
    assert_eq!(
        (
            collected.deleted_deployments,
            collected.deleted_builds,
            collected.deleted_replicas,
        ),
        (1, 2, 2)
    );
    assert!(world.list::<Deployment>("Deployment").await?.is_empty());
    assert!(world.list::<Build>("Build").await?.is_empty());
    assert!(world.list::<ReplicaState>("ReplicaState").await?.is_empty());
    assert_eq!(build.spec.deployment_id, deployment.meta.id);
    Ok(())
}

#[tokio::test]
async fn runtime_releases_finalizer_only_after_child_collection()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new(image_service()).await?;
    let reconciler = Arc::new(DeploymentReconciler::new(
        world.cluster_id.clone(),
        settings(),
        Arc::new(FixedTimestampClock(Timestamp(5_000))),
    )?);
    let runtime = reconciler.runtime(
        Arc::new(world.fenced.clone()),
        Arc::new(NoopClock),
        RuntimeConfig::new(
            Duration::from_secs(30),
            Backoff::new(Duration::from_millis(10), Duration::from_secs(1))?,
        )?,
    );
    assert_eq!(runtime.reconcile_snapshot().await?, 0);
    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    let service = world.one::<Service>("Service").await?;
    assert!(service.meta.finalizers.contains(&kernel_api::FinalizerName(
        "deployment.maestro.dev/children".to_string()
    )));
    let deployment = world.one::<Deployment>("Deployment").await?;
    world
        .update::<Deployment>("Deployment", deployment.meta.id.as_str(), |resource| {
            resource.status.phase = DeploymentPhase::Removed;
        })
        .await?;
    world
        .update::<Service>("Service", "api", |resource| {
            resource.meta.deletion_timestamp = Some(Timestamp(5_000));
        })
        .await?;

    assert_eq!(runtime.reconcile_snapshot().await?, 1);
    assert!(world.list::<Service>("Service").await?.is_empty());
    assert!(world.list::<Deployment>("Deployment").await?.is_empty());
    Ok(())
}

struct World {
    cluster_id: ClusterId,
    keys: Keyspace,
    store: Arc<InMemoryStore>,
    fenced: FencedStore,
    controller: DeploymentController,
    _session: Box<dyn Session>,
}

impl World {
    async fn new(service: Service) -> Result<Self, Box<dyn std::error::Error>> {
        let cluster_id = ClusterId::new("cluster-1")?;
        let keys = Keyspace::new(&cluster_id);
        let store = Arc::new(InMemoryStore::new(Arc::new(NoopClock)));
        let session = store.session(Duration::from_secs(30)).await?;
        let leader = store
            .put_cas(PutRequest {
                key: keys.leader(),
                value: b"deployment-controller".to_vec(),
                expected: ExpectedVersion::Missing,
                session: Some(SessionBinding {
                    session_id: session.id(),
                }),
            })
            .await?;
        let CasOutcome::Applied(leader) = leader else {
            return Err("leader campaign conflicted".into());
        };
        let token = LeadershipToken::from_campaign(
            LeaderIdentity {
                node_id: NodeId::new("node-1")?,
                instance_id: NodeInstanceId::new("deployment-controller")?,
            },
            session.id(),
            leader.version,
        );
        let fenced = FencedStore::new(store.clone(), keys.leader(), token);
        let controller = DeploymentController::new(cluster_id.clone(), settings())?;
        let world = Self {
            cluster_id,
            keys,
            store,
            fenced,
            controller,
            _session: session,
        };
        world.put("Service", &service.meta.id, &service).await?;
        Ok(world)
    }

    async fn reconcile(
        &self,
        now: Timestamp,
    ) -> Result<crate::DeploymentReport, crate::DeploymentError> {
        self.controller
            .reconcile_service(
                &self.fenced,
                &ServiceId::new("api").expect("fixture service id"),
                now,
            )
            .await
    }

    async fn put<Id: Clone + Into<ResourceName>>(
        &self,
        kind: &str,
        id: &Id,
        resource: &impl serde::Serialize,
    ) -> Result<(), Box<dyn std::error::Error>> {
        let outcome = self
            .store
            .put_cas(PutRequest {
                key: self
                    .keys
                    .resource(&ResourceKind::new(kind)?, &id.clone().into()),
                value: serde_json::to_vec(resource)?,
                expected: ExpectedVersion::Missing,
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err(format!("{kind} create conflicted").into())
        }
    }

    async fn update<Resource: serde::de::DeserializeOwned + serde::Serialize>(
        &self,
        kind: &str,
        id: &str,
        change: impl FnOnce(&mut Resource),
    ) -> Result<(), Box<dyn std::error::Error>> {
        let key = self.keys.resource(
            &ResourceKind::new(kind)?,
            &ResourceName::new(id.to_string())?,
        );
        let stored = self.store.get(&key).await?.ok_or("resource missing")?;
        let mut resource = serde_json::from_slice::<Resource>(&stored.value)?;
        change(&mut resource);
        let outcome = self
            .store
            .put_cas(PutRequest {
                key,
                value: serde_json::to_vec(&resource)?,
                expected: ExpectedVersion::Exact(stored.version),
                session: None,
            })
            .await?;
        if matches!(outcome, CasOutcome::Applied(_)) {
            Ok(())
        } else {
            Err(format!("{kind} update conflicted").into())
        }
    }

    async fn list<Resource: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
    ) -> Result<Vec<Resource>, Box<dyn std::error::Error>> {
        self.store
            .list(&self.keys.resource_kind(&ResourceKind::new(kind)?))
            .await?
            .values
            .into_iter()
            .map(|stored| serde_json::from_slice(&stored.value).map_err(Into::into))
            .collect()
    }

    async fn one<Resource: serde::de::DeserializeOwned>(
        &self,
        kind: &str,
    ) -> Result<Resource, Box<dyn std::error::Error>> {
        let mut resources = self.list(kind).await?;
        if resources.len() != 1 {
            return Err(format!("expected one {kind}, found {}", resources.len()).into());
        }
        Ok(resources.remove(0))
    }
}

fn settings() -> LifecycleSettings {
    LifecycleSettings {
        drain_grace: Duration::from_secs(30),
    }
}

fn image_service() -> Service {
    service(ArtifactTemplate::Image {
        reference: "registry.test/api:latest".to_string(),
    })
}

fn build_service() -> Service {
    service(ArtifactTemplate::Build {
        template: BuildTemplate {
            source: BuildSource::Git {
                repository: "https://example.test/repo.git".to_string(),
                revision: "main".to_string(),
            },
            dockerfile: "Dockerfile".to_string(),
            watch: false,
            registry: None,
            depot: None,
            environment: BTreeMap::new(),
            secrets: BTreeMap::new(),
        },
    })
}

fn service(artifact: ArtifactTemplate) -> Service {
    Object {
        meta: metadata(ServiceId::new("api").expect("service id")),
        spec: ServiceSpec {
            name: "API".to_string(),
            version: "1.0.0".to_string(),
            artifact,
            preview: None,
            command: None,
            replicas: 1,
            exposed_ports: vec![8080],
            health_check: None,
            max_restarts: Some(3),
            environment: BTreeMap::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: None,
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    }
}

fn ready_replica(deployment: &Deployment, assignment_id: &AssignmentId) -> ReplicaState {
    Object {
        meta: metadata(
            ReplicaStateId::new(format!("replica-{assignment_id}")).expect("replica state id"),
        ),
        spec: ReplicaStateSpec {
            service_id: deployment.spec.service_id.clone(),
            deployment_id: deployment.meta.id.clone(),
            assignment_id: assignment_id.clone(),
            replica_index: 0,
        },
        status: ReplicaStateStatus {
            phase: DeploymentPhase::Ready,
            node_id: Some(NodeId::new("node-1").expect("node id")),
            workload_id: None,
            healthcheck_failures: 0,
            restart_attempts: 0,
            restart_pending_attempt: None,
            restart_not_before: None,
            conditions: Vec::new(),
        },
    }
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}

struct FixedTimestampClock(Timestamp);

impl TimestampClock for FixedTimestampClock {
    fn now(&self) -> Timestamp {
        self.0
    }
}

struct NoopClock;

#[async_trait]
impl Clock for NoopClock {
    fn now(&self) -> MonotonicTime {
        MonotonicTime::default()
    }

    async fn sleep_until(&self, _deadline: MonotonicTime) {
        std::future::pending::<()>().await;
    }
}
