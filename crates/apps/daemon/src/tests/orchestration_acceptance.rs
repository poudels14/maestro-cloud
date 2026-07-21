use std::collections::BTreeMap;
use std::fmt::{Display, Formatter};
use std::time::Duration;

use async_trait::async_trait;
use clustertest::{
    AcceptanceCluster, ClusterSnapshot, DeploymentPhase as SnapshotDeploymentPhase,
    DeploymentSnapshot, FixtureArtifact, FixtureName, FixtureVersion, IngressFixture, ReplicaCount,
    ReplicaOverride, ReplicaSnapshot, ResourceAvailability, ServiceFixture, ServiceSnapshot,
    scenarios,
};
use kernel_api::{
    Assignment, AssignmentPhase, Deployment, DeploymentGoal,
    DeploymentPhase as ResourceDeploymentPhase, IngressRouteId, ReplicaState, ResourceKind,
    ResourceName, Service, ServiceId,
};
use kernel_store::Store;

use super::orchestration::{RolloutWorld, put};
use super::orchestration_fixture::{route, service};

const INITIAL_TIME_MILLIS: i64 = 10_000;

struct AcceptanceWorld {
    inner: RolloutWorld,
    now_millis: i64,
}

impl AcceptanceWorld {
    async fn new(node_count: u8) -> Result<Self, AcceptanceError> {
        Ok(Self {
            inner: RolloutWorld::new_empty(node_count)
                .await
                .map_err(AcceptanceError::from_driver)?,
            now_millis: INITIAL_TIME_MILLIS,
        })
    }

    async fn apply_service(
        &self,
        fixture: &ServiceFixture,
    ) -> Result<kernel_api::Generation, AcceptanceError> {
        let service_id = service_id(&fixture.name)?;
        let key = self.inner.keys.resource(
            &ResourceKind::new("Service").map_err(AcceptanceError::from_driver)?,
            &ResourceName::from(service_id.clone()),
        );
        if self
            .inner
            .store
            .get(&key)
            .await
            .map_err(AcceptanceError::from_driver)?
            .is_some()
        {
            let updated = self
                .inner
                .update_service_by_id(&service_id, |resource| {
                    resource.meta.generation =
                        kernel_api::Generation(resource.meta.generation.0.saturating_add(1));
                    resource.spec.name = fixture.name.as_str().to_string();
                    resource.spec.version = fixture.version.as_str().to_string();
                    resource.spec.replicas = fixture.replicas.get();
                })
                .await
                .map_err(AcceptanceError::from_driver)?;
            Ok(updated.meta.generation)
        } else {
            let mut resource =
                service(fixture.replicas.get()).map_err(AcceptanceError::from_driver)?;
            resource.meta.id = service_id;
            resource.spec.name = fixture.name.as_str().to_string();
            resource.spec.version = fixture.version.as_str().to_string();
            let generation = resource.meta.generation;
            put(&self.inner.store, &self.inner.keys, "Service", &resource)
                .await
                .map_err(AcceptanceError::from_driver)?;
            self.apply_ingress(fixture).await?;
            Ok(generation)
        }
    }

    async fn apply_ingress(&self, fixture: &ServiceFixture) -> Result<(), AcceptanceError> {
        let IngressFixture::Host(host) = &fixture.ingress else {
            return Ok(());
        };
        let service_id = service_id(&fixture.name)?;
        let mut resource = route().map_err(AcceptanceError::from_driver)?;
        resource.meta.id = IngressRouteId::new(format!("{}-route", service_id.as_str()))
            .map_err(AcceptanceError::from_driver)?;
        resource.spec.service_id = service_id;
        resource.spec.hosts = vec![host.clone()];
        put(
            &self.inner.store,
            &self.inner.keys,
            "IngressRoute",
            &resource,
        )
        .await
        .map_err(AcceptanceError::from_driver)
    }

    async fn snapshot(&self) -> Result<ClusterSnapshot<kernel_api::DeploymentId>, AcceptanceError> {
        let mut services = self
            .inner
            .list::<Service>("Service")
            .await
            .map_err(AcceptanceError::from_driver)?;
        let deployments = self
            .inner
            .list::<Deployment>("Deployment")
            .await
            .map_err(AcceptanceError::from_driver)?;
        let assignments = self
            .inner
            .list::<Assignment>("Assignment")
            .await
            .map_err(AcceptanceError::from_driver)?;
        let replicas = self
            .inner
            .list::<ReplicaState>("ReplicaState")
            .await
            .map_err(AcceptanceError::from_driver)?;

        services.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));
        let assignments = assignments
            .into_iter()
            .map(|assignment| (assignment.meta.id.clone(), assignment))
            .collect::<BTreeMap<_, _>>();
        let services = services
            .into_iter()
            .map(|service| project_service(&service, &deployments, &assignments, &replicas))
            .collect::<Vec<_>>();
        Ok(ClusterSnapshot { services })
    }
}

#[async_trait]
impl AcceptanceCluster for AcceptanceWorld {
    type DeploymentId = kernel_api::DeploymentId;
    type Error = AcceptanceError;

    async fn rollout(
        &mut self,
        fixture: ServiceFixture,
    ) -> Result<Self::DeploymentId, Self::Error> {
        let service_id = service_id(&fixture.name)?;
        let generation = self.apply_service(&fixture).await?;
        for _pass in 0..4 {
            self.inner
                .reconcile_operators()
                .await
                .map_err(AcceptanceError::from_driver)?;
            let queued = self
                .inner
                .list::<Deployment>("Deployment")
                .await
                .map_err(AcceptanceError::from_driver)?
                .into_iter()
                .find(|deployment| {
                    deployment.spec.service_id == service_id
                        && deployment.spec.service_generation == generation
                });
            if let Some(deployment) = queued {
                return Ok(deployment.meta.id);
            }
        }
        Err(AcceptanceError::new(
            "deployment operator did not queue generation",
        ))
    }

    async fn cancel(&mut self, deployment_id: &Self::DeploymentId) -> Result<(), Self::Error> {
        self.inner
            .request_deployment_goal(deployment_id, DeploymentGoal::Cancel)
            .await
            .map_err(AcceptanceError::from_driver)
    }

    async fn set_replicas(
        &mut self,
        name: &FixtureName,
        replica_override: ReplicaOverride,
    ) -> Result<(), Self::Error> {
        let service_id = service_id(name)?;
        let replicas = match replica_override {
            ReplicaOverride::Set(replicas) => Some(replicas.get()),
            ReplicaOverride::Clear => None,
        };
        self.inner
            .update_service_by_id(&service_id, |service| {
                service.status.replica_override = replicas;
            })
            .await
            .map(|_service| ())
            .map_err(AcceptanceError::from_driver)
    }

    async fn advance(&mut self, duration: Duration) -> Result<(), Self::Error> {
        let millis = i64::try_from(duration.as_millis())
            .map_err(|_error| AcceptanceError::new("logical time advance overflowed"))?;
        self.now_millis = self.now_millis.saturating_add(millis);
        self.inner.set_time(self.now_millis);
        Ok(())
    }

    async fn reconcile_once(&mut self) -> Result<(), Self::Error> {
        self.inner
            .reconcile_operators()
            .await
            .map_err(AcceptanceError::from_driver)
    }

    async fn snapshot_now(&mut self) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error> {
        self.snapshot().await
    }

    async fn await_converged(
        &mut self,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error> {
        self.inner
            .converge()
            .await
            .map_err(AcceptanceError::from_driver)?;
        self.snapshot().await
    }
}

fn project_service(
    service: &Service,
    deployments: &[Deployment],
    assignments: &BTreeMap<kernel_api::AssignmentId, Assignment>,
    replicas: &[ReplicaState],
) -> ServiceSnapshot<kernel_api::DeploymentId> {
    let mut related = deployments
        .iter()
        .filter(|deployment| deployment.spec.service_id == service.meta.id)
        .collect::<Vec<_>>();
    related.sort_by(|left, right| {
        left.spec
            .service_generation
            .cmp(&right.spec.service_generation)
            .then_with(|| left.meta.id.cmp(&right.meta.id))
    });
    let deployments = related
        .into_iter()
        .map(|deployment| project_deployment(deployment, assignments, replicas))
        .collect::<Vec<_>>();
    ServiceSnapshot {
        name: FixtureName::new(service.spec.name.clone()),
        configured_replicas: ReplicaCount::new(service.spec.replicas),
        replica_override: service.status.replica_override.map(ReplicaCount::new),
        deployments,
    }
}

fn project_deployment(
    deployment: &Deployment,
    assignments: &BTreeMap<kernel_api::AssignmentId, Assignment>,
    replicas: &[ReplicaState],
) -> DeploymentSnapshot<kernel_api::DeploymentId> {
    let phase = project_phase(deployment.status.phase);
    let mut replicas = replicas
        .iter()
        .filter(|replica| {
            replica.spec.deployment_id == deployment.meta.id
                && assignments.contains_key(&replica.spec.assignment_id)
        })
        .map(|replica| {
            let workload = assignments
                .get(&replica.spec.assignment_id)
                .filter(|assignment| assignment.status.phase == AssignmentPhase::Running)
                .map_or(ResourceAvailability::Unavailable, |_assignment| {
                    ResourceAvailability::Available
                });
            ReplicaSnapshot {
                index: replica.spec.replica_index,
                phase: project_phase(replica.status.phase),
                restart_attempts: replica.status.restart_attempts,
                healthcheck_failures: replica.status.healthcheck_failures,
                workload,
            }
        })
        .collect::<Vec<_>>();
    replicas.sort_by_key(|replica| replica.index);
    DeploymentSnapshot {
        id: deployment.meta.id.clone(),
        version: FixtureVersion::new(deployment.spec.service.version.clone()),
        phase,
        phase_history: vec![phase],
        artifact: deployment
            .status
            .image_digest
            .clone()
            .map(FixtureArtifact::new),
        replicas,
    }
}

fn project_phase(phase: ResourceDeploymentPhase) -> SnapshotDeploymentPhase {
    match phase {
        ResourceDeploymentPhase::Queued => SnapshotDeploymentPhase::Queued,
        ResourceDeploymentPhase::Building => SnapshotDeploymentPhase::Building,
        ResourceDeploymentPhase::PendingReady => SnapshotDeploymentPhase::PendingReady,
        ResourceDeploymentPhase::Ready => SnapshotDeploymentPhase::Ready,
        ResourceDeploymentPhase::Crashed => SnapshotDeploymentPhase::Crashed,
        ResourceDeploymentPhase::Terminated => SnapshotDeploymentPhase::Terminated,
        ResourceDeploymentPhase::Removed => SnapshotDeploymentPhase::Removed,
        ResourceDeploymentPhase::Draining => SnapshotDeploymentPhase::Draining,
        ResourceDeploymentPhase::Canceled => SnapshotDeploymentPhase::Canceled,
    }
}

fn service_id(name: &FixtureName) -> Result<ServiceId, AcceptanceError> {
    ServiceId::new(name.as_str()).map_err(AcceptanceError::from_driver)
}

#[derive(Debug)]
struct AcceptanceError(String);

impl AcceptanceError {
    fn new(message: impl Into<String>) -> Self {
        Self(message.into())
    }

    fn from_driver(error: impl Display) -> Self {
        Self(error.to_string())
    }
}

impl Display for AcceptanceError {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(&self.0)
    }
}

impl std::error::Error for AcceptanceError {}

#[tokio::test]
async fn shared_lifecycle_scenarios_drive_composed_operators()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        scenarios::rollout_reaches_ready(&mut AcceptanceWorld::new(node_count).await?).await?;
        scenarios::redeploy_drains_previous(&mut AcceptanceWorld::new(node_count).await?).await?;
        scenarios::queued_deployment_can_be_canceled(&mut AcceptanceWorld::new(node_count).await?)
            .await?;
        scenarios::replica_override_round_trips(&mut AcceptanceWorld::new(node_count).await?)
            .await?;
        scenarios::drained_deployment_finalizes(&mut AcceptanceWorld::new(node_count).await?)
            .await?;
    }
    Ok(())
}
