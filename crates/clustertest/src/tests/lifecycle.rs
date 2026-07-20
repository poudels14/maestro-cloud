use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;

use crate::{
    AcceptanceCluster, ArtifactBehavior, ClusterSnapshot, DeploymentPhase, DeploymentSnapshot,
    FaultInjectableCluster, FixtureArtifact, FixtureName, LifecycleFaultCluster, ReplicaCount,
    ReplicaIndex, ReplicaOverride, ReplicaRecordDisposition, ReplicaSnapshot, ResourceAvailability,
    RolloutFailure, ServiceFixture, ServiceSnapshot, scenarios,
};

struct WorldService {
    fixture: ServiceFixture,
    replica_override: Option<ReplicaCount>,
    deployments: Vec<DeploymentSnapshot<u64>>,
}

pub(super) struct LifecycleWorld {
    next_deployment: u64,
    services: BTreeMap<FixtureName, WorldService>,
    deployment_services: BTreeMap<u64, FixtureName>,
    failures: BTreeMap<u64, RolloutFailure>,
    exhausted: BTreeSet<(u64, u32)>,
    missing_records: BTreeSet<u64>,
    pub(super) artifact_behaviors: BTreeMap<u64, ArtifactBehavior>,
    pub(super) prepared: BTreeSet<u64>,
    pub(super) built: BTreeSet<u64>,
    pub(super) health_writes: u64,
    drain_grace_elapsed: bool,
}

impl LifecycleWorld {
    pub(super) fn new() -> Self {
        Self {
            next_deployment: 1,
            services: BTreeMap::new(),
            deployment_services: BTreeMap::new(),
            failures: BTreeMap::new(),
            exhausted: BTreeSet::new(),
            missing_records: BTreeSet::new(),
            artifact_behaviors: BTreeMap::new(),
            prepared: BTreeSet::new(),
            built: BTreeSet::new(),
            health_writes: 0,
            drain_grace_elapsed: false,
        }
    }

    fn snapshot(&self) -> ClusterSnapshot<u64> {
        ClusterSnapshot {
            services: self
                .services
                .iter()
                .map(|(name, service)| ServiceSnapshot {
                    name: name.clone(),
                    configured_replicas: service.fixture.replicas,
                    replica_override: service.replica_override,
                    deployments: service
                        .deployments
                        .iter()
                        .cloned()
                        .map(|mut deployment| {
                            deployment.phase_history = vec![deployment.phase];
                            deployment
                        })
                        .collect(),
                })
                .collect(),
        }
    }

    fn service_mut(&mut self, deployment: u64) -> Result<&mut WorldService, LifecycleWorldError> {
        let name = self
            .deployment_services
            .get(&deployment)
            .ok_or(LifecycleWorldError("unknown deployment"))?;
        self.services
            .get_mut(name)
            .ok_or(LifecycleWorldError("unknown service"))
    }

    pub(super) fn deployment_mut(
        &mut self,
        deployment: u64,
    ) -> Result<&mut DeploymentSnapshot<u64>, LifecycleWorldError> {
        self.service_mut(deployment)?
            .deployments
            .iter_mut()
            .find(|candidate| candidate.id == deployment)
            .ok_or(LifecycleWorldError("unknown deployment"))
    }

    fn settle(&mut self, readiness: WorldReadiness) {
        let failures = &self.failures;
        let exhausted = &self.exhausted;
        let missing_records = &self.missing_records;
        let artifact_behaviors = &self.artifact_behaviors;
        let prepared = &mut self.prepared;
        let built = &mut self.built;
        for service in self.services.values_mut() {
            for deployment in &mut service.deployments {
                if deployment.phase == DeploymentPhase::Canceled {
                    continue;
                }
                prepared.insert(deployment.id);
                if failures.contains_key(&deployment.id) {
                    deployment.phase = DeploymentPhase::Crashed;
                    deployment.replicas.clear();
                } else if missing_records.contains(&deployment.id) {
                    deployment.phase = DeploymentPhase::Terminated;
                    deployment.replicas.clear();
                } else {
                    match artifact_behaviors.get(&deployment.id) {
                        Some(ArtifactBehavior::NeverCompletes) => {
                            deployment.phase = if self.drain_grace_elapsed {
                                DeploymentPhase::Crashed
                            } else {
                                DeploymentPhase::Building
                            };
                        }
                        Some(ArtifactBehavior::CompleteWith(artifact)) => {
                            built.insert(deployment.id);
                            deployment.artifact = Some(artifact.clone());
                        }
                        None => {
                            built.insert(deployment.id);
                            deployment.artifact =
                                Some(FixtureArtifact::new(format!("artifact-{}", deployment.id)));
                        }
                    }
                }
            }
            let Some(active_index) = service.deployments.iter().rposition(|deployment| {
                !matches!(
                    deployment.phase,
                    DeploymentPhase::Canceled
                        | DeploymentPhase::Crashed
                        | DeploymentPhase::Terminated
                        | DeploymentPhase::Removed
                ) && !matches!(
                    artifact_behaviors.get(&deployment.id),
                    Some(ArtifactBehavior::NeverCompletes)
                )
            }) else {
                continue;
            };
            let desired = service
                .replica_override
                .map(|replicas| {
                    ReplicaCount::new(replicas.get().max(service.fixture.replicas.get()))
                })
                .unwrap_or(service.fixture.replicas)
                .get();
            let Some(active) = service.deployments.get_mut(active_index) else {
                continue;
            };
            active.replicas.retain(|replica| replica.index < desired);
            for index in 0..desired {
                if !active.replicas.iter().any(|replica| replica.index == index) {
                    active.replicas.push(ReplicaSnapshot {
                        index,
                        phase: DeploymentPhase::PendingReady,
                        restart_attempts: 0,
                        healthcheck_failures: 0,
                        workload: ResourceAvailability::Available,
                    });
                }
            }
            active.replicas.sort_by_key(|replica| replica.index);
            let settled_phase = match readiness {
                WorldReadiness::Automatic => DeploymentPhase::Ready,
                WorldReadiness::External
                    if active.phase == DeploymentPhase::Ready
                        || active
                            .replicas
                            .iter()
                            .all(|replica| replica.phase == DeploymentPhase::Ready) =>
                {
                    DeploymentPhase::Ready
                }
                WorldReadiness::External => DeploymentPhase::PendingReady,
            };
            for replica in &mut active.replicas {
                if exhausted.contains(&(active.id, replica.index)) {
                    replica.phase = DeploymentPhase::Crashed;
                    replica.restart_attempts = u32::MAX;
                    replica.workload = ResourceAvailability::Unavailable;
                } else {
                    if replica.phase == DeploymentPhase::Crashed {
                        replica.restart_attempts = replica.restart_attempts.saturating_add(1);
                    }
                    replica.phase = settled_phase;
                    replica.workload = ResourceAvailability::Available;
                }
            }
            if active
                .replicas
                .iter()
                .all(|replica| exhausted.contains(&(active.id, replica.index)))
            {
                active.phase = DeploymentPhase::Crashed;
            } else {
                active.phase = settled_phase;
            }
            if active.phase == DeploymentPhase::Ready {
                for (index, previous) in service.deployments.iter_mut().enumerate() {
                    if index != active_index
                        && !matches!(
                            previous.phase,
                            DeploymentPhase::Canceled
                                | DeploymentPhase::Crashed
                                | DeploymentPhase::Terminated
                                | DeploymentPhase::Removed
                        )
                    {
                        previous.phase = if self.drain_grace_elapsed {
                            for replica in &mut previous.replicas {
                                replica.workload = ResourceAvailability::Unavailable;
                            }
                            DeploymentPhase::Removed
                        } else {
                            DeploymentPhase::Draining
                        };
                    }
                }
            }
        }
    }
}

#[derive(Clone, Copy)]
enum WorldReadiness {
    Automatic,
    External,
}

#[derive(Debug, thiserror::Error)]
#[error("lifecycle world failed: {0}")]
pub(super) struct LifecycleWorldError(pub(super) &'static str);

#[async_trait]
impl AcceptanceCluster for LifecycleWorld {
    type DeploymentId = u64;
    type Error = LifecycleWorldError;

    async fn rollout(
        &mut self,
        service: ServiceFixture,
    ) -> Result<Self::DeploymentId, Self::Error> {
        let deployment = self.next_deployment;
        self.next_deployment += 1;
        self.deployment_services
            .insert(deployment, service.name.clone());
        let entry = self
            .services
            .entry(service.name.clone())
            .or_insert_with(|| WorldService {
                fixture: service.clone(),
                replica_override: None,
                deployments: Vec::new(),
            });
        entry.fixture = service.clone();
        entry.deployments.push(DeploymentSnapshot {
            id: deployment,
            version: service.version,
            phase: DeploymentPhase::Queued,
            phase_history: vec![DeploymentPhase::Queued],
            artifact: None,
            replicas: Vec::new(),
        });
        Ok(deployment)
    }

    async fn cancel(&mut self, deployment_id: &Self::DeploymentId) -> Result<(), Self::Error> {
        self.deployment_mut(*deployment_id)?.phase = DeploymentPhase::Canceled;
        Ok(())
    }

    async fn set_replicas(
        &mut self,
        service: &FixtureName,
        replica_override: ReplicaOverride,
    ) -> Result<(), Self::Error> {
        let service = self
            .services
            .get_mut(service)
            .ok_or(LifecycleWorldError("unknown service"))?;
        service.replica_override = match replica_override {
            ReplicaOverride::Set(replicas) => Some(replicas),
            ReplicaOverride::Clear => None,
        };
        Ok(())
    }

    async fn advance(&mut self, _duration: Duration) -> Result<(), Self::Error> {
        self.drain_grace_elapsed = true;
        Ok(())
    }

    async fn reconcile_once(&mut self) -> Result<(), Self::Error> {
        self.settle(WorldReadiness::External);
        Ok(())
    }

    async fn snapshot_now(&mut self) -> Result<ClusterSnapshot<u64>, Self::Error> {
        Ok(self.snapshot())
    }

    async fn await_converged(&mut self) -> Result<ClusterSnapshot<u64>, Self::Error> {
        self.settle(WorldReadiness::Automatic);
        Ok(self.snapshot())
    }
}

#[async_trait]
impl FaultInjectableCluster for LifecycleWorld {
    async fn inject_rollout_failure(
        &mut self,
        deployment_id: &Self::DeploymentId,
        failure: RolloutFailure,
    ) -> Result<(), Self::Error> {
        self.failures.insert(*deployment_id, failure);
        Ok(())
    }

    async fn inject_replica_crash(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<(), Self::Error> {
        let replica = self
            .deployment_mut(*deployment_id)?
            .replicas
            .iter_mut()
            .find(|replica| replica.index == replica_index.get())
            .ok_or(LifecycleWorldError("unknown replica"))?;
        replica.phase = DeploymentPhase::Crashed;
        replica.workload = ResourceAvailability::Unavailable;
        Ok(())
    }
}

#[async_trait]
impl LifecycleFaultCluster for LifecycleWorld {
    async fn settle_operation_sequence(
        &mut self,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error> {
        self.settle(WorldReadiness::Automatic);
        Ok(self.snapshot())
    }

    async fn await_started(
        &mut self,
        _deployment_id: &Self::DeploymentId,
        _replicas: ReplicaCount,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error> {
        self.settle(WorldReadiness::External);
        Ok(self.snapshot())
    }

    async fn await_stable_without_readiness(
        &mut self,
    ) -> Result<ClusterSnapshot<Self::DeploymentId>, Self::Error> {
        self.settle(WorldReadiness::External);
        Ok(self.snapshot())
    }

    async fn inject_exhausted_replica(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
    ) -> Result<(), Self::Error> {
        self.exhausted.insert((*deployment_id, replica_index.get()));
        Ok(())
    }

    async fn inject_workload_termination(
        &mut self,
        deployment_id: &Self::DeploymentId,
        replica_index: ReplicaIndex,
        record: ReplicaRecordDisposition,
    ) -> Result<(), Self::Error> {
        if record == ReplicaRecordDisposition::Missing {
            self.missing_records.insert(*deployment_id);
        } else {
            let replica = self
                .deployment_mut(*deployment_id)?
                .replicas
                .iter_mut()
                .find(|replica| replica.index == replica_index.get())
                .ok_or(LifecycleWorldError("unknown replica"))?;
            replica.workload = ResourceAvailability::Unavailable;
        }
        Ok(())
    }
}

#[tokio::test]
async fn basic_lifecycle_scenarios_pass_fast_fake() {
    scenarios::rollout_reaches_ready(&mut LifecycleWorld::new())
        .await
        .expect("rollout scenario");
    scenarios::redeploy_drains_previous(&mut LifecycleWorld::new())
        .await
        .expect("redeploy scenario");
    scenarios::queued_deployment_can_be_canceled(&mut LifecycleWorld::new())
        .await
        .expect("cancel scenario");
    scenarios::replica_override_round_trips(&mut LifecycleWorld::new())
        .await
        .expect("replica override scenario");
    scenarios::drained_deployment_finalizes(&mut LifecycleWorld::new())
        .await
        .expect("drain finalization scenario");
    scenarios::build_failure_marks_deployment_crashed(&mut LifecycleWorld::new())
        .await
        .expect("build failure scenario");
    scenarios::prepare_failure_marks_deployment_crashed(&mut LifecycleWorld::new())
        .await
        .expect("prepare failure scenario");
    scenarios::crashed_replica_restarts_in_place(&mut LifecycleWorld::new())
        .await
        .expect("replica restart scenario");
}

#[tokio::test]
async fn lifecycle_fault_scenarios_pass_fast_fake() {
    scenarios::exhausted_replica_stays_down_while_peers_run(&mut LifecycleWorld::new())
        .await
        .expect("exhausted replica scenario");
    scenarios::all_exhausted_replicas_crash_deployment(&mut LifecycleWorld::new())
        .await
        .expect("all exhausted replicas scenario");
    scenarios::initial_replica_crash_preserves_pending_peers(&mut LifecycleWorld::new())
        .await
        .expect("initial replica crash scenario");
    scenarios::missing_workload_record_is_recovered(&mut LifecycleWorld::new())
        .await
        .expect("missing workload record scenario");
    scenarios::rollout_failure_is_isolated_between_services(&mut LifecycleWorld::new())
        .await
        .expect("isolated failure scenario");
    scenarios::old_workload_crash_does_not_break_redeployment(&mut LifecycleWorld::new())
        .await
        .expect("old workload crash scenario");
}
