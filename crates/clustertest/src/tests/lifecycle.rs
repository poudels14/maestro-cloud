use std::collections::{BTreeMap, BTreeSet};
use std::time::Duration;

use async_trait::async_trait;

use super::lifecycle_topology::assign_replica_placements;

use crate::{
    AcceptanceCluster, ArtifactBehavior, ClusterSnapshot, DeploymentPhase, DeploymentSnapshot,
    FaultInjectableCluster, FixtureArtifact, FixtureName, FixtureNodeName, LifecycleFaultCluster,
    ReplicaCount, ReplicaIndex, ReplicaOverride, ReplicaRecordDisposition, ReplicaSnapshot,
    ResourceAvailability, RolloutFailure, ServiceFixture, ServiceSnapshot,
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
    pub(super) topology_nodes: Vec<FixtureNodeName>,
    pub(super) draining_nodes: BTreeSet<FixtureNodeName>,
    pub(super) service_affinity: BTreeMap<FixtureName, FixtureNodeName>,
    pub(super) frozen_services: BTreeSet<FixtureName>,
    pub(super) removing_deployments: BTreeSet<u64>,
    pub(super) deleting_services: BTreeSet<FixtureName>,
    next_workload_instance: u64,
    drain_grace_elapsed: bool,
}

impl LifecycleWorld {
    pub(super) fn new() -> Self {
        Self::with_node_count(1)
    }

    pub(super) fn with_node_count(node_count: usize) -> Self {
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
            topology_nodes: (1..=node_count.max(1))
                .map(|index| FixtureNodeName::new(format!("node-{index}")))
                .collect(),
            draining_nodes: BTreeSet::new(),
            service_affinity: BTreeMap::new(),
            frozen_services: BTreeSet::new(),
            removing_deployments: BTreeSet::new(),
            deleting_services: BTreeSet::new(),
            next_workload_instance: 1,
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
                    active_deployment_id: service
                        .deployments
                        .iter()
                        .rev()
                        .find(|deployment| deployment.phase == DeploymentPhase::Ready)
                        .map(|deployment| deployment.id),
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
        let drain_grace_elapsed = self.drain_grace_elapsed;
        if drain_grace_elapsed {
            let deleting_services = &self.deleting_services;
            self.services
                .retain(|name, _service| !deleting_services.contains(name));
            self.deployment_services
                .retain(|_deployment, name| !deleting_services.contains(name));
        }
        let failures = &self.failures;
        let exhausted = &self.exhausted;
        let missing_records = &self.missing_records;
        let artifact_behaviors = &self.artifact_behaviors;
        let frozen_services = &self.frozen_services;
        let removing_deployments = &self.removing_deployments;
        let deleting_services = &self.deleting_services;
        let topology_nodes = &self.topology_nodes;
        let draining_nodes = &self.draining_nodes;
        let service_affinity = &self.service_affinity;
        let next_workload_instance = &mut self.next_workload_instance;
        let prepared = &mut self.prepared;
        let built = &mut self.built;
        for (service_name, service) in &mut self.services {
            if deleting_services.contains(service_name) {
                for deployment in &mut service.deployments {
                    deployment.phase = DeploymentPhase::Draining;
                }
                continue;
            }
            for deployment in &mut service.deployments {
                if deployment.phase == DeploymentPhase::Canceled {
                    continue;
                }
                if removing_deployments.contains(&deployment.id) {
                    deployment.phase = if drain_grace_elapsed {
                        deployment.replicas.clear();
                        DeploymentPhase::Removed
                    } else {
                        DeploymentPhase::Draining
                    };
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
                            deployment.phase = if drain_grace_elapsed {
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
            let frozen = frozen_services.contains(service_name);
            let Some(active_index) = service.deployments.iter().rposition(|deployment| {
                !matches!(
                    deployment.phase,
                    DeploymentPhase::Canceled
                        | DeploymentPhase::Crashed
                        | DeploymentPhase::Terminated
                        | DeploymentPhase::Removed
                ) && !removing_deployments.contains(&deployment.id)
                    && (!frozen || deployment.phase != DeploymentPhase::Queued)
                    && !matches!(
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
                        healthcheck_failures: 0,
                        workload: ResourceAvailability::Available,
                        node: None,
                        workload_instance: None,
                    });
                }
            }
            active.replicas.sort_by_key(|replica| replica.index);
            assign_replica_placements(
                active,
                topology_nodes,
                draining_nodes,
                service_affinity.get(service_name),
                next_workload_instance,
            );
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
                    replica.workload = ResourceAvailability::Unavailable;
                } else {
                    if replica.phase == DeploymentPhase::Crashed {
                        replica.healthcheck_failures = 0;
                    }
                    replica.phase = settled_phase;
                    replica.workload = ResourceAvailability::Available;
                }
            }
            if active
                .replicas
                .iter()
                .any(|replica| replica.phase == DeploymentPhase::Crashed)
            {
                active.phase = DeploymentPhase::Crashed;
            } else {
                active.phase = settled_phase;
            }
            if active.phase == DeploymentPhase::Ready {
                for (index, previous) in service.deployments.iter_mut().enumerate() {
                    if index < active_index
                        && !matches!(
                            previous.phase,
                            DeploymentPhase::Canceled
                                | DeploymentPhase::Crashed
                                | DeploymentPhase::Terminated
                                | DeploymentPhase::Removed
                        )
                    {
                        previous.phase = if drain_grace_elapsed {
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
