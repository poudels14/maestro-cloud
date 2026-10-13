use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use maestro_core::{
    Assignment, Deployment, DeploymentStatus, NodeId, ReplicaId, ReplicaPhase, ReplicaStatus,
    ResourceDefaults, ServiceId,
};
use maestro_store::Mutation;

use crate::error::ControlError;
use crate::scheduler::{self, PlacementRequest};
use crate::writer::StoreWriter;

const ACTIVE_STATUSES: [DeploymentStatus; 3] = [
    DeploymentStatus::Deploying,
    DeploymentStatus::Ready,
    DeploymentStatus::Draining,
];

pub trait ObservedReplicas: Send + Sync + 'static {
    fn replica(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus>;

    fn replicas(&self) -> Vec<ReplicaStatus>;
}

#[derive(Clone)]
pub struct DeployerSettings {
    pub defaults: ResourceDefaults,
    pub deploy_timeout: Duration,
    pub drain_grace: Duration,
    pub history_limit: usize,
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,
}

pub struct Deployer<W: StoreWriter, O: ObservedReplicas> {
    writer: Arc<W>,
    observed: Arc<O>,
    settings: DeployerSettings,
}

impl<W: StoreWriter, O: ObservedReplicas> Deployer<W, O> {
    pub fn new(writer: Arc<W>, observed: Arc<O>, settings: DeployerSettings) -> Self {
        Self {
            writer,
            observed,
            settings,
        }
    }

    pub async fn tick(&self) -> Result<(), ControlError> {
        self.start_queued().await?;
        self.place_deploying().await?;
        self.settle_deploying().await?;
        self.drain_superseded().await?;
        self.stop_drained().await?;
        self.release_inactive().await
    }

    async fn start_queued(&self) -> Result<(), ControlError> {
        let now = self.now();
        let mutations = self
            .state()
            .deployments_with_status(DeploymentStatus::Queued)?
            .into_iter()
            .filter_map(|deployment| {
                deployment.spec.image.clone().map(|image| {
                    status_change(
                        &deployment,
                        DeploymentStatus::Deploying,
                        Some(image),
                        None,
                        now,
                    )
                })
            })
            .collect::<Vec<_>>();
        self.write(mutations).await
    }

    async fn place_deploying(&self) -> Result<(), ControlError> {
        let existing = self.all_assignments()?;
        let unplaced = self
            .state()
            .deployments_with_status(DeploymentStatus::Deploying)?
            .into_iter()
            .filter(|deployment| {
                !existing
                    .values()
                    .flatten()
                    .any(|assignment| assignment.deployment_id == deployment.id)
            })
            .collect::<Vec<_>>();
        let requests = unplaced
            .iter()
            .flat_map(placement_requests)
            .collect::<Vec<_>>();
        if !requests.is_empty() {
            let current = existing.values().flatten().cloned().collect::<Vec<_>>();
            let placement = scheduler::place(
                &self.state().nodes()?,
                &current,
                &requests,
                &self.settings.defaults,
            );
            let failed_deployments = unplaced
                .iter()
                .filter_map(|deployment| {
                    placement
                        .unschedulable
                        .iter()
                        .find(|(replica_id, _)| {
                            requests.iter().any(|request| {
                                &request.replica_id == replica_id
                                    && request.deployment_id == deployment.id
                            })
                        })
                        .map(|(_, reason)| (deployment, reason.clone()))
                })
                .collect::<Vec<_>>();
            let placed = placement
                .assignments
                .into_iter()
                .filter(|assignment| {
                    failed_deployments
                        .iter()
                        .all(|(deployment, _)| deployment.id != assignment.deployment_id)
                })
                .collect::<Vec<_>>();
            let now = self.now();
            let failures = failed_deployments.iter().map(|(deployment, reason)| {
                status_change(
                    deployment,
                    DeploymentStatus::Failed,
                    None,
                    Some(format!("unschedulable: {reason}")),
                    now,
                )
            });
            let mut next = existing;
            placed.into_iter().for_each(|assignment| {
                next.entry(assignment.node_id).or_default().push(assignment)
            });
            let assignment_writes =
                next.into_iter()
                    .map(|(node_id, assignments)| Mutation::SetNodeAssignments {
                        node_id,
                        assignments,
                    });
            self.write(failures.chain(assignment_writes).collect())
                .await?;
        }
        Ok(())
    }

    async fn settle_deploying(&self) -> Result<(), ControlError> {
        let now = self.now();
        let timeout_ms = self.settings.deploy_timeout.as_millis() as u64;
        let mutations = self
            .state()
            .deployments_with_status(DeploymentStatus::Deploying)?
            .into_iter()
            .filter_map(|deployment| {
                let phases = (0..deployment.spec.deploy.replicas)
                    .map(|index| {
                        self.observed
                            .replica(&ReplicaId::for_deployment(&deployment.id, index))
                            .map(|status| status.phase)
                    })
                    .collect::<Vec<_>>();
                let all_healthy = phases
                    .iter()
                    .all(|phase| *phase == Some(ReplicaPhase::Healthy));
                let any_failed = phases.contains(&Some(ReplicaPhase::Failed));
                let timed_out = now.saturating_sub(deployment.updated_at_ms) > timeout_ms;
                match (all_healthy, any_failed, timed_out) {
                    (true, _, _) => Some(status_change(
                        &deployment,
                        DeploymentStatus::Ready,
                        None,
                        None,
                        now,
                    )),
                    (false, true, _) => Some(status_change(
                        &deployment,
                        DeploymentStatus::Failed,
                        None,
                        Some("a replica failed to start".to_string()),
                        now,
                    )),
                    (false, false, true) => Some(status_change(
                        &deployment,
                        DeploymentStatus::Failed,
                        None,
                        Some("timed out waiting for replicas to become healthy".to_string()),
                        now,
                    )),
                    (false, false, false) => None,
                }
            })
            .collect::<Vec<_>>();
        self.write(mutations).await
    }

    async fn drain_superseded(&self) -> Result<(), ControlError> {
        let now = self.now();
        let ready = self
            .state()
            .deployments_with_status(DeploymentStatus::Ready)?
            .into_iter()
            .fold(
                BTreeMap::<ServiceId, Vec<Deployment>>::new(),
                |mut by_service, deployment| {
                    by_service
                        .entry(deployment.service_id.clone())
                        .or_default()
                        .push(deployment);
                    by_service
                },
            );
        let mutations = ready
            .into_values()
            .flat_map(|mut deployments| {
                deployments.sort_by_key(|deployment| deployment.created_at_ms);
                deployments.pop();
                deployments
            })
            .map(|deployment| {
                status_change(&deployment, DeploymentStatus::Draining, None, None, now)
            })
            .collect::<Vec<_>>();
        self.write(mutations).await
    }

    async fn stop_drained(&self) -> Result<(), ControlError> {
        let now = self.now();
        let grace_ms = self.settings.drain_grace.as_millis() as u64;
        let drained = self
            .state()
            .deployments_with_status(DeploymentStatus::Draining)?
            .into_iter()
            .filter(|deployment| now.saturating_sub(deployment.updated_at_ms) >= grace_ms)
            .collect::<Vec<_>>();
        let history_limit = self.settings.history_limit;
        let mutations = drained
            .iter()
            .flat_map(|deployment| {
                [
                    status_change(deployment, DeploymentStatus::Stopped, None, None, now),
                    Mutation::PruneDeployments {
                        service_id: deployment.service_id.clone(),
                        keep: history_limit,
                    },
                ]
            })
            .collect::<Vec<_>>();
        self.write(mutations).await
    }

    async fn release_inactive(&self) -> Result<(), ControlError> {
        let existing = self.all_assignments()?;
        let active = ACTIVE_STATUSES
            .iter()
            .map(|status| self.state().deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .map(|deployment| deployment.id)
            .collect::<Vec<_>>();
        let mutations = existing
            .into_iter()
            .filter_map(|(node_id, assignments)| {
                let kept = assignments
                    .iter()
                    .filter(|assignment| active.contains(&assignment.deployment_id))
                    .cloned()
                    .collect::<Vec<_>>();
                (kept.len() != assignments.len()).then_some(Mutation::SetNodeAssignments {
                    node_id,
                    assignments: kept,
                })
            })
            .collect::<Vec<_>>();
        self.write(mutations).await
    }

    fn all_assignments(&self) -> Result<BTreeMap<NodeId, Vec<Assignment>>, ControlError> {
        self.state()
            .nodes()?
            .iter()
            .map(|node| Ok((node.id, self.state().assignments(node.id)?)))
            .collect()
    }

    async fn write(&self, mutations: Vec<Mutation>) -> Result<(), ControlError> {
        if !mutations.is_empty() {
            self.writer.write(mutations).await?;
        }
        Ok(())
    }

    fn state(&self) -> &maestro_store::StateMachine {
        self.writer.state()
    }

    fn now(&self) -> u64 {
        (self.settings.clock)()
    }
}

impl Default for DeployerSettings {
    fn default() -> Self {
        Self {
            defaults: ResourceDefaults::default(),
            deploy_timeout: Duration::from_secs(600),
            drain_grace: Duration::from_secs(5),
            history_limit: 20,
            clock: Arc::new(unix_millis),
        }
    }
}

fn placement_requests(deployment: &Deployment) -> Vec<PlacementRequest> {
    let image = deployment
        .image
        .clone()
        .or_else(|| deployment.spec.image.clone())
        .unwrap_or_default();
    (0..deployment.spec.deploy.replicas)
        .map(|index| PlacementRequest {
            replica_id: ReplicaId::for_deployment(&deployment.id, index),
            service_id: deployment.service_id.clone(),
            deployment_id: deployment.id.clone(),
            index,
            image: image.clone(),
            resources: deployment.spec.deploy.resources.clone(),
        })
        .collect()
}

fn status_change(
    deployment: &Deployment,
    status: DeploymentStatus,
    image: Option<String>,
    failure: Option<String>,
    at_ms: u64,
) -> Mutation {
    Mutation::SetDeploymentStatus {
        service_id: deployment.service_id.clone(),
        deployment_id: deployment.id.clone(),
        status,
        image,
        failure,
        at_ms,
    }
}

pub(crate) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
