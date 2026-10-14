use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maestro_core::{
    Assignment, Deployment, DeploymentId, DeploymentStatus, NodeId, ReplicaId, ReplicaPhase,
    ReplicaStatus, ResourceDefaults, ServiceId,
};
use maestro_store::Mutation;

use crate::error::ControlError;
use crate::scheduler::{self, PlacementRequest};
use crate::writer::StoreWriter;

const SUPERSEDABLE_STATUSES: [DeploymentStatus; 4] = [
    DeploymentStatus::Queued,
    DeploymentStatus::Building,
    DeploymentStatus::Deploying,
    DeploymentStatus::Ready,
];

const ACTIVE_STATUSES: [DeploymentStatus; 3] = [
    DeploymentStatus::Deploying,
    DeploymentStatus::Ready,
    DeploymentStatus::Draining,
];

pub trait ObservedReplicas: Send + Sync + 'static {
    fn replica(&self, replica_id: &ReplicaId) -> Option<ReplicaStatus>;

    fn replicas(&self) -> Vec<ReplicaStatus>;

    fn node_available(&self, _node_id: NodeId) -> bool {
        true
    }
}

pub trait ImageBuilder: Send + Sync + 'static {
    fn build(&self, deployment: Deployment) -> impl Future<Output = Result<String, String>> + Send;

    fn resolve(&self, image: String) -> impl Future<Output = Result<String, String>> + Send;
}

enum ImageStep {
    Build,
    Resolve(String),
}

#[derive(Clone)]
pub struct DeployerSettings {
    pub defaults: ResourceDefaults,
    pub deploy_timeout: Duration,
    pub drain_grace: Duration,
    pub history_limit: usize,
    pub max_concurrent_builds: usize,
    pub clock: Arc<dyn Fn() -> u64 + Send + Sync>,
}

pub struct Deployer<W: StoreWriter, O: ObservedReplicas, B: ImageBuilder> {
    writer: Arc<W>,
    observed: Arc<O>,
    builder: Arc<B>,
    active_builds: Arc<Mutex<HashSet<DeploymentId>>>,
    active_resolutions: Arc<Mutex<HashSet<DeploymentId>>>,
    settings: DeployerSettings,
}

impl<W: StoreWriter, O: ObservedReplicas, B: ImageBuilder> Deployer<W, O, B> {
    pub fn new(
        writer: Arc<W>,
        observed: Arc<O>,
        builder: Arc<B>,
        settings: DeployerSettings,
    ) -> Self {
        Self {
            writer,
            observed,
            builder,
            active_builds: Arc::default(),
            active_resolutions: Arc::default(),
            settings,
        }
    }

    pub async fn tick(&self) -> Result<(), ControlError> {
        self.start_queued().await?;
        self.run_builds().await?;
        self.reschedule_lost().await?;
        self.place_deploying().await?;
        self.scale_active().await?;
        self.settle_deploying().await?;
        self.drain_superseded().await?;
        self.stop_drained().await?;
        self.release_inactive().await
    }

    async fn start_queued(&self) -> Result<(), ControlError> {
        let now = self.now();
        let (prepared, unresolved): (Vec<_>, Vec<_>) = self
            .state()
            .deployments_with_status(DeploymentStatus::Queued)?
            .into_iter()
            .partition(|deployment| deployment.image.is_some());
        unresolved
            .into_iter()
            .filter(|deployment| !lock(&self.active_resolutions).contains(&deployment.id))
            .for_each(|deployment| {
                if let Some(image) = deployment.spec.image.clone() {
                    self.spawn_image_step(
                        deployment,
                        DeploymentStatus::Queued,
                        ImageStep::Resolve(image),
                    );
                }
            });
        let mutations = prepared
            .iter()
            .map(|deployment| {
                status_change(deployment, DeploymentStatus::Deploying, None, None, now)
            })
            .collect();
        self.write(mutations).await
    }

    async fn run_builds(&self) -> Result<(), ControlError> {
        let state = self.state();
        let mut waiting = [DeploymentStatus::Building, DeploymentStatus::Queued]
            .iter()
            .map(|status| state.deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .concat()
            .into_iter()
            .filter(|deployment| deployment.spec.build.is_some() && deployment.image.is_none())
            .filter(|deployment| !self.is_building(&deployment.id))
            .collect::<Vec<_>>();
        waiting.sort_by_key(|deployment| {
            (
                deployment.status != DeploymentStatus::Building,
                deployment.created_at_ms,
            )
        });
        let capacity = self
            .settings
            .max_concurrent_builds
            .saturating_sub(lock(&self.active_builds).len());
        for deployment in waiting.into_iter().take(capacity) {
            if deployment.status == DeploymentStatus::Queued {
                let now = self.now();
                self.write(vec![status_change(
                    &deployment,
                    DeploymentStatus::Building,
                    None,
                    None,
                    now,
                )])
                .await?;
            }
            self.spawn_image_step(deployment, DeploymentStatus::Building, ImageStep::Build);
        }
        Ok(())
    }

    fn spawn_image_step(
        &self,
        deployment: Deployment,
        expected: DeploymentStatus,
        step: ImageStep,
    ) {
        let tracked = match step {
            ImageStep::Build => self.active_builds.clone(),
            ImageStep::Resolve(_) => self.active_resolutions.clone(),
        };
        lock(&tracked).insert(deployment.id.clone());
        let writer = self.writer.clone();
        let builder = self.builder.clone();
        let clock = self.settings.clock.clone();
        tokio::spawn(async move {
            let (result, failure_prefix) = match step {
                ImageStep::Build => (builder.build(deployment.clone()).await, "build failed"),
                ImageStep::Resolve(image) => {
                    (builder.resolve(image).await, "image resolution failed")
                }
            };
            let (status, image, failure) = match result {
                Ok(image) => (DeploymentStatus::Deploying, Some(image), None),
                Err(reason) => (
                    DeploymentStatus::Failed,
                    None,
                    Some(format!("{failure_prefix}: {reason}")),
                ),
            };
            let unchanged = writer
                .state()
                .deployment(&deployment.service_id, &deployment.id)
                .ok()
                .flatten()
                .is_some_and(|current| current.status == expected);
            let written = match unchanged {
                true => writer
                    .write(vec![status_change(
                        &deployment,
                        status,
                        image,
                        failure,
                        clock(),
                    )])
                    .await
                    .map(|_| ()),
                false => Ok(()),
            };
            if let Err(error) = written {
                tracing::warn!(deployment = %deployment.id, %error, "failed to record image result");
            }
            lock(&tracked).remove(&deployment.id);
        });
    }

    fn is_building(&self, deployment_id: &DeploymentId) -> bool {
        lock(&self.active_builds).contains(deployment_id)
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
            .map(|deployment| {
                Ok(placement_requests(
                    deployment,
                    self.target_replicas(deployment)?,
                ))
            })
            .collect::<Result<Vec<_>, ControlError>>()?
            .concat();
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

    async fn reschedule_lost(&self) -> Result<(), ControlError> {
        let nodes = self.state().nodes()?;
        let lost_nodes = nodes
            .iter()
            .filter(|node| !self.observed.node_available(node.id))
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let existing = self.all_assignments()?;
        let active = [DeploymentStatus::Deploying, DeploymentStatus::Ready]
            .iter()
            .map(|status| self.state().deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        let stranded = existing
            .iter()
            .filter(|(node_id, _)| lost_nodes.contains(node_id))
            .flat_map(|(_, assignments)| assignments.iter())
            .filter(|assignment| {
                active
                    .iter()
                    .any(|deployment| deployment.id == assignment.deployment_id)
            })
            .cloned()
            .collect::<Vec<_>>();
        if !stranded.is_empty() {
            let mut next_index = existing.values().flatten().fold(
                BTreeMap::<DeploymentId, u32>::new(),
                |mut next, assignment| {
                    let index = next.entry(assignment.deployment_id.clone()).or_default();
                    *index = (*index).max(assignment.index + 1);
                    next
                },
            );
            let requests = stranded
                .iter()
                .filter_map(|assignment| {
                    let deployment = active
                        .iter()
                        .find(|deployment| deployment.id == assignment.deployment_id)?;
                    let index = next_index.entry(deployment.id.clone()).or_default();
                    let request = PlacementRequest {
                        replica_id: ReplicaId::for_deployment(&deployment.id, *index),
                        service_id: deployment.service_id.clone(),
                        deployment_id: deployment.id.clone(),
                        index: *index,
                        image: assignment.image.clone(),
                        resources: deployment.spec.deploy.resources.clone(),
                    };
                    *index += 1;
                    Some(request)
                })
                .collect::<Vec<_>>();
            let healthy_nodes = nodes
                .into_iter()
                .filter(|node| !lost_nodes.contains(&node.id))
                .collect::<Vec<_>>();
            let surviving = existing
                .iter()
                .filter(|(node_id, _)| !lost_nodes.contains(node_id))
                .flat_map(|(_, assignments)| assignments.iter().cloned())
                .collect::<Vec<_>>();
            let placement = scheduler::place(
                &healthy_nodes,
                &surviving,
                &requests,
                &self.settings.defaults,
            );
            placement.unschedulable.iter().for_each(|(replica_id, reason)| {
                tracing::warn!(%replica_id, %reason, "cannot reschedule a replica from a lost node");
            });
            let mut next = existing;
            next.iter_mut()
                .filter(|(node_id, _)| lost_nodes.contains(node_id))
                .for_each(|(_, assignments)| {
                    assignments.retain(|assignment| !stranded.contains(assignment))
                });
            placement.assignments.into_iter().for_each(|assignment| {
                next.entry(assignment.node_id).or_default().push(assignment)
            });
            let mutations = next
                .into_iter()
                .map(|(node_id, assignments)| Mutation::SetNodeAssignments {
                    node_id,
                    assignments,
                })
                .collect();
            self.write(mutations).await?;
        }
        Ok(())
    }

    async fn settle_deploying(&self) -> Result<(), ControlError> {
        let now = self.now();
        let timeout_ms = self.settings.deploy_timeout.as_millis() as u64;
        let assignments = self.all_assignments()?;
        let mutations = self
            .state()
            .deployments_with_status(DeploymentStatus::Deploying)?
            .into_iter()
            .filter_map(|deployment| {
                let phases = assignments
                    .values()
                    .flatten()
                    .filter(|assignment| assignment.deployment_id == deployment.id)
                    .map(|assignment| {
                        self.observed
                            .replica(&assignment.replica_id)
                            .map(|status| status.phase)
                    })
                    .collect::<Vec<_>>();
                let target = self.target_replicas(&deployment).ok()?;
                let all_healthy = phases.len() as u32 == target
                    && phases
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

    async fn scale_active(&self) -> Result<(), ControlError> {
        let existing = self.all_assignments()?;
        let current = existing.values().flatten().cloned().collect::<Vec<_>>();
        let active = [DeploymentStatus::Deploying, DeploymentStatus::Ready]
            .iter()
            .map(|status| self.state().deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        let mut surplus = Vec::new();
        let mut requests = Vec::new();
        for deployment in &active {
            let mut assigned = current
                .iter()
                .filter(|assignment| assignment.deployment_id == deployment.id)
                .collect::<Vec<_>>();
            let target = self.target_replicas(deployment)? as usize;
            if !assigned.is_empty() && assigned.len() != target {
                assigned.sort_by_key(|assignment| assignment.index);
                let next_index = assigned.last().map_or(0, |assignment| assignment.index + 1);
                let image = assigned[0].image.clone();
                surplus.extend(
                    assigned
                        .iter()
                        .skip(target)
                        .map(|assignment| assignment.replica_id.clone()),
                );
                requests.extend(
                    (0..target.saturating_sub(assigned.len()) as u32).map(|offset| {
                        let index = next_index + offset;
                        PlacementRequest {
                            replica_id: ReplicaId::for_deployment(&deployment.id, index),
                            service_id: deployment.service_id.clone(),
                            deployment_id: deployment.id.clone(),
                            index,
                            image: image.clone(),
                            resources: deployment.spec.deploy.resources.clone(),
                        }
                    }),
                );
            }
        }
        if !surplus.is_empty() || !requests.is_empty() {
            let placement = scheduler::place(
                &self.state().nodes()?,
                &current,
                &requests,
                &self.settings.defaults,
            );
            placement
                .unschedulable
                .iter()
                .for_each(|(replica_id, reason)| {
                    tracing::warn!(%replica_id, %reason, "cannot place an added replica");
                });
            let mut next = existing;
            next.values_mut().for_each(|assignments| {
                assignments.retain(|assignment| !surplus.contains(&assignment.replica_id))
            });
            placement.assignments.into_iter().for_each(|assignment| {
                next.entry(assignment.node_id).or_default().push(assignment)
            });
            let mutations = next
                .into_iter()
                .map(|(node_id, assignments)| Mutation::SetNodeAssignments {
                    node_id,
                    assignments,
                })
                .collect();
            self.write(mutations).await?;
        }
        Ok(())
    }

    async fn drain_superseded(&self) -> Result<(), ControlError> {
        let now = self.now();
        let state = self.state();
        let newest_ready = state
            .deployments_with_status(DeploymentStatus::Ready)?
            .into_iter()
            .fold(
                BTreeMap::<ServiceId, u64>::new(),
                |mut newest, deployment| {
                    let created = newest.entry(deployment.service_id.clone()).or_default();
                    *created = (*created).max(deployment.created_at_ms);
                    newest
                },
            );
        let candidates = SUPERSEDABLE_STATUSES
            .iter()
            .map(|status| state.deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        let mutations = candidates
            .iter()
            .filter(|deployment| {
                newest_ready
                    .get(&deployment.service_id)
                    .is_some_and(|newest| deployment.created_at_ms < *newest)
            })
            .map(|deployment| {
                let next = match deployment.status {
                    DeploymentStatus::Ready => DeploymentStatus::Draining,
                    _ => DeploymentStatus::Cancelled,
                };
                status_change(deployment, next, None, None, now)
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

    fn target_replicas(&self, deployment: &Deployment) -> Result<u32, ControlError> {
        Ok(self
            .state()
            .service_controls(&deployment.service_id)?
            .effective_replicas(deployment.spec.deploy.replicas))
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
            max_concurrent_builds: 2,
            clock: Arc::new(unix_millis),
        }
    }
}

fn placement_requests(deployment: &Deployment, replicas: u32) -> Vec<PlacementRequest> {
    let image = deployment
        .image
        .clone()
        .or_else(|| deployment.spec.image.clone())
        .unwrap_or_default();
    (0..replicas)
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

fn lock(set: &Mutex<HashSet<DeploymentId>>) -> std::sync::MutexGuard<'_, HashSet<DeploymentId>> {
    set.lock().unwrap_or_else(|poisoned| poisoned.into_inner())
}

pub(crate) fn unix_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
