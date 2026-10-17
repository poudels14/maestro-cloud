use std::collections::{BTreeMap, HashSet};
use std::future::Future;
use std::sync::{Arc, Mutex};
use std::time::Duration;

use maestro_core::{
    Assignment, Deployment, DeploymentId, DeploymentStatus, GitCommit, NodeId, NodeRecord,
    NodeState, ReplicaId, ReplicaPhase, ReplicaStatus, ResourceDefaults, ServiceId, ServiceSpec,
    UnschedulableReplica,
};
use maestro_store::Mutation;
use tokio::sync::oneshot;

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

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuiltImage {
    pub image: String,
    pub commit: Option<GitCommit>,
    pub built_on: Option<NodeId>,
}

pub trait ImageBuilder: Send + Sync + 'static {
    fn build(
        &self,
        deployment: Deployment,
        repository: ServiceId,
        checked_out: oneshot::Sender<GitCommit>,
    ) -> impl Future<Output = Result<BuiltImage, String>> + Send;

    fn resolve(&self, image: String) -> impl Future<Output = Result<String, String>> + Send;

    fn resolve_sources(
        &self,
        deployment: Deployment,
    ) -> impl Future<Output = Result<ServiceSpec, String>> + Send;
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
    unschedulable: Mutex<Vec<UnschedulableReplica>>,
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
            unschedulable: Mutex::default(),
            settings,
        }
    }

    pub async fn tick(&self) -> Result<(), ControlError> {
        lock_report(&self.unschedulable).clear();
        self.start_queued().await?;
        self.run_builds().await?;
        self.reschedule_lost().await?;
        self.place_deploying().await?;
        self.scale_active().await?;
        self.settle_deploying().await?;
        self.drain_superseded().await?;
        self.stop_drained().await?;
        self.release_inactive().await?;
        self.publish_unschedulable().await
    }

    async fn publish_unschedulable(&self) -> Result<(), ControlError> {
        let replicas = lock_report(&self.unschedulable).clone();
        match self.state().unschedulable()? == replicas {
            true => Ok(()),
            false => {
                self.write(vec![Mutation::SetUnschedulable { replicas }])
                    .await
            }
        }
    }

    fn note_unschedulable(&self, requests: &[PlacementRequest], failures: &[(ReplicaId, String)]) {
        let mut report = lock_report(&self.unschedulable);
        failures.iter().for_each(|(replica_id, reason)| {
            if let Some(request) = requests
                .iter()
                .find(|request| &request.replica_id == replica_id)
            {
                report.push(UnschedulableReplica {
                    service_id: request.service_id.clone(),
                    deployment_id: request.deployment_id.clone(),
                    replica_index: request.index,
                    reason: reason.clone(),
                });
            }
        });
    }

    async fn start_queued(&self) -> Result<(), ControlError> {
        let now = self.now();
        let (with_sources, queued): (Vec<_>, Vec<_>) = self
            .state()
            .deployments_with_status(DeploymentStatus::Queued)?
            .into_iter()
            .partition(|deployment| deployment.spec.has_secret_sources());
        with_sources
            .into_iter()
            .filter(|deployment| !lock(&self.active_resolutions).contains(&deployment.id))
            .for_each(|deployment| self.spawn_source_resolution(deployment));
        let (prepared, unresolved): (Vec<_>, Vec<_>) = queued
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
            .filter(|deployment| {
                deployment.spec.build.is_some()
                    && deployment.image.is_none()
                    && !deployment.spec.has_secret_sources()
            })
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

    fn spawn_source_resolution(&self, deployment: Deployment) {
        lock(&self.active_resolutions).insert(deployment.id.clone());
        let writer = self.writer.clone();
        let builder = self.builder.clone();
        let tracked = self.active_resolutions.clone();
        let clock = self.settings.clock.clone();
        tokio::spawn(async move {
            let result = builder.resolve_sources(deployment.clone()).await;
            let still_queued = writer
                .state()
                .deployment(&deployment.service_id, &deployment.id)
                .ok()
                .flatten()
                .is_some_and(|current| current.status == DeploymentStatus::Queued);
            let mutation = match result {
                Ok(spec) => Mutation::SetDeploymentSpec {
                    service_id: deployment.service_id.clone(),
                    deployment_id: deployment.id.clone(),
                    spec,
                },
                Err(reason) => status_change(
                    &deployment,
                    DeploymentStatus::Failed,
                    None,
                    Some(format!("secret resolution failed: {reason}")),
                    clock(),
                ),
            };
            if still_queued && let Err(error) = writer.write(vec![mutation]).await {
                tracing::warn!(deployment = %deployment.id, %error, "failed to record resolved sources");
            }
            lock(&tracked).remove(&deployment.id);
        });
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
                ImageStep::Build => {
                    let (checked_out, commit) = oneshot::channel();
                    tokio::spawn(record_commit(writer.clone(), deployment.clone(), commit));
                    let repository = writer
                        .state()
                        .preview_sources()
                        .ok()
                        .and_then(|sources| {
                            sources
                                .into_iter()
                                .find(|(id, _)| *id == deployment.service_id)
                        })
                        .map(|(_, source)| source.base_service_id)
                        .unwrap_or_else(|| deployment.service_id.clone());
                    (
                        builder
                            .build(deployment.clone(), repository, checked_out)
                            .await,
                        "build failed",
                    )
                }
                ImageStep::Resolve(image) => (
                    builder.resolve(image).await.map(|image| BuiltImage {
                        image,
                        commit: None,
                        built_on: None,
                    }),
                    "image resolution failed",
                ),
            };
            let (status, image, failure, build) = match result {
                Ok(built) => (
                    DeploymentStatus::Deploying,
                    Some(built.image),
                    None,
                    (built.commit.is_some() || built.built_on.is_some()).then_some(
                        Mutation::SetDeploymentBuild {
                            service_id: deployment.service_id.clone(),
                            deployment_id: deployment.id.clone(),
                            commit: built.commit,
                            built_on: built.built_on,
                        },
                    ),
                ),
                Err(reason) => (
                    DeploymentStatus::Failed,
                    None,
                    Some(format!("{failure_prefix}: {reason}")),
                    None,
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
                    .write(
                        build
                            .into_iter()
                            .chain([status_change(&deployment, status, image, failure, clock())])
                            .collect(),
                    )
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
                &self.available_nodes()?,
                &current,
                &requests,
                &self.settings.defaults,
            );
            self.note_unschedulable(&requests, &placement.unschedulable);
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
                    let request =
                        PlacementRequest::for_replica(deployment, *index, assignment.image.clone());
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
            self.note_unschedulable(&requests, &placement.unschedulable);
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
        let now = self.now();
        let grace_ms = self.settings.drain_grace.as_millis() as u64;
        let existing = self.all_assignments()?;
        let current = existing.values().flatten().cloned().collect::<Vec<_>>();
        let draining_nodes = self
            .state()
            .nodes()?
            .into_iter()
            .filter(|node| node.state == NodeState::Draining)
            .map(|node| node.id)
            .collect::<Vec<_>>();
        let active = [DeploymentStatus::Deploying, DeploymentStatus::Ready]
            .iter()
            .map(|status| self.state().deployments_with_status(*status))
            .collect::<Result<Vec<_>, _>>()?
            .concat();
        let mut released = Vec::new();
        let mut requests = Vec::new();
        for deployment in &active {
            let assigned = current
                .iter()
                .filter(|assignment| assignment.deployment_id == deployment.id)
                .filter(|assignment| !assignment.is_retiring())
                .collect::<Vec<_>>();
            if let Some(first) = assigned.first() {
                let (mut serving, draining): (Vec<&Assignment>, Vec<&Assignment>) = assigned
                    .iter()
                    .copied()
                    .partition(|assignment| !draining_nodes.contains(&assignment.node_id));
                serving.sort_by_key(|assignment| assignment.index);
                let target = self.target_replicas(deployment)? as usize;
                let next_index = current
                    .iter()
                    .filter(|assignment| assignment.deployment_id == deployment.id)
                    .map(|assignment| assignment.index + 1)
                    .max()
                    .unwrap_or_default();
                let healthy_serving = serving
                    .iter()
                    .take(target)
                    .filter(|assignment| self.is_healthy(&assignment.replica_id))
                    .count();
                released.extend(
                    serving
                        .iter()
                        .skip(target)
                        .map(|assignment| assignment.replica_id.clone()),
                );
                if healthy_serving >= target {
                    released.extend(
                        draining
                            .iter()
                            .map(|assignment| assignment.replica_id.clone()),
                    );
                }
                requests.extend(
                    (0..target.saturating_sub(serving.len()) as u32).map(|offset| {
                        PlacementRequest::for_replica(
                            deployment,
                            next_index + offset,
                            first.image.clone(),
                        )
                    }),
                );
            }
        }
        let retired = current
            .iter()
            .filter(|assignment| {
                assignment
                    .retiring_since_ms
                    .is_some_and(|since| now.saturating_sub(since) >= grace_ms)
            })
            .map(|assignment| assignment.replica_id.clone())
            .collect::<Vec<_>>();
        if !released.is_empty() || !requests.is_empty() || !retired.is_empty() {
            let placement = scheduler::place(
                &self.available_nodes()?,
                &current,
                &requests,
                &self.settings.defaults,
            );
            self.note_unschedulable(&requests, &placement.unschedulable);
            let mut next = existing;
            next.values_mut().for_each(|assignments| {
                assignments.retain(|assignment| !retired.contains(&assignment.replica_id));
                assignments
                    .iter_mut()
                    .filter(|assignment| released.contains(&assignment.replica_id))
                    .for_each(|assignment| {
                        assignment.retiring_since_ms.get_or_insert(now);
                    });
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

    fn available_nodes(&self) -> Result<Vec<NodeRecord>, ControlError> {
        Ok(self
            .state()
            .nodes()?
            .into_iter()
            .filter(|node| self.observed.node_available(node.id))
            .collect())
    }

    fn is_healthy(&self, replica_id: &ReplicaId) -> bool {
        self.observed
            .replica(replica_id)
            .is_some_and(|status| status.phase == ReplicaPhase::Healthy)
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
            max_concurrent_builds: 5,
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
        .map(|index| PlacementRequest::for_replica(deployment, index, image.clone()))
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

fn lock_report(
    report: &Mutex<Vec<UnschedulableReplica>>,
) -> std::sync::MutexGuard<'_, Vec<UnschedulableReplica>> {
    report
        .lock()
        .unwrap_or_else(|poisoned| poisoned.into_inner())
}

async fn record_commit<W: StoreWriter>(
    writer: Arc<W>,
    deployment: Deployment,
    commit: oneshot::Receiver<GitCommit>,
) {
    if let Ok(commit) = commit.await {
        let mutation = Mutation::SetDeploymentBuild {
            service_id: deployment.service_id,
            deployment_id: deployment.id.clone(),
            commit: Some(commit),
            built_on: None,
        };
        if let Err(error) = writer.write(vec![mutation]).await {
            tracing::warn!(deployment = %deployment.id, %error, "failed to record the build commit");
        }
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
