use std::{
    collections::{BTreeMap, BTreeSet, HashSet},
    sync::Arc,
    time::Duration,
};

use anyhow::{Result, anyhow};
use tokio::sync::broadcast;

use crate::{
    cluster::{
        Assignment, AssignmentManifest,
        assignment_store::{AssignmentStore, ReplaceOutcome},
        elector::{EtcdLeaderElector, LeaderElector},
        registry::{NodeAvailabilityEvent, NodeRegistry},
        scheduler::{self, ScheduleInput},
        traefik::{EtcdTrafficManager, RoutingTarget},
        types::{
            DeploymentGroup, LeadershipState, ServiceScheduleSpec, TrafficGeneration,
            UnschedulableReplica,
        },
    },
    deployment::{
        store::{AtomicDeploymentUpdates, ClusterStore},
        types::{Deployment, DeploymentStatus, ReplicaState},
    },
    logs::Logger,
    signal::ShutdownEvent,
    slack::SlackNotifier,
};

pub struct LeaderLoop {
    cluster_id: String,
    cluster_name: String,
    elector: Arc<EtcdLeaderElector>,
    assignments: Arc<dyn AssignmentStore>,
    registry: Arc<dyn NodeRegistry>,
    store: Arc<dyn ClusterStore>,
    traffic: Arc<EtcdTrafficManager>,
    logger: Logger,
    slack: SlackNotifier,
}

impl LeaderLoop {
    pub fn new(
        cluster_id: String,
        cluster_name: String,
        elector: Arc<EtcdLeaderElector>,
        assignments: Arc<dyn AssignmentStore>,
        registry: Arc<dyn NodeRegistry>,
        store: Arc<dyn ClusterStore>,
        traffic: Arc<EtcdTrafficManager>,
        logger: Logger,
        slack: SlackNotifier,
    ) -> Self {
        Self {
            cluster_id,
            cluster_name,
            elector,
            assignments,
            registry,
            store,
            traffic,
            logger,
            slack,
        }
    }

    pub async fn run(self, mut shutdown: broadcast::Receiver<ShutdownEvent>) {
        let mut leadership = self.elector.watch();
        let mut interval = tokio::time::interval(Duration::from_secs(5));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                _ = shutdown.recv() => break,
                changed = leadership.changed() => {
                    if changed.is_err() { break; }
                }
                _ = interval.tick() => {}
            }
            let LeadershipState::Leading(token) = leadership.borrow().clone() else {
                continue;
            };
            if let Err(error) = self.tick(&token).await {
                self.logger
                    .emit("error", &format!("leader scheduling tick failed: {error}"));
            }
        }
    }

    async fn tick(&self, token: &crate::cluster::types::LeadershipToken) -> Result<()> {
        if self.elector.state() != LeadershipState::Leading(token.clone()) {
            return Ok(());
        }
        self.traffic.reconcile_ingress_blocklist(token).await?;
        let now_ms = i64::try_from(crate::utils::time::current_time_millis()?)
            .map_err(|_| anyhow!("current time does not fit i64"))?;
        let nodes = self.registry.list_nodes().await?;
        let availability_events = self
            .registry
            .reconcile_liveness(token, &nodes, now_ms)
            .await?;
        self.notify_availability_events(availability_events, now_ms);
        self.store.sweep_cluster_state(token, now_ms).await?;
        let node_records = self
            .store
            .list_cluster_node_records()
            .await?
            .into_iter()
            .map(|record| (record.last_info.node_id.clone(), record))
            .collect::<BTreeMap<_, _>>();
        let mut node_states = BTreeMap::new();
        for node in &nodes {
            node_states.insert(
                node.node_id.clone(),
                self.registry.get_node_state(&node.node_id).await?,
            );
        }
        let manifests = self.assignments.list_all().await?;
        let current = manifests
            .iter()
            .flat_map(|manifest| manifest.assignments.iter().cloned())
            .collect::<Vec<_>>();
        let replica_states = self.assignments.list_replica_states().await?;
        let (services, mut validation_errors) =
            self.schedule_specs(&replica_states, now_ms).await?;
        let drain_specs = services.clone();
        let drain_node_states = node_states.clone();
        let current_for_drains = current.clone();

        let live_by_id = nodes
            .iter()
            .map(|node| (node.node_id.as_str(), node))
            .collect::<BTreeMap<_, _>>();
        let held = current
            .iter()
            .filter(
                |assignment| match live_by_id.get(assignment.node_id.as_str()) {
                    Some(node) => {
                        if node_states
                            .get(&assignment.node_id)
                            .is_some_and(|state| state.unschedulable)
                        {
                            return true;
                        }
                        if node.data_plane_ready
                            && now_ms.saturating_sub(node.data_plane_checked_at_ms) <= 15_000
                        {
                            return false;
                        }
                        node_records
                            .get(&assignment.node_id)
                            .and_then(|record| record.data_plane_lost_at_ms)
                            .is_none_or(|lost_at| now_ms.saturating_sub(lost_at) < 30_000)
                    }
                    None => node_records
                        .get(&assignment.node_id)
                        .and_then(|record| record.lost_at_ms)
                        .is_none_or(|lost_at| now_ms.saturating_sub(lost_at) < 30_000),
                },
            )
            .map(|assignment| assignment.assignment_id.clone())
            .collect::<BTreeSet<_>>();
        let mut planned = scheduler::plan(ScheduleInput {
            cluster_id: self.cluster_id.clone(),
            services,
            nodes: nodes.clone(),
            node_states,
            current,
            held,
            now_ms,
        });
        validation_errors.append(&mut planned.unschedulable);

        validation_errors.extend(
            self.coordinate_drain_assignments(
                token,
                &mut planned.assignments,
                &current_for_drains,
                &drain_specs,
                &nodes,
                &drain_node_states,
                &replica_states,
                now_ms,
            )
            .await?,
        );
        let traffic_by_service = self
            .traffic
            .list_traffic()
            .await?
            .into_iter()
            .map(|traffic| (traffic.service_id.clone(), traffic))
            .collect::<BTreeMap<_, _>>();
        let traffic_exclusions = hold_scaled_down_assignments(
            &mut planned.assignments,
            &current_for_drains,
            &drain_specs,
            &nodes,
            &replica_states,
            &traffic_by_service,
            now_ms,
        );
        validation_errors.extend(scheduler::ensure_workload_addresses(
            &mut planned.assignments,
            &nodes,
            &current_for_drains,
        ));

        let planned_assignments = planned.assignments;
        let mut desired_by_node = BTreeMap::<String, Vec<Assignment>>::new();
        for assignment in &planned_assignments {
            desired_by_node
                .entry(assignment.node_id.clone())
                .or_default()
                .push(assignment.clone());
        }
        let all_node_ids = manifests
            .iter()
            .map(|manifest| manifest.node_id.clone())
            .chain(nodes.iter().map(|node| node.node_id.clone()))
            .chain(desired_by_node.keys().cloned())
            .collect::<BTreeSet<_>>();
        let existing_by_node = manifests
            .into_iter()
            .map(|manifest| (manifest.node_id.clone(), manifest))
            .collect::<BTreeMap<_, _>>();
        for node_id in all_node_ids {
            let mut desired = desired_by_node.remove(&node_id).unwrap_or_default();
            desired.sort_by(|left, right| {
                left.service_id
                    .cmp(&right.service_id)
                    .then_with(|| left.deployment_id.cmp(&right.deployment_id))
                    .then_with(|| left.replica_index.cmp(&right.replica_index))
            });
            let current = existing_by_node.get(&node_id);
            if current.is_some_and(|manifest| manifest.assignments == desired) {
                continue;
            }
            let expected_generation = current.map(|manifest| manifest.generation).unwrap_or(0);
            let outcome = self
                .assignments
                .replace_for_node(
                    token,
                    expected_generation,
                    AssignmentManifest {
                        node_id: node_id.clone(),
                        generation: expected_generation,
                        assignments: desired,
                    },
                )
                .await?;
            match outcome {
                ReplaceOutcome::Applied | ReplaceOutcome::GenerationConflict => {}
                ReplaceOutcome::LeadershipLost => return Ok(()),
            }
        }
        let _ = self
            .assignments
            .write_unschedulable(token, &validation_errors)
            .await?;
        self.coordinate_rollouts(
            token,
            &planned_assignments,
            &replica_states,
            &nodes,
            &traffic_exclusions,
            now_ms,
        )
        .await?;
        Ok(())
    }

    fn notify_availability_events(&self, events: Vec<NodeAvailabilityEvent>, now_ms: i64) {
        for event in events {
            match event {
                NodeAvailabilityEvent::Down { node, .. } => {
                    self.logger.emit(
                        "error",
                        &format!(
                            "cluster node `{}` ({}) is down: control-plane heartbeat expired",
                            node.node_id, node.hostname
                        ),
                    );
                    self.slack.notify_node_down(&node);
                }
                NodeAvailabilityEvent::Recovered { node, since_ms } => {
                    let unavailable_for_ms = now_ms.saturating_sub(since_ms);
                    self.logger.emit(
                        "info",
                        &format!(
                            "cluster node `{}` ({}) recovered after {} ms",
                            node.node_id, node.hostname, unavailable_for_ms
                        ),
                    );
                    self.slack.notify_node_recovered(&node, unavailable_for_ms);
                }
                NodeAvailabilityEvent::DataPlaneUnavailable {
                    node,
                    since_ms,
                    reason,
                } => {
                    let unavailable_for_ms = now_ms.saturating_sub(since_ms);
                    self.logger.emit(
                        "error",
                        &format!(
                            "cluster node `{}` ({}) data plane is unavailable after {} ms: {reason}",
                            node.node_id, node.hostname, unavailable_for_ms
                        ),
                    );
                    self.slack
                        .notify_node_unavailable(&node, &reason, unavailable_for_ms);
                }
                NodeAvailabilityEvent::DataPlaneRecovered { node, since_ms } => {
                    let unavailable_for_ms = now_ms.saturating_sub(since_ms);
                    self.logger.emit(
                        "info",
                        &format!(
                            "cluster node `{}` ({}) data plane recovered after {} ms",
                            node.node_id, node.hostname, unavailable_for_ms
                        ),
                    );
                    self.slack.notify_node_available(&node, unavailable_for_ms);
                }
            }
        }
    }

    async fn schedule_specs(
        &self,
        replica_states: &[ReplicaState],
        now_ms: i64,
    ) -> Result<(Vec<ServiceScheduleSpec>, Vec<UnschedulableReplica>)> {
        let mut specs = Vec::new();
        let mut errors = Vec::new();
        for info in self.store.list_service_infos().await? {
            let service_id = info.config.id.clone();
            let mut deployments = self.store.list_service_deployments(&service_id).await?;
            deployments.retain(|deployment| {
                matches!(
                    deployment.status,
                    DeploymentStatus::Ready
                        | DeploymentStatus::PendingReady
                        | DeploymentStatus::Building
                        | DeploymentStatus::Draining
                ) && (deployment.build.is_some() || deployment.config.image.is_some())
                    && (deployment.status != DeploymentStatus::Draining
                        || deployment.drained_at.is_none_or(|drained_at| {
                            i64::try_from(drained_at).ok().is_some_and(|drained_at| {
                                now_ms < drained_at.saturating_add(30_000)
                            })
                        }))
            });
            deployments.sort_by(|left, right| {
                deployment_order(&left.status)
                    .cmp(&deployment_order(&right.status))
                    .then_with(|| left.created_at.cmp(&right.created_at))
                    .then_with(|| left.id.cmp(&right.id))
            });
            if deployments.is_empty() {
                continue;
            }
            let writable_volume = info
                .config
                .deploy
                .volumes
                .iter()
                .any(|volume| !volume.read_only);
            let hard_pinned = info
                .config
                .deploy
                .node_affinity
                .as_ref()
                .and_then(|affinity| affinity.node_id.as_ref())
                .is_some();
            if writable_volume && !hard_pinned {
                for deployment in &deployments {
                    for replica_index in 0..info.effective_replicas() {
                        errors.push(UnschedulableReplica {
                            service_id: service_id.clone(),
                            deployment_id: deployment.id.clone(),
                            replica_index,
                            reason: "writable host volumes require deploy.node-affinity.node-id"
                                .to_string(),
                        });
                    }
                }
            }
            let deployment_ids = deployments
                .iter()
                .map(|deployment| deployment.id.as_str())
                .collect::<HashSet<_>>();
            let unhealthy_slots = replica_states
                .iter()
                .filter(|state| state.status == DeploymentStatus::Crashed)
                .filter_map(|state| {
                    let deployment_id = state.deployment_id.as_ref()?;
                    if !deployment_ids.contains(deployment_id.as_str()) {
                        return None;
                    }
                    Some((
                        deployment_id.clone(),
                        state.node_id.clone()?,
                        state.replica_index,
                        state.assignment_id.clone()?,
                    ))
                })
                .collect();
            specs.push(ServiceScheduleSpec {
                service_id,
                groups: deployments
                    .into_iter()
                    .map(|deployment| DeploymentGroup {
                        deployment_id: deployment.id,
                        replicas: info.effective_replicas(),
                    })
                    .collect(),
                node_affinity: if writable_volume && !hard_pinned {
                    Some(crate::cluster::NodeAffinity {
                        node_id: Some("writable-volume-requires-explicit-node-pin".to_string()),
                        labels: BTreeMap::new(),
                    })
                } else {
                    info.config.deploy.node_affinity.clone()
                },
                unhealthy_slots,
            });
        }
        Ok((specs, errors))
    }

    #[allow(clippy::too_many_arguments)]
    async fn coordinate_drain_assignments(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        planned: &mut Vec<Assignment>,
        current: &[Assignment],
        specs: &[ServiceScheduleSpec],
        nodes: &[crate::cluster::NodeInfo],
        node_states: &BTreeMap<String, crate::cluster::NodeState>,
        replica_states: &[ReplicaState],
        now_ms: i64,
    ) -> Result<Vec<UnschedulableReplica>> {
        if self.elector.state() != LeadershipState::Leading(token.clone()) {
            return Ok(Vec::new());
        }
        let mut errors = Vec::new();
        let mut staged = 0_usize;
        let sources = current
            .iter()
            .filter(|assignment| {
                node_states
                    .get(&assignment.node_id)
                    .is_some_and(|state| state.unschedulable)
            })
            .cloned()
            .collect::<Vec<_>>();
        for source in sources {
            let Some(spec) = specs.iter().find(|spec| {
                spec.service_id == source.service_id
                    && spec
                        .groups
                        .iter()
                        .any(|group| group.deployment_id == source.deployment_id)
            }) else {
                continue;
            };
            if spec
                .groups
                .iter()
                .find(|group| group.deployment_id == source.deployment_id)
                .is_some_and(|group| source.replica_index >= group.replicas)
            {
                continue;
            }
            let mut replacement = current
                .iter()
                .chain(planned.iter())
                .filter(|assignment| {
                    assignment.replaces_assignment_id.as_deref()
                        == Some(source.assignment_id.as_str())
                })
                .max_by_key(|assignment| assignment.placement_epoch)
                .cloned();
            let traffic = self.traffic.read_traffic(&source.service_id).await?;
            if let (Some(replacement), Some(traffic)) = (&replacement, &traffic)
                && traffic
                    .active_assignment_ids
                    .contains(&replacement.assignment_id)
                && !traffic
                    .active_assignment_ids
                    .contains(&source.assignment_id)
                && now_ms >= traffic.drain_old_after_ms
            {
                planned.retain(|assignment| assignment.assignment_id != source.assignment_id);
                if !planned
                    .iter()
                    .any(|assignment| assignment.assignment_id == replacement.assignment_id)
                {
                    planned.push(replacement.clone());
                }
                continue;
            }
            if replacement.as_ref().is_some_and(|replacement| {
                replica_states.iter().any(|state| {
                    state.assignment_id.as_deref() == Some(&replacement.assignment_id)
                        && state.status == DeploymentStatus::Crashed
                })
            }) && traffic.as_ref().is_none_or(|traffic| {
                replacement.as_ref().is_none_or(|replacement| {
                    !traffic
                        .active_assignment_ids
                        .contains(&replacement.assignment_id)
                })
            }) && let Some(failed) = replacement.take()
            {
                planned.retain(|assignment| assignment.assignment_id != failed.assignment_id);
            }
            if let Some(replacement) = replacement {
                if !planned
                    .iter()
                    .any(|assignment| assignment.assignment_id == replacement.assignment_id)
                {
                    planned.push(replacement);
                }
                continue;
            }
            if staged >= 2 {
                continue;
            }
            let failed_nodes = replica_states
                .iter()
                .filter(|state| state.status == DeploymentStatus::Crashed)
                .filter_map(|state| {
                    let assignment_id = state.assignment_id.as_deref()?;
                    current
                        .iter()
                        .find(|assignment| assignment.assignment_id == assignment_id)
                        .filter(|assignment| {
                            assignment.replaces_assignment_id.as_deref()
                                == Some(source.assignment_id.as_str())
                        })
                        .map(|assignment| assignment.node_id.as_str())
                })
                .collect::<HashSet<_>>();
            let mut candidates = nodes
                .iter()
                .filter(|node| node.node_id != source.node_id)
                .filter(|node| node.role.runs_workloads())
                .filter(|node| node.data_plane_ready)
                .filter(|node| now_ms.saturating_sub(node.data_plane_checked_at_ms) <= 15_000)
                .filter(|node| {
                    !node_states
                        .get(&node.node_id)
                        .is_some_and(|state| state.unschedulable)
                })
                .filter(|node| !failed_nodes.contains(node.node_id.as_str()))
                .filter(|node| {
                    spec.node_affinity.as_ref().is_none_or(|affinity| {
                        affinity
                            .node_id
                            .as_ref()
                            .is_none_or(|node_id| node_id == &node.node_id)
                            && affinity
                                .labels
                                .iter()
                                .all(|(key, value)| node.labels.get(key) == Some(value))
                    })
                })
                .collect::<Vec<_>>();
            candidates.sort_by_key(|node| {
                let same_deployment = planned.iter().any(|assignment| {
                    assignment.node_id == node.node_id
                        && assignment.service_id == source.service_id
                        && assignment.deployment_id == source.deployment_id
                });
                let load = planned
                    .iter()
                    .filter(|assignment| assignment.node_id == node.node_id)
                    .count();
                (
                    same_deployment,
                    load,
                    node.role != crate::cluster::NodeRole::Worker,
                    node.node_id.as_str(),
                )
            });
            let Some(target) = candidates.first() else {
                errors.push(UnschedulableReplica {
                    service_id: source.service_id.clone(),
                    deployment_id: source.deployment_id.clone(),
                    replica_index: source.replica_index,
                    reason: format!(
                        "drain blocked: no replacement target for node {}",
                        source.node_id
                    ),
                });
                continue;
            };
            let epoch = current
                .iter()
                .chain(planned.iter())
                .filter(|assignment| {
                    assignment.service_id == source.service_id
                        && assignment.deployment_id == source.deployment_id
                        && assignment.replica_index == source.replica_index
                })
                .map(|assignment| assignment.placement_epoch)
                .max()
                .unwrap_or(source.placement_epoch)
                .saturating_add(1);
            planned.push(Assignment {
                assignment_id: scheduler::assignment_id(
                    &self.cluster_id,
                    &source.service_id,
                    &source.deployment_id,
                    source.replica_index,
                    &target.node_id,
                    epoch,
                ),
                placement_epoch: epoch,
                service_id: source.service_id.clone(),
                deployment_id: source.deployment_id.clone(),
                replica_index: source.replica_index,
                node_id: target.node_id.clone(),
                container_ip: None,
                replaces_assignment_id: Some(source.assignment_id.clone()),
                created_at_ms: now_ms,
            });
            staged += 1;
        }
        planned.sort_by(|left, right| {
            left.node_id
                .cmp(&right.node_id)
                .then_with(|| left.service_id.cmp(&right.service_id))
                .then_with(|| left.deployment_id.cmp(&right.deployment_id))
                .then_with(|| left.replica_index.cmp(&right.replica_index))
                .then_with(|| left.assignment_id.cmp(&right.assignment_id))
        });
        Ok(errors)
    }

    async fn coordinate_rollouts(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        assignments: &[Assignment],
        states: &[ReplicaState],
        nodes: &[crate::cluster::NodeInfo],
        traffic_exclusions: &BTreeSet<String>,
        now_ms: i64,
    ) -> Result<()> {
        let service_infos = self.store.list_service_infos().await?;
        let service_ids = service_infos
            .iter()
            .map(|info| info.config.id.as_str())
            .collect::<HashSet<_>>();
        for stale in self.traffic.list_traffic().await? {
            if !service_ids.contains(stale.service_id.as_str()) {
                self.traffic
                    .remove_service(token, &stale.service_id)
                    .await?;
            }
        }
        let live_ready_nodes = nodes
            .iter()
            .filter(|node| {
                node.data_plane_ready
                    && now_ms.saturating_sub(node.data_plane_checked_at_ms) <= 15_000
            })
            .map(|node| node.node_id.as_str())
            .collect::<HashSet<_>>();
        for info in service_infos {
            let service_id = info.config.id.clone();
            let deployments = self.store.list_service_deployments(&service_id).await?;
            let incoming = deployments.iter().find(|deployment| {
                matches!(
                    deployment.status,
                    DeploymentStatus::PendingReady | DeploymentStatus::Building
                ) && (deployment.build.is_some() || deployment.config.image.is_some())
            });
            let current_traffic = self.traffic.read_traffic(&service_id).await?;
            let ready_deployment = deployments
                .iter()
                .find(|deployment| deployment.status == DeploymentStatus::Ready);
            let target_deployment = incoming
                .or_else(|| {
                    current_traffic.as_ref().and_then(|traffic| {
                        deployments
                            .iter()
                            .find(|deployment| deployment.id == traffic.deployment_id)
                    })
                })
                .or(ready_deployment);
            let Some(target_deployment) = target_deployment else {
                continue;
            };
            let desired = assignments
                .iter()
                .filter(|assignment| {
                    assignment.service_id == service_id
                        && assignment.deployment_id == target_deployment.id
                        && !traffic_exclusions.contains(&assignment.assignment_id)
                })
                .collect::<Vec<_>>();
            let exact_ready = desired
                .iter()
                .filter_map(|assignment| {
                    if !live_ready_nodes.contains(assignment.node_id.as_str()) {
                        return None;
                    }
                    let state = states.iter().find(|state| {
                        state.status == DeploymentStatus::Ready
                            && state.node_id.as_ref() == Some(&assignment.node_id)
                            && state.assignment_id.as_deref()
                                == Some(assignment.assignment_id.as_str())
                    })?;
                    Some((assignment, state))
                })
                .collect::<Vec<_>>();
            let has_active_drain_chain = desired.iter().any(|replacement| {
                replacement
                    .replaces_assignment_id
                    .as_ref()
                    .is_some_and(|source_id| {
                        desired
                            .iter()
                            .any(|source| source.assignment_id == *source_id)
                    })
            });
            if has_active_drain_chain
                && current_traffic
                    .as_ref()
                    .is_some_and(|traffic| traffic.deployment_id == target_deployment.id)
            {
                self.coordinate_drain_traffic(
                    token,
                    &service_id,
                    target_deployment,
                    &desired,
                    &exact_ready,
                    current_traffic.as_ref().expect("checked above"),
                    now_ms,
                )
                .await?;
                self.traffic
                    .gc_old_generations(token, &service_id, now_ms)
                    .await?;
                continue;
            }
            let all_ready = desired.len()
                == usize::try_from(info.effective_replicas()).unwrap_or(usize::MAX)
                && exact_ready.len() == desired.len();
            if incoming.is_some_and(|incoming| incoming.id == target_deployment.id) && !all_ready {
                if let Some(current) = &current_traffic
                    && let Some(active) = deployments
                        .iter()
                        .find(|deployment| deployment.id == current.deployment_id)
                {
                    self.refresh_active_routing(
                        token,
                        &service_id,
                        active,
                        assignments,
                        states,
                        &live_ready_nodes,
                        traffic_exclusions,
                        now_ms,
                    )
                    .await?;
                }
                self.traffic
                    .gc_old_generations(token, &service_id, now_ms)
                    .await?;
                continue;
            }
            let targets = exact_ready
                .iter()
                .map(|(assignment, state)| RoutingTarget {
                    assignment: (**assignment).clone(),
                    endpoint: state.endpoint.clone(),
                })
                .collect::<Vec<_>>();
            if target_deployment.config.ingress.is_some()
                && targets.iter().any(|target| target.endpoint.is_none())
            {
                continue;
            }
            let status_updates = if let Some(incoming) =
                incoming.filter(|incoming| incoming.id == target_deployment.id)
            {
                let incoming_ref = Deployment {
                    service_id: service_id.clone(),
                    id: incoming.id.clone(),
                    replica_index: 0,
                };
                let draining = deployments
                    .iter()
                    .filter(|deployment| {
                        deployment.status == DeploymentStatus::Ready && deployment.id != incoming.id
                    })
                    .map(|deployment| Deployment {
                        service_id: service_id.clone(),
                        id: deployment.id.clone(),
                        replica_index: 0,
                    })
                    .collect::<Vec<_>>();
                self.store
                    .prepare_rollout_status_cutover(
                        &incoming_ref,
                        &draining,
                        u64::try_from(now_ms).unwrap_or_default(),
                    )
                    .await?
            } else {
                AtomicDeploymentUpdates::default()
            };
            self.traffic
                .cutover(
                    token,
                    &self.cluster_name,
                    &service_id,
                    &target_deployment.id,
                    target_deployment.config.ingress.as_ref(),
                    targets,
                    now_ms,
                    status_updates,
                )
                .await?;
            self.traffic
                .gc_old_generations(token, &service_id, now_ms)
                .await?;
        }
        Ok(())
    }

    async fn coordinate_drain_traffic(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        service_id: &str,
        deployment: &crate::deployment::types::ServiceDeployment,
        desired: &[&Assignment],
        exact_ready: &[(&&Assignment, &ReplicaState)],
        current: &crate::cluster::TrafficGeneration,
        now_ms: i64,
    ) -> Result<()> {
        let ready_replacements = desired
            .iter()
            .filter(|assignment| assignment.replaces_assignment_id.is_some())
            .filter(|assignment| {
                exact_ready
                    .iter()
                    .any(|(ready, _)| ready.assignment_id == assignment.assignment_id)
            })
            .copied()
            .collect::<Vec<_>>();
        let mut serving = current
            .active_assignment_ids
            .iter()
            .filter_map(|active_id| {
                ready_replacements
                    .iter()
                    .find(|replacement| {
                        replacement.replaces_assignment_id.as_deref() == Some(active_id.as_str())
                    })
                    .copied()
                    .or_else(|| {
                        desired
                            .iter()
                            .find(|assignment| assignment.assignment_id == *active_id)
                            .copied()
                    })
            })
            .collect::<Vec<_>>();
        for replacement in ready_replacements {
            let source_active = replacement
                .replaces_assignment_id
                .as_ref()
                .is_some_and(|source| current.active_assignment_ids.contains(source));
            let target_active = current
                .active_assignment_ids
                .contains(&replacement.assignment_id);
            if !source_active && !target_active {
                serving.push(replacement);
            }
        }
        serving.sort_by(|left, right| left.assignment_id.cmp(&right.assignment_id));
        serving.dedup_by(|left, right| left.assignment_id == right.assignment_id);
        let targets = serving
            .into_iter()
            .filter_map(|assignment| {
                let (_, state) = exact_ready
                    .iter()
                    .find(|(ready, _)| ready.assignment_id == assignment.assignment_id)?;
                Some(RoutingTarget {
                    assignment: assignment.clone(),
                    endpoint: state.endpoint.clone(),
                })
            })
            .collect::<Vec<_>>();
        if deployment.config.ingress.is_some()
            && targets.iter().any(|target| target.endpoint.is_none())
        {
            return Ok(());
        }
        if targets.is_empty() {
            return Ok(());
        }
        self.traffic
            .cutover(
                token,
                &self.cluster_name,
                service_id,
                &deployment.id,
                deployment.config.ingress.as_ref(),
                targets,
                now_ms,
                AtomicDeploymentUpdates::default(),
            )
            .await?;
        Ok(())
    }

    async fn refresh_active_routing(
        &self,
        token: &crate::cluster::types::LeadershipToken,
        service_id: &str,
        deployment: &crate::deployment::types::ServiceDeployment,
        assignments: &[Assignment],
        states: &[ReplicaState],
        live_ready_nodes: &HashSet<&str>,
        traffic_exclusions: &BTreeSet<String>,
        now_ms: i64,
    ) -> Result<()> {
        let targets = assignments
            .iter()
            .filter(|assignment| {
                assignment.service_id == service_id
                    && assignment.deployment_id == deployment.id
                    && live_ready_nodes.contains(assignment.node_id.as_str())
                    && !traffic_exclusions.contains(&assignment.assignment_id)
            })
            .filter_map(|assignment| {
                let state = states.iter().find(|state| {
                    state.status == DeploymentStatus::Ready
                        && state.node_id.as_ref() == Some(&assignment.node_id)
                        && state.assignment_id.as_deref() == Some(assignment.assignment_id.as_str())
                })?;
                Some(RoutingTarget {
                    assignment: assignment.clone(),
                    endpoint: state.endpoint.clone(),
                })
            })
            .collect();
        self.traffic
            .cutover(
                token,
                &self.cluster_name,
                service_id,
                &deployment.id,
                deployment.config.ingress.as_ref(),
                targets,
                now_ms,
                AtomicDeploymentUpdates::default(),
            )
            .await?;
        Ok(())
    }
}

fn deployment_order(status: &DeploymentStatus) -> u8 {
    match status {
        DeploymentStatus::Draining => 0,
        DeploymentStatus::Ready => 1,
        DeploymentStatus::PendingReady => 2,
        DeploymentStatus::Building => 3,
        _ => 4,
    }
}

fn hold_scaled_down_assignments(
    planned: &mut Vec<Assignment>,
    current: &[Assignment],
    specs: &[ServiceScheduleSpec],
    nodes: &[crate::cluster::NodeInfo],
    states: &[ReplicaState],
    traffic_by_service: &BTreeMap<String, TrafficGeneration>,
    now_ms: i64,
) -> BTreeSet<String> {
    let desired_replicas = specs
        .iter()
        .flat_map(|spec| {
            spec.groups.iter().map(|group| {
                (
                    (spec.service_id.clone(), group.deployment_id.clone()),
                    group.replicas,
                )
            })
        })
        .collect::<BTreeMap<_, _>>();
    let live_ready_nodes = nodes
        .iter()
        .filter(|node| {
            node.data_plane_ready && now_ms.saturating_sub(node.data_plane_checked_at_ms) <= 15_000
        })
        .map(|node| node.node_id.as_str())
        .collect::<HashSet<_>>();
    let mut planned_ids = planned
        .iter()
        .map(|assignment| assignment.assignment_id.clone())
        .collect::<HashSet<_>>();
    let mut traffic_exclusions = BTreeSet::new();

    for assignment in current {
        let Some(replicas) = desired_replicas.get(&(
            assignment.service_id.clone(),
            assignment.deployment_id.clone(),
        )) else {
            continue;
        };
        if assignment.replica_index < *replicas {
            continue;
        }
        let Some(traffic) = traffic_by_service
            .get(&assignment.service_id)
            .filter(|traffic| traffic.deployment_id == assignment.deployment_id)
        else {
            continue;
        };
        let active = traffic
            .active_assignment_ids
            .contains(&assignment.assignment_id);
        let remaining_ready = (0..*replicas).all(|replica_index| {
            planned.iter().any(|candidate| {
                candidate.service_id == assignment.service_id
                    && candidate.deployment_id == assignment.deployment_id
                    && candidate.replica_index == replica_index
                    && live_ready_nodes.contains(candidate.node_id.as_str())
                    && states.iter().any(|state| {
                        state.status == DeploymentStatus::Ready
                            && state.node_id.as_ref() == Some(&candidate.node_id)
                            && state.assignment_id.as_deref()
                                == Some(candidate.assignment_id.as_str())
                    })
            })
        });
        let hold = if active {
            if remaining_ready {
                traffic_exclusions.insert(assignment.assignment_id.clone());
            }
            true
        } else if now_ms < traffic.drain_old_after_ms {
            traffic_exclusions.insert(assignment.assignment_id.clone());
            true
        } else {
            false
        };
        if hold && planned_ids.insert(assignment.assignment_id.clone()) {
            planned.push(assignment.clone());
        }
    }
    planned.sort_by(|left, right| {
        left.node_id
            .cmp(&right.node_id)
            .then_with(|| left.service_id.cmp(&right.service_id))
            .then_with(|| left.deployment_id.cmp(&right.deployment_id))
            .then_with(|| left.replica_index.cmp(&right.replica_index))
            .then_with(|| left.assignment_id.cmp(&right.assignment_id))
    });
    traffic_exclusions
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cluster::{NodeInfo, NodeRole};
    use std::net::Ipv4Addr;

    fn assignment(index: u32) -> Assignment {
        Assignment {
            assignment_id: format!("assignment-{index}"),
            placement_epoch: 1,
            service_id: "web".to_string(),
            deployment_id: "dep1".to_string(),
            replica_index: index,
            node_id: format!("node-{index}"),
            container_ip: None,
            replaces_assignment_id: None,
            created_at_ms: 1,
        }
    }

    fn node(index: u32) -> NodeInfo {
        NodeInfo {
            node_id: format!("node-{index}"),
            instance_id: format!("instance-{index}"),
            hostname: format!("node-{index}"),
            role: NodeRole::Worker,
            cluster_host_ip: Ipv4Addr::new(10, 20, 0, u8::try_from(index + 1).unwrap()),
            cluster_api_port: 3001,
            cluster_gateway_port: 3002,
            subnet: format!("172.22.{}.0/24", index + 1),
            tailscale_ip: None,
            data_plane_ready: true,
            data_plane_checked_at_ms: 1_000,
            data_plane_error: None,
            version: "test".to_string(),
            started_at_ms: 1,
            labels: BTreeMap::new(),
        }
    }

    fn state(index: u32, status: DeploymentStatus) -> ReplicaState {
        ReplicaState {
            service_id: Some("web".to_string()),
            deployment_id: Some("dep1".to_string()),
            replica_index: index,
            status,
            healthcheck_failures: 0,
            restart_attempts: 0,
            node_id: Some(format!("node-{index}")),
            assignment_id: Some(format!("assignment-{index}")),
            endpoint: None,
            error: None,
        }
    }

    fn spec(replicas: u32) -> ServiceScheduleSpec {
        ServiceScheduleSpec {
            service_id: "web".to_string(),
            groups: vec![DeploymentGroup {
                deployment_id: "dep1".to_string(),
                replicas,
            }],
            node_affinity: None,
            unhealthy_slots: BTreeSet::new(),
        }
    }

    fn traffic(active: &[u32], drain_old_after_ms: i64) -> BTreeMap<String, TrafficGeneration> {
        BTreeMap::from([(
            "web".to_string(),
            TrafficGeneration {
                service_id: "web".to_string(),
                deployment_id: "dep1".to_string(),
                traffic_epoch: 1,
                active_assignment_ids: active
                    .iter()
                    .map(|index| format!("assignment-{index}"))
                    .collect(),
                active_node_ids: active.iter().map(|index| format!("node-{index}")).collect(),
                generation: "generation".to_string(),
                routing_fingerprint: String::new(),
                switched_at_ms: 1_000,
                drain_old_after_ms,
            },
        )])
    }

    #[test]
    fn active_scaled_down_assignment_is_held_while_traffic_is_removed() {
        let current = vec![assignment(0), assignment(1), assignment(2)];
        let mut planned = vec![assignment(0), assignment(1)];
        let nodes = vec![node(0), node(1), node(2)];
        let states = vec![
            state(0, DeploymentStatus::Ready),
            state(1, DeploymentStatus::Ready),
            state(2, DeploymentStatus::Ready),
        ];

        let exclusions = hold_scaled_down_assignments(
            &mut planned,
            &current,
            &[spec(2)],
            &nodes,
            &states,
            &traffic(&[0, 1, 2], 31_000),
            1_000,
        );

        assert_eq!(planned.len(), 3);
        assert_eq!(exclusions, BTreeSet::from(["assignment-2".to_string()]));
    }

    #[test]
    fn active_assignment_stays_in_traffic_until_remaining_slots_are_ready() {
        let current = vec![assignment(0), assignment(1), assignment(2)];
        let mut planned = vec![assignment(0), assignment(1)];
        let nodes = vec![node(0), node(1), node(2)];
        let states = vec![
            state(0, DeploymentStatus::Ready),
            state(1, DeploymentStatus::PendingReady),
            state(2, DeploymentStatus::Ready),
        ];

        let exclusions = hold_scaled_down_assignments(
            &mut planned,
            &current,
            &[spec(2)],
            &nodes,
            &states,
            &traffic(&[0, 1, 2], 31_000),
            1_000,
        );

        assert_eq!(planned.len(), 3);
        assert!(exclusions.is_empty());
    }

    #[test]
    fn retired_assignment_is_removed_only_after_the_drain_deadline() {
        let current = vec![assignment(0), assignment(1), assignment(2)];
        let nodes = vec![node(0), node(1), node(2)];
        let states = vec![
            state(0, DeploymentStatus::Ready),
            state(1, DeploymentStatus::Ready),
        ];
        let traffic = traffic(&[0, 1], 31_000);
        let mut draining = vec![assignment(0), assignment(1)];

        let exclusions = hold_scaled_down_assignments(
            &mut draining,
            &current,
            &[spec(2)],
            &nodes,
            &states,
            &traffic,
            30_999,
        );

        assert_eq!(draining.len(), 3);
        assert_eq!(exclusions, BTreeSet::from(["assignment-2".to_string()]));

        let mut completed = vec![assignment(0), assignment(1)];
        let exclusions = hold_scaled_down_assignments(
            &mut completed,
            &current,
            &[spec(2)],
            &nodes,
            &states,
            &traffic,
            31_000,
        );

        assert_eq!(completed.len(), 2);
        assert!(exclusions.is_empty());
    }
}
