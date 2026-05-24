//! Leader-only scheduling loop. Activates when this node becomes the elected
//! leader; deactivates when leadership is lost.
//!
//! On each tick (or on watched changes), the leader:
//!   1. Loads the current service registry + node list
//!   2. Ensures every service has an allocated port from the [`PortAllocator`]
//!   3. Asks the [`Scheduler`] to produce a [`SchedulePlan`]
//!   4. Writes per-node assignments into the [`AssignmentStore`]
//!   5. Rebuilds the Traefik config and writes it to the
//!      [`TraefikConfigSink`] (in production: etcd `traefik/` keys)

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{Result, anyhow};
use async_trait::async_trait;

use super::assignment_store::AssignmentStore;
use super::elector::LeaderElector;
use super::metrics::ClusterMetrics;
use super::port_allocator::PortAllocator;
use super::quorum::balance_for_odd_workload_count;
use super::registry::NodeRegistry;
use super::scheduler::Scheduler;
use super::scheduling::{Assignment, NodeCapacity, SchedulePlan, ServiceScheduleSpec};
use super::traefik_aggregator::{AggregatorInput, TraefikDynamicConfig, build_traefik_config};
use super::types::{LeadershipState, NodeId, NodeInfo};
use crate::deployment::types::ServiceConfig;
use crate::utils::time::current_time_millis;

#[async_trait]
pub trait ServiceCatalog: Send + Sync {
    async fn list(&self) -> Result<Vec<ServiceConfig>>;
    async fn current_deployment_id(&self, service_id: &str) -> Result<Option<String>>;
    /// Slots whose persisted replica state indicates they're not healthy and
    /// should be rescheduled. The scheduler treats these as unsticky so it
    /// will place them on a different node when possible.
    async fn unhealthy_slots(&self) -> Result<Vec<super::scheduling::ReplicaSlot>> {
        Ok(Vec::new())
    }
}

#[async_trait]
pub trait TraefikConfigSink: Send + Sync {
    async fn write(&self, config: &TraefikDynamicConfig) -> Result<()>;
}

pub struct LeaderLoop {
    pub catalog: Arc<dyn ServiceCatalog>,
    pub registry: Arc<dyn NodeRegistry>,
    pub assignments: Arc<dyn AssignmentStore>,
    pub port_allocator: Arc<dyn PortAllocator>,
    pub scheduler: Arc<dyn Scheduler>,
    pub traefik_sink: Arc<dyn TraefikConfigSink>,
    pub elector: Arc<dyn LeaderElector>,
    pub tick_interval: Duration,
    pub default_entry_point: String,
    /// Optional callback invoked with (assignments, nodes) after every successful
    /// scheduling tick. Used in production to update per-service DNS records.
    pub on_plan_applied: Option<Arc<dyn PlanObserver>>,
    /// Optional metrics sink. Counters incremented per tick.
    pub metrics: Option<Arc<ClusterMetrics>>,
}

#[async_trait]
pub trait PlanObserver: Send + Sync {
    async fn observe(&self, assignments: &[Assignment], nodes: &[NodeInfo]);
}

impl LeaderLoop {
    pub async fn run_once(&self, now_ms: u64) -> Result<SchedulePlan> {
        let started_at = std::time::Instant::now();
        if let Some(metrics) = self.metrics.as_ref() {
            ClusterMetrics::incr(&metrics.scheduling_ticks);
        }
        if !matches!(self.elector.state(), LeadershipState::Leading(_)) {
            if let Some(metrics) = self.metrics.as_ref() {
                ClusterMetrics::incr(&metrics.scheduling_tick_failures);
            }
            return Err(anyhow!("run_once invoked on non-leader"));
        }
        let services = self.catalog.list().await?;
        let nodes = self.registry.list_nodes().await?;
        let nodes = balance_for_odd_workload_count(&nodes);
        self.sweep_stale_state(&services, &nodes).await;
        let mut specs: Vec<ServiceScheduleSpec> = Vec::with_capacity(services.len());
        for service in &services {
            let port = self.port_allocator.allocate(&service.id).await?;
            let deployment_id = self
                .catalog
                .current_deployment_id(&service.id)
                .await?
                .unwrap_or_default();
            specs.push(ServiceScheduleSpec {
                service_id: service.id.clone(),
                deployment_id,
                desired_replicas: service.deploy.replicas,
                node_affinity: service.deploy.node_affinity.clone(),
                assigned_port: Some(port),
            });
        }

        let capacities: Vec<NodeCapacity> = nodes
            .iter()
            .map(|node| NodeCapacity {
                node_id: node.node_id.clone(),
                labels: node.labels.clone(),
                can_run_workloads: node.role.can_run_workloads() && !node.unschedulable,
            })
            .collect();

        let existing = self.assignments.list_all().await.unwrap_or_default();
        let unhealthy = self.catalog.unhealthy_slots().await.unwrap_or_default();
        if let Some(metrics) = self.metrics.as_ref() {
            ClusterMetrics::set(&metrics.current_unhealthy_replicas, unhealthy.len() as u64);
        }
        let plan = self
            .scheduler
            .plan(&specs, &capacities, &existing, &unhealthy, now_ms);

        let mut per_node: BTreeMap<NodeId, Vec<Assignment>> = BTreeMap::new();
        for assignment in &plan.assignments {
            per_node
                .entry(assignment.node_id.clone())
                .or_default()
                .push(assignment.clone());
        }
        for node in &nodes {
            let assignments = per_node.remove(&node.node_id).unwrap_or_default();
            if let Err(err) = self
                .assignments
                .replace_for_node(&node.node_id, &assignments)
                .await
            {
                eprintln!(
                    "leader-loop: failed to write assignments for {}: {err}",
                    node.node_id
                );
            }
        }
        for (node_id, _) in per_node {
            eprintln!(
                "leader-loop: scheduler produced assignments for unknown node {node_id}; skipping"
            );
        }

        let traefik_config = build_traefik_config(AggregatorInput {
            services: &services,
            assignments: &plan.assignments,
            nodes: &nodes,
            default_entry_point: &self.default_entry_point,
        });
        if let Err(err) = self.traefik_sink.write(&traefik_config).await {
            eprintln!("leader-loop: failed to write traefik config: {err}");
        }

        if let Some(observer) = self.on_plan_applied.as_ref() {
            observer.observe(&plan.assignments, &nodes).await;
        }

        if let Some(metrics) = self.metrics.as_ref() {
            ClusterMetrics::add(
                &metrics.scheduling_tick_duration_ms_total,
                started_at.elapsed().as_millis() as u64,
            );
        }

        Ok(plan)
    }

    async fn sweep_stale_state(
        &self,
        services: &[crate::deployment::types::ServiceConfig],
        nodes: &[NodeInfo],
    ) {
        let live_node_ids: std::collections::HashSet<NodeId> =
            nodes.iter().map(|node| node.node_id.clone()).collect();
        let live_service_ids: std::collections::HashSet<String> =
            services.iter().map(|service| service.id.clone()).collect();

        let all_assignments = self.assignments.list_all().await.unwrap_or_default();
        let mut stale_by_node: BTreeMap<NodeId, Vec<Assignment>> = BTreeMap::new();
        let mut live_by_node: BTreeMap<NodeId, Vec<Assignment>> = BTreeMap::new();
        for assignment in all_assignments {
            if !live_node_ids.contains(&assignment.node_id) {
                stale_by_node
                    .entry(assignment.node_id.clone())
                    .or_default()
                    .push(assignment);
            } else if !live_service_ids.contains(&assignment.service_id) {
                stale_by_node
                    .entry(assignment.node_id.clone())
                    .or_default()
                    .push(assignment);
            } else {
                live_by_node
                    .entry(assignment.node_id.clone())
                    .or_default()
                    .push(assignment);
            }
        }
        let stale_count: usize = stale_by_node.values().map(|list| list.len()).sum();
        for (node_id, _) in &stale_by_node {
            let keep = live_by_node.remove(node_id).unwrap_or_default();
            if let Err(err) = self.assignments.replace_for_node(node_id, &keep).await {
                eprintln!("leader-loop: failed to drop stale assignments for {node_id}: {err}");
            }
        }
        if stale_count > 0 {
            if let Some(metrics) = self.metrics.as_ref() {
                ClusterMetrics::add(&metrics.stale_assignments_swept, stale_count as u64);
            }
        }

        // Releasing ports for vanished services is best-effort and deferred:
        // listing the allocator by probing every known service id is
        // impractical, so we rely on the catalog being the source of truth
        // and release lazily when a redeploy with a new id appears.
    }

    pub async fn run_while_leader(&self) {
        let mut leader_state = self.elector.subscribe();
        loop {
            if matches!(leader_state.borrow().clone(), LeadershipState::Leading(_)) {
                if let Err(err) = self.run_once(current_time_millis().unwrap_or(0)).await {
                    eprintln!("leader-loop tick failed: {err}");
                }
                tokio::select! {
                    _ = tokio::time::sleep(self.tick_interval) => {}
                    _ = leader_state.changed() => {}
                }
            } else if leader_state.changed().await.is_err() {
                return;
            }
        }
    }
}

#[allow(dead_code)]
pub fn list_all_node_ids(nodes: &[NodeInfo]) -> Vec<NodeId> {
    nodes.iter().map(|node| node.node_id.clone()).collect()
}
