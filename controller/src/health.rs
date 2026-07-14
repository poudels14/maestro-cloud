//! Decision layer that sits between health observations (e.g. HTTP probes)
//! and the persistent store.
//!
//! The probe used to read/write [`ReplicaState`] from the store directly.
//! With this abstraction the probe just reports observations; the
//! [`ReplicaHealthMonitor`] decides when N failures becomes Crashed, etc.
//! Tests can hold the same monitor and drive transitions deterministically
//! without going through HTTP polling.

use std::sync::Arc;

use anyhow::Result;
use async_trait::async_trait;

use crate::deployment::store::ClusterStore;
use crate::deployment::types::{DeploymentStatus, ReplicaState};

pub const DEFAULT_MAX_HEALTHCHECK_FAILURES: u32 = 10;
pub const MAX_REPLICA_RESTART_ATTEMPTS: u32 = 10;

#[async_trait]
pub trait ReplicaHealthMonitor: Send + Sync {
    async fn report_healthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        assignment_id: Option<&str>,
    ) -> Result<()>;

    async fn report_unhealthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        assignment_id: Option<&str>,
        reason: &str,
    ) -> Result<()>;
}

pub struct DefaultHealthMonitor {
    store: Arc<dyn ClusterStore>,
    max_failures: u32,
    node_id: Option<String>,
}

impl DefaultHealthMonitor {
    pub fn new(store: Arc<dyn ClusterStore>, max_failures: u32) -> Self {
        Self {
            store,
            max_failures,
            node_id: None,
        }
    }

    pub fn for_node(mut self, node_id: Option<String>) -> Self {
        self.node_id = node_id;
        self
    }

    async fn current_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        assignment_id: Option<&str>,
    ) -> Option<ReplicaState> {
        let states = self
            .store
            .list_replica_states(service_id, deployment_id)
            .await
            .ok()?;
        if let Some(node_id) = &self.node_id
            && let Some(state) = states.iter().find(|state| {
                state.replica_index == replica_index
                    && state.node_id.as_ref() == Some(node_id)
                    && assignment_id.is_none_or(|assignment_id| {
                        state.assignment_id.as_deref() == Some(assignment_id)
                    })
            })
        {
            return Some(state.clone());
        }
        if assignment_id.is_some() {
            return None;
        }
        states
            .into_iter()
            .find(|state| state.replica_index == replica_index && state.node_id.as_ref().is_none())
    }
}

#[async_trait]
impl ReplicaHealthMonitor for DefaultHealthMonitor {
    async fn report_healthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        assignment_id: Option<&str>,
    ) -> Result<()> {
        let current = self
            .current_state(service_id, deployment_id, replica_index, assignment_id)
            .await;
        let needs_update = current
            .as_ref()
            .map(|state| state.status != DeploymentStatus::Ready || state.healthcheck_failures != 0)
            .unwrap_or(true);
        if needs_update {
            let restart_attempts = current
                .as_ref()
                .map(|state| state.restart_attempts)
                .unwrap_or(0);
            self.store
                .upsert_replica_state(
                    service_id,
                    deployment_id,
                    ReplicaState {
                        service_id: current.as_ref().and_then(|state| state.service_id.clone()),
                        deployment_id: current
                            .as_ref()
                            .and_then(|state| state.deployment_id.clone()),
                        replica_index,
                        status: DeploymentStatus::Ready,
                        healthcheck_failures: 0,
                        restart_attempts,
                        node_id: current.as_ref().and_then(|state| state.node_id.clone()),
                        assignment_id: current
                            .as_ref()
                            .and_then(|state| state.assignment_id.clone()),
                        endpoint: current.as_ref().and_then(|state| state.endpoint.clone()),
                        error: None,
                    },
                )
                .await?;
        }
        Ok(())
    }

    async fn report_unhealthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        assignment_id: Option<&str>,
        _reason: &str,
    ) -> Result<()> {
        let current = self
            .current_state(service_id, deployment_id, replica_index, assignment_id)
            .await;
        if let Some(state) = &current
            && state.status == DeploymentStatus::Crashed
        {
            return Ok(());
        }
        let next_failures = current
            .as_ref()
            .map(|state| state.healthcheck_failures.saturating_add(1))
            .unwrap_or(1);

        let next_status = if next_failures >= self.max_failures {
            DeploymentStatus::Crashed
        } else {
            DeploymentStatus::PendingReady
        };
        let restart_attempts = current
            .as_ref()
            .map(|state| state.restart_attempts)
            .unwrap_or(0);

        let next_state = ReplicaState {
            service_id: current.as_ref().and_then(|state| state.service_id.clone()),
            deployment_id: current
                .as_ref()
                .and_then(|state| state.deployment_id.clone()),
            replica_index,
            status: next_status.clone(),
            healthcheck_failures: next_failures,
            restart_attempts,
            node_id: current.as_ref().and_then(|state| state.node_id.clone()),
            assignment_id: current
                .as_ref()
                .and_then(|state| state.assignment_id.clone()),
            endpoint: current.as_ref().and_then(|state| state.endpoint.clone()),
            error: if next_status == DeploymentStatus::Crashed {
                Some(_reason.to_string())
            } else {
                None
            },
        };
        let needs_update = current
            .as_ref()
            .map(|state| state != &next_state)
            .unwrap_or(true);
        if needs_update {
            self.store
                .upsert_replica_state(service_id, deployment_id, next_state)
                .await?;
        }
        Ok(())
    }
}
