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
    ) -> Result<()>;

    async fn report_unhealthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
        reason: &str,
    ) -> Result<()>;
}

pub struct DefaultHealthMonitor {
    store: Arc<dyn ClusterStore>,
    max_failures: u32,
}

impl DefaultHealthMonitor {
    pub fn new(store: Arc<dyn ClusterStore>, max_failures: u32) -> Self {
        Self {
            store,
            max_failures,
        }
    }

    async fn current_state(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
    ) -> Option<ReplicaState> {
        self.store
            .list_replica_states(service_id, deployment_id)
            .await
            .ok()?
            .into_iter()
            .find(|state| state.replica_index == replica_index)
    }
}

#[async_trait]
impl ReplicaHealthMonitor for DefaultHealthMonitor {
    async fn report_healthy(
        &self,
        service_id: &str,
        deployment_id: &str,
        replica_index: u32,
    ) -> Result<()> {
        let current = self
            .current_state(service_id, deployment_id, replica_index)
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
                        replica_index,
                        status: DeploymentStatus::Ready,
                        healthcheck_failures: 0,
                        restart_attempts,
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
        _reason: &str,
    ) -> Result<()> {
        let current = self
            .current_state(service_id, deployment_id, replica_index)
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
            replica_index,
            status: next_status,
            healthcheck_failures: next_failures,
            restart_attempts,
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
