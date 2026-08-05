use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use kernel_api::{
    Assignment, AssignmentId, Deployment, DeploymentId, DeploymentPhase, ReplicaState, WorkloadId,
};
use kernel_store::WatchCursor;
use runtime::NetworkHandle;

use super::AssignmentAgent;
use crate::assignment_error::AssignmentAgentError;
#[cfg(unix)]
use crate::assignment_node_api::active_node_api_workloads;
use crate::assignment_replica::ensure_replica;
use crate::assignment_replica::record_resolved_secrets;
use crate::assignment_resource::{decode_assignments, decode_deployments, decode_replicas};
use crate::assignment_status::{AssignmentOutcome, ConvergeFailure};
use crate::assignment_types::{
    AssignmentReconcileReport, ConvergedAssignment, WorkloadDns, earliest,
};

struct AssignmentResources<'a> {
    deployments: &'a BTreeMap<DeploymentId, Deployment>,
    replicas: &'a BTreeMap<AssignmentId, ReplicaState>,
    malformed_replicas: usize,
    network: &'a NetworkHandle,
}

impl AssignmentAgent {
    pub(super) async fn reconcile_with_cursor(
        &self,
    ) -> Result<(AssignmentReconcileReport, WatchCursor), AssignmentAgentError> {
        let assignment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.assignment_kind))
            .await?;
        let deployment_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.deployment_kind))
            .await?;
        let replica_snapshot = self
            .store
            .list(&self.keyspace.resource_kind(&self.replica_kind))
            .await?;
        let (assignments, malformed_assignments) = decode_assignments(
            &assignment_snapshot.values,
            &self.keyspace,
            &self.assignment_kind,
            &self.settings.node_id,
        );
        let (deployments, malformed_deployments) = decode_deployments(
            &deployment_snapshot.values,
            &self.keyspace,
            &self.deployment_kind,
            &self.settings.node_id,
        );
        let (replicas, malformed_replicas) = decode_replicas(
            &replica_snapshot.values,
            &self.keyspace,
            &self.replica_kind,
            &self.settings.node_id,
        );
        let local = assignments
            .into_iter()
            .filter(|assignment| assignment.spec.node_id == self.settings.node_id)
            .collect::<Vec<_>>();
        let active = local
            .iter()
            .filter(|assignment| assignment.meta.deletion_timestamp.is_none())
            .collect::<Vec<_>>();
        let active_deployments = active
            .iter()
            .map(|assignment| assignment.spec.deployment_id.clone())
            .collect::<BTreeSet<_>>();
        self.resolved_deployments
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .retain(|deployment_id, _| active_deployments.contains(deployment_id));
        let network = self.network.ensure_network(&self.settings.network).await?;
        let mut report = AssignmentReconcileReport {
            desired: active.len(),
            malformed_resources: malformed_assignments
                .saturating_add(malformed_deployments)
                .saturating_add(malformed_replicas),
            ..Default::default()
        };
        let resources = AssignmentResources {
            deployments: &deployments,
            replicas: &replicas,
            malformed_replicas,
            network: &network,
        };

        if malformed_assignments == 0 {
            let active_ids = active
                .iter()
                .map(|assignment| assignment.meta.id.clone())
                .collect::<BTreeSet<_>>();
            let observed = self
                .runtime
                .list(&self.settings.cluster_id, &self.settings.node_id)
                .await?;
            for workload in observed {
                if !active_ids.contains(&workload.metadata.assignment_id) {
                    self.remove_workload(&workload.handle, &network).await?;
                    report.garbage_collected = report.garbage_collected.saturating_add(1);
                }
            }
            let active_workload_ids = active
                .iter()
                .map(|assignment| WorkloadId::new(assignment.meta.id.as_str()))
                .collect::<Result<BTreeSet<_>, _>>()?;
            report.user_namespaces_collected = self
                .runtime
                .cleanup_user_namespaces(&active_workload_ids)
                .await?;
            report.address_reservations_collected = self
                .network
                .reconcile_address_owners(&network, &active_workload_ids)
                .await?;
            let active_workloads = active
                .iter()
                .map(|assignment| assignment.meta.id.to_string())
                .collect::<BTreeSet<_>>();
            report.secret_mounts_collected = self.secrets.cleanup_stale(&active_workloads).await?;
            #[cfg(unix)]
            if malformed_deployments == 0 {
                let active_node_api_workloads = active_node_api_workloads(&active, &deployments);
                report.node_api_mounts_collected = self
                    .node_api
                    .cleanup_stale(&active_node_api_workloads)
                    .await?;
            }
        }
        self.reconcile_active(&active, &resources, &mut report)
            .await?;
        for assignment in local
            .iter()
            .filter(|assignment| assignment.meta.deletion_timestamp.is_some())
        {
            self.update_status(assignment, AssignmentOutcome::Stopped)
                .await?;
        }
        Ok((
            report,
            std::cmp::min(
                std::cmp::min(assignment_snapshot.cursor, deployment_snapshot.cursor),
                replica_snapshot.cursor,
            ),
        ))
    }

    async fn reconcile_active(
        &self,
        active: &[&Assignment],
        resources: &AssignmentResources<'_>,
        report: &mut AssignmentReconcileReport,
    ) -> Result<(), AssignmentAgentError> {
        let resolver_service_id = match &self.settings.dns {
            WorkloadDns::DelegatedService(service_id) => service_id,
            WorkloadDns::Static(address) => {
                for assignment in active {
                    self.reconcile_resource(assignment, resources, Some(*address), report)
                        .await?;
                }
                return Ok(());
            }
            WorkloadDns::Disabled => {
                for assignment in active {
                    self.reconcile_resource(assignment, resources, None, report)
                        .await?;
                }
                return Ok(());
            }
        };

        let mut dns_server = None;
        for assignment in active
            .iter()
            .filter(|assignment| &assignment.spec.service_id == resolver_service_id)
        {
            let address = self
                .reconcile_resource(assignment, resources, None, report)
                .await?;
            if dns_server.is_none()
                && resources
                    .deployments
                    .get(&assignment.spec.deployment_id)
                    .is_some_and(|deployment| deployment.status.phase == DeploymentPhase::Ready)
            {
                dns_server = address;
            }
        }
        for assignment in active
            .iter()
            .filter(|assignment| &assignment.spec.service_id != resolver_service_id)
        {
            match dns_server {
                Some(dns_server) => {
                    self.reconcile_resource(assignment, resources, Some(dns_server), report)
                        .await?;
                }
                None => {
                    self.record_outcome(
                        assignment,
                        Err(ConvergeFailure::pending(
                            "DnsResolverUnavailable",
                            format!(
                                "delegated DNS service `{resolver_service_id}` has no ready local assignment"
                            ),
                        )),
                        report,
                    )
                    .await?;
                }
            }
        }
        Ok(())
    }

    async fn reconcile_resource(
        &self,
        assignment: &Assignment,
        resources: &AssignmentResources<'_>,
        dns_server: Option<IpAddr>,
        report: &mut AssignmentReconcileReport,
    ) -> Result<Option<IpAddr>, AssignmentAgentError> {
        let outcome = match resources.deployments.get(&assignment.spec.deployment_id) {
            Some(deployment) => {
                let replica = match resources.replicas.get(&assignment.meta.id) {
                    Some(replica) => Some(replica.clone()),
                    None if resources.malformed_replicas == 0 => {
                        let ensured = ensure_replica(
                            self.store.as_ref(),
                            &self.keyspace,
                            &self.assignment_kind,
                            &self.replica_kind,
                            assignment,
                        )
                        .await?;
                        if ensured.created {
                            report.replica_states_created =
                                report.replica_states_created.saturating_add(1);
                        }
                        Some(ensured.replica)
                    }
                    None => None,
                };
                self.converge_assignment(
                    assignment,
                    deployment,
                    replica.as_ref(),
                    resources.network,
                    dns_server,
                )
                .await
            }
            None => Err(ConvergeFailure::pending(
                "DeploymentMissing",
                format!(
                    "deployment `{}` is not present in the observed snapshot",
                    assignment.spec.deployment_id
                ),
            )),
        };
        self.record_outcome(assignment, outcome, report).await
    }

    async fn record_outcome(
        &self,
        assignment: &Assignment,
        outcome: Result<ConvergedAssignment, ConvergeFailure>,
        report: &mut AssignmentReconcileReport,
    ) -> Result<Option<IpAddr>, AssignmentAgentError> {
        match outcome {
            Ok(converged) => {
                if let Some(replica_id) = converged.replica_id.as_ref() {
                    record_resolved_secrets(
                        self.store.as_ref(),
                        &self.keyspace,
                        &self.replica_kind,
                        assignment,
                        replica_id,
                        &converged.resolved_secrets,
                    )
                    .await?;
                }
                self.update_status(
                    assignment,
                    AssignmentOutcome::Running {
                        handle: &converged.handle,
                        workload_address: converged.workload_address,
                    },
                )
                .await?;
                report.running = report.running.saturating_add(1);
                if converged.restarted {
                    report.restarted = report.restarted.saturating_add(1);
                }
                Ok(Some(converged.workload_address))
            }
            Err(failure) => {
                report.requeue_at = earliest(report.requeue_at, failure.retry_at());
                self.update_status(assignment, AssignmentOutcome::Unresolved(&failure))
                    .await?;
                report.unresolved = report.unresolved.saturating_add(1);
                Ok(None)
            }
        }
    }
}
