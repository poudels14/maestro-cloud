use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{Assignment, Deployment, DeploymentId, NodeApiAccess, WorkloadId};
use node_fabric::WorkloadClaims;
use runtime::{WorkloadMount, WorkloadUserNamespace};

use crate::assignment_plan::{node_api_user, workload_labels};
use crate::assignment_status::ConvergeFailure;
use crate::node_api::{NodeApiSocketOwner, WorkloadControlAccess};
use crate::node_api_mount::NodeApiMountManager;

pub(crate) async fn mount_node_api(
    manager: &NodeApiMountManager,
    assignment: &Assignment,
    deployment: &Deployment,
    workload_id: &WorkloadId,
    user_namespace: Option<WorkloadUserNamespace>,
) -> Result<Option<WorkloadMount>, ConvergeFailure> {
    let Some(user) = node_api_user(deployment)? else {
        return Ok(None);
    };
    let mount = manager
        .ensure(
            workload_id,
            NodeApiSocketOwner {
                user_id: user.user_id,
                group_id: user.group_id,
                peer_user_id: user_namespace
                    .map(|namespace| namespace.host_user(user))
                    .transpose()?
                    .map_or(user.user_id, |host_user| host_user.user_id),
            },
            WorkloadClaims {
                workload_id: workload_id.clone(),
                assignment_id: assignment.meta.id.clone(),
                node_id: assignment.spec.node_id.clone(),
                service_id: assignment.spec.service_id.clone(),
                deployment_id: assignment.spec.deployment_id.clone(),
                labels: workload_labels(assignment, deployment),
            },
            match deployment.spec.service.node_api {
                NodeApiAccess::Privileged => WorkloadControlAccess::Allowed,
                NodeApiAccess::Disabled | NodeApiAccess::IdentityAndTelemetry => {
                    WorkloadControlAccess::Denied
                }
            },
        )
        .await?;
    Ok(Some(mount))
}

pub(crate) fn active_node_api_workloads(
    assignments: &[&Assignment],
    deployments: &BTreeMap<DeploymentId, Deployment>,
) -> BTreeSet<String> {
    assignments
        .iter()
        .filter(|assignment| {
            deployments
                .get(&assignment.spec.deployment_id)
                .is_some_and(|deployment| deployment.spec.service.node_api.is_enabled())
        })
        .map(|assignment| assignment.meta.id.to_string())
        .collect()
}
