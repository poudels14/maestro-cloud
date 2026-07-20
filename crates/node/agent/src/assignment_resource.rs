use std::collections::BTreeMap;

use kernel_api::{Assignment, AssignmentId, Deployment, DeploymentId, ReplicaState};
use kernel_store::StoredValue;

use crate::assignment_error::AssignmentAgentError;

pub(crate) fn decode_assignments(values: &[StoredValue]) -> (Vec<Assignment>, usize) {
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<Assignment, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded.into_iter().filter_map(Result::ok).collect(),
        malformed,
    )
}

pub(crate) fn decode_deployments(
    values: &[StoredValue],
) -> (BTreeMap<DeploymentId, Deployment>, usize) {
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<Deployment, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded
            .into_iter()
            .filter_map(Result::ok)
            .map(|deployment| (deployment.meta.id.clone(), deployment))
            .collect(),
        malformed,
    )
}

pub(crate) fn decode_replicas(
    values: &[StoredValue],
) -> (BTreeMap<AssignmentId, ReplicaState>, usize) {
    let decoded = values
        .iter()
        .map(|stored| serde_json::from_slice(&stored.value))
        .collect::<Vec<Result<ReplicaState, _>>>();
    let malformed = decoded.iter().filter(|result| result.is_err()).count();
    (
        decoded
            .into_iter()
            .filter_map(Result::ok)
            .map(|replica| (replica.spec.assignment_id.clone(), replica))
            .collect(),
        malformed,
    )
}

pub(crate) fn decode_assignment(stored: &StoredValue) -> Result<Assignment, AssignmentAgentError> {
    serde_json::from_slice(&stored.value).map_err(|error| {
        AssignmentAgentError::MalformedAssignment {
            key: stored.key.to_string(),
            message: error.to_string(),
        }
    })
}
