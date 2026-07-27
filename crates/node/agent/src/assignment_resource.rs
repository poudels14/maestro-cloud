use std::collections::BTreeMap;

use kernel_api::{
    Assignment, AssignmentId, Deployment, DeploymentId, NodeId, ReplicaState, ResourceKind,
    ResourceName,
};
use kernel_store::{Keyspace, StoredValue};

use crate::assignment_error::AssignmentAgentError;

pub(crate) fn decode_assignments(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind: &ResourceKind,
    node_id: &NodeId,
) -> (Vec<Assignment>, usize) {
    decode_resources(
        values,
        keyspace,
        kind,
        node_id,
        |assignment: &Assignment| assignment.meta.id.as_str(),
    )
}

pub(crate) fn decode_deployments(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind: &ResourceKind,
    node_id: &NodeId,
) -> (BTreeMap<DeploymentId, Deployment>, usize) {
    let (decoded, malformed) = decode_resources(
        values,
        keyspace,
        kind,
        node_id,
        |deployment: &Deployment| deployment.meta.id.as_str(),
    );
    (
        decoded
            .into_iter()
            .map(|deployment| (deployment.meta.id.clone(), deployment))
            .collect(),
        malformed,
    )
}

pub(crate) fn decode_replicas(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind: &ResourceKind,
    node_id: &NodeId,
) -> (BTreeMap<AssignmentId, ReplicaState>, usize) {
    let (decoded, malformed) =
        decode_resources(values, keyspace, kind, node_id, |replica: &ReplicaState| {
            replica.meta.id.as_str()
        });
    (
        decoded
            .into_iter()
            .map(|replica| (replica.spec.assignment_id.clone(), replica))
            .collect(),
        malformed,
    )
}

fn decode_resources<Resource>(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind: &ResourceKind,
    node_id: &NodeId,
    resource_id: fn(&Resource) -> &str,
) -> (Vec<Resource>, usize)
where
    Resource: for<'de> serde::Deserialize<'de>,
{
    let mut decoded = Vec::new();
    let mut malformed = 0_usize;
    for stored in values {
        let resource = match serde_json::from_slice::<Resource>(&stored.value) {
            Ok(resource) => resource,
            Err(error) => {
                malformed = malformed.saturating_add(1);
                warn_malformed(kind, node_id, stored, &error);
                continue;
            }
        };
        let id = resource_id(&resource);
        let expected_key = match ResourceName::new(id) {
            Ok(name) => keyspace.resource(kind, &name),
            Err(error) => {
                malformed = malformed.saturating_add(1);
                warn_malformed(kind, node_id, stored, &error);
                continue;
            }
        };
        if stored.key != expected_key {
            malformed = malformed.saturating_add(1);
            tracing::warn!(
                kind = %kind,
                node_id = %node_id,
                resource_key = %stored.key,
                resource_id = id,
                expected_key = %expected_key,
                "misidentified assignment input resource was skipped"
            );
            continue;
        }
        decoded.push(resource);
    }
    (decoded, malformed)
}

fn warn_malformed(
    kind: &ResourceKind,
    node_id: &NodeId,
    stored: &StoredValue,
    error: &dyn std::fmt::Display,
) {
    tracing::warn!(
        kind = %kind,
        node_id = %node_id,
        resource_key = %stored.key,
        error = %error,
        "malformed assignment input resource was skipped"
    );
}

pub(crate) fn decode_assignment(stored: &StoredValue) -> Result<Assignment, AssignmentAgentError> {
    serde_json::from_slice(&stored.value).map_err(|error| {
        AssignmentAgentError::MalformedAssignment {
            key: stored.key.to_string(),
            message: error.to_string(),
        }
    })
}
