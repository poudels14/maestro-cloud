use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Deployment, DeploymentId,
    DeploymentSpec, DeploymentStatus, Node, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, NodeSpec, NodeStatus, Object, ReplicaState, ReplicaStateId,
    ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName, Service, ServiceId,
    ServiceSpec, ServiceStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::SchedulerError;

pub(crate) struct ResourceSnapshot {
    pub(crate) services: BTreeMap<ServiceId, Service>,
    pub(crate) deployments: BTreeMap<DeploymentId, Deployment>,
    pub(crate) nodes: BTreeMap<NodeId, Node>,
    pub(crate) networks: BTreeMap<NodeNetworkId, NodeNetwork>,
    pub(crate) assignments: BTreeMap<AssignmentId, Assignment>,
    pub(crate) replicas: BTreeMap<ReplicaStateId, ReplicaState>,
    pub(crate) assignment_values: Vec<StoredValue>,
    pub(crate) dependency_compares: Vec<Compare>,
}

impl ResourceSnapshot {
    pub(crate) async fn load(
        fenced_store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, SchedulerError> {
        let snapshot = fenced_store.list(&keyspace.resources()).await?;
        let services = decode_kind::<ServiceId, ServiceSpec, ServiceStatus>(
            &snapshot.values,
            keyspace,
            "Service",
        )?;
        let deployments = decode_kind::<DeploymentId, DeploymentSpec, DeploymentStatus>(
            &snapshot.values,
            keyspace,
            "Deployment",
        )?;
        let nodes =
            decode_kind::<NodeId, NodeSpec, NodeStatus>(&snapshot.values, keyspace, "Node")?;
        let networks = decode_kind::<NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus>(
            &snapshot.values,
            keyspace,
            "NodeNetwork",
        )?;
        let assignments = decode_kind::<AssignmentId, AssignmentSpec, AssignmentStatus>(
            &snapshot.values,
            keyspace,
            "Assignment",
        )?;
        let replicas = decode_kind::<ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus>(
            &snapshot.values,
            keyspace,
            "ReplicaState",
        )?;

        let dependency_compares = services
            .values
            .iter()
            .chain(deployments.values.iter())
            .chain(nodes.values.iter())
            .chain(networks.values.iter())
            .chain(replicas.values.iter())
            .map(|stored| Compare {
                key: stored.key.clone(),
                expected: ExpectedVersion::Exact(stored.version),
            })
            .collect();
        Ok(Self {
            services: services.resources,
            deployments: deployments.resources,
            nodes: nodes.resources,
            networks: networks.resources,
            assignments: assignments.resources,
            replicas: replicas.resources,
            assignment_values: assignments.values,
            dependency_compares,
        })
    }
}

struct DecodedKind<Id, Spec, Status> {
    resources: BTreeMap<Id, Object<Id, Spec, Status>>,
    values: Vec<StoredValue>,
}

fn decode_kind<Id, Spec, Status>(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind_name: &'static str,
) -> Result<DecodedKind<Id, Spec, Status>, SchedulerError>
where
    Id: Clone + Ord + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let kind = ResourceKind::new(kind_name)?;
    let prefix = keyspace.resource_kind(&kind);
    let mut resources = BTreeMap::new();
    let mut matched = Vec::new();
    for stored in values
        .iter()
        .filter(|stored| stored.key.as_str().starts_with(prefix.as_str()))
    {
        let mut resource: Object<Id, Spec, Status> = serde_json::from_slice(&stored.value)
            .map_err(|error| SchedulerError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(SchedulerError::ResourceIdentityMismatch {
                kind: kind_name,
                resource_id: resource.meta.id.to_string(),
                key: stored.key.to_string(),
            });
        }
        resource.meta.revision = stored.version.resource_revision();
        let resource_id = resource.meta.id.clone();
        if resources.insert(resource_id.clone(), resource).is_some() {
            return Err(SchedulerError::DuplicateResource {
                kind: kind_name,
                resource_id: resource_id.to_string(),
            });
        }
        matched.push(stored.clone());
    }
    Ok(DecodedKind {
        resources,
        values: matched,
    })
}
