use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Deployment, DeploymentId,
    DeploymentSpec, DeploymentStatus, Node, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, NodeSpec, NodeStatus, Object, ResourceKind, ResourceName, Service,
    ServiceId, ServiceSpec, ServiceStatus, TrafficGeneration, TrafficGenerationId,
    TrafficGenerationSpec, TrafficGenerationStatus,
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
    pub(crate) traffic_generations: BTreeMap<TrafficGenerationId, TrafficGeneration>,
    pub(crate) assignment_values: Vec<StoredValue>,
    service_values: BTreeMap<ServiceId, StoredValue>,
    deployment_values: BTreeMap<DeploymentId, StoredValue>,
    node_values: BTreeMap<NodeId, StoredValue>,
    network_values: BTreeMap<NodeNetworkId, StoredValue>,
    traffic_generation_values: BTreeMap<TrafficGenerationId, StoredValue>,
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
        let traffic_generations = decode_kind::<
            TrafficGenerationId,
            TrafficGenerationSpec,
            TrafficGenerationStatus,
        >(&snapshot.values, keyspace, "TrafficGeneration")?;

        let assignment_values = assignments.values.into_values().collect();
        Ok(Self {
            services: services.resources,
            deployments: deployments.resources,
            nodes: nodes.resources,
            networks: networks.resources,
            assignments: assignments.resources,
            traffic_generations: traffic_generations.resources,
            assignment_values,
            service_values: services.values,
            deployment_values: deployments.values,
            node_values: nodes.values,
            network_values: networks.values,
            traffic_generation_values: traffic_generations.values,
        })
    }

    /// Returns only the resource versions that can change one Service's placement.
    ///
    /// Assignment generation serializes scheduler-owned writes cluster-wide. Unrelated Service
    /// and Deployment history therefore must not consume etcd transaction operations for this
    /// Service's assignment update.
    pub(crate) fn dependency_compares(&self, service_id: &ServiceId) -> Vec<Compare> {
        let mut values = Vec::new();
        if let Some(service) = self.service_values.get(service_id) {
            values.push(service);
        }
        values.extend(
            self.deployments
                .iter()
                .filter_map(|(deployment_id, deployment)| {
                    (deployment.spec.service_id == *service_id)
                        .then(|| self.deployment_values.get(deployment_id))
                        .flatten()
                }),
        );
        values.extend(self.node_values.values());
        values.extend(self.network_values.values());
        values.extend(
            self.traffic_generations
                .iter()
                .filter_map(|(generation_id, generation)| {
                    (generation.spec.service_id == *service_id)
                        .then(|| self.traffic_generation_values.get(generation_id))
                        .flatten()
                }),
        );
        values.into_iter().map(exact).collect()
    }
}

struct DecodedKind<Id, Spec, Status> {
    resources: BTreeMap<Id, Object<Id, Spec, Status>>,
    values: BTreeMap<Id, StoredValue>,
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
    let mut matched = BTreeMap::new();
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
        matched.insert(resource_id, stored.clone());
    }
    Ok(DecodedKind {
        resources,
        values: matched,
    })
}

fn exact(stored: &StoredValue) -> Compare {
    Compare {
        key: stored.key.clone(),
        expected: ExpectedVersion::Exact(stored.version),
    }
}
