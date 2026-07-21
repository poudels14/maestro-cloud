use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Build, BuildId, BuildSpec,
    BuildStatus, Deployment, DeploymentId, DeploymentSpec, DeploymentStatus, Object, ReplicaState,
    ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName, Service,
    ServiceId, ServiceSpec, ServiceStatus, TrafficGeneration, TrafficGenerationId,
    TrafficGenerationSpec, TrafficGenerationStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{DeploymentError, DeploymentInput, LifecycleSettings};

pub(crate) struct ResourceSnapshot {
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) deployments: BTreeMap<DeploymentId, StoredResource<Deployment>>,
    pub(crate) builds: BTreeMap<BuildId, StoredResource<Build>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) replicas: BTreeMap<ReplicaStateId, StoredResource<ReplicaState>>,
    pub(crate) traffic: BTreeMap<TrafficGenerationId, StoredResource<TrafficGeneration>>,
}

impl ResourceSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, DeploymentError> {
        let values = store.list(&keyspace.resources()).await?.values;
        Ok(Self {
            services: decode_kind::<ServiceId, ServiceSpec, ServiceStatus>(
                &values, keyspace, "Service",
            )?,
            deployments: decode_kind::<DeploymentId, DeploymentSpec, DeploymentStatus>(
                &values,
                keyspace,
                "Deployment",
            )?,
            builds: decode_kind::<BuildId, BuildSpec, BuildStatus>(&values, keyspace, "Build")?,
            assignments: decode_kind::<AssignmentId, AssignmentSpec, AssignmentStatus>(
                &values,
                keyspace,
                "Assignment",
            )?,
            replicas: decode_kind::<ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus>(
                &values,
                keyspace,
                "ReplicaState",
            )?,
            traffic: decode_kind::<
                TrafficGenerationId,
                TrafficGenerationSpec,
                TrafficGenerationStatus,
            >(&values, keyspace, "TrafficGeneration")?,
        })
    }

    pub(crate) fn input(
        &self,
        cluster_id: kernel_api::ClusterId,
        now: kernel_api::Timestamp,
        settings: LifecycleSettings,
    ) -> DeploymentInput {
        DeploymentInput {
            cluster_id,
            now,
            settings,
            services: resources(&self.services),
            deployments: resources(&self.deployments),
            builds: resources(&self.builds),
            assignments: resources(&self.assignments),
            replicas: resources(&self.replicas),
            traffic_generations: resources(&self.traffic),
        }
    }

    pub(crate) fn dependency_compares(&self) -> Vec<Compare> {
        self.values()
            .map(|stored| Compare {
                key: stored.key.clone(),
                expected: ExpectedVersion::Exact(stored.version),
            })
            .collect()
    }

    fn values(&self) -> impl Iterator<Item = &StoredValue> {
        self.services
            .values()
            .map(|resource| &resource.stored)
            .chain(self.deployments.values().map(|resource| &resource.stored))
            .chain(self.builds.values().map(|resource| &resource.stored))
            .chain(self.assignments.values().map(|resource| &resource.stored))
            .chain(self.replicas.values().map(|resource| &resource.stored))
            .chain(self.traffic.values().map(|resource| &resource.stored))
    }
}

pub(crate) struct StoredResource<Resource> {
    pub(crate) resource: Resource,
    pub(crate) stored: StoredValue,
}

type StoredObjects<Id, Spec, Status> = BTreeMap<Id, StoredResource<Object<Id, Spec, Status>>>;

fn resources<Id, Resource>(indexed: &BTreeMap<Id, StoredResource<Resource>>) -> Vec<Resource>
where
    Resource: Clone,
{
    indexed
        .values()
        .map(|stored| stored.resource.clone())
        .collect()
}

fn decode_kind<Id, Spec, Status>(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind_name: &'static str,
) -> Result<StoredObjects<Id, Spec, Status>, DeploymentError>
where
    Id: Clone + Ord + Display + Into<ResourceName> + DeserializeOwned,
    Spec: DeserializeOwned,
    Status: DeserializeOwned,
{
    let kind = ResourceKind::new(kind_name)?;
    let prefix = keyspace.resource_kind(&kind);
    let mut resources = BTreeMap::new();
    for stored in values
        .iter()
        .filter(|stored| stored.key.as_str().starts_with(prefix.as_str()))
    {
        let mut resource: Object<Id, Spec, Status> = serde_json::from_slice(&stored.value)
            .map_err(|error| DeploymentError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(DeploymentError::ResourceIdentityMismatch {
                kind: kind_name,
                resource_id: resource.meta.id.to_string(),
                key: stored.key.to_string(),
            });
        }
        resource.meta.revision = stored.version.resource_revision();
        let resource_id = resource.meta.id.clone();
        if resources
            .insert(
                resource_id.clone(),
                StoredResource {
                    resource,
                    stored: stored.clone(),
                },
            )
            .is_some()
        {
            return Err(DeploymentError::DuplicateResource {
                kind: kind_name,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(resources)
}
