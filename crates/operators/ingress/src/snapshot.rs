use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Deployment, DeploymentId,
    DeploymentSpec, DeploymentStatus, IngressBlocklist, IngressBlocklistId, IngressBlocklistSpec,
    IngressBlocklistStatus, IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus,
    Object, ReplicaState, ReplicaStateId, ReplicaStateSpec, ReplicaStateStatus, ResourceKind,
    ResourceName, Service, ServiceId, ServiceSpec, ServiceStatus, TrafficGeneration,
    TrafficGenerationId, TrafficGenerationSpec, TrafficGenerationStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{IngressError, IngressInput, IngressSettings};

pub(crate) struct ResourceSnapshot {
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) deployments: BTreeMap<DeploymentId, StoredResource<Deployment>>,
    pub(crate) routes: BTreeMap<IngressRouteId, StoredResource<IngressRoute>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) replicas: BTreeMap<ReplicaStateId, StoredResource<ReplicaState>>,
    pub(crate) generations: BTreeMap<TrafficGenerationId, StoredResource<TrafficGeneration>>,
    pub(crate) blocklists: BTreeMap<IngressBlocklistId, StoredResource<IngressBlocklist>>,
}

impl ResourceSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, IngressError> {
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
            routes: decode_kind::<IngressRouteId, IngressRouteSpec, IngressRouteStatus>(
                &values,
                keyspace,
                "IngressRoute",
            )?,
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
            generations: decode_kind::<
                TrafficGenerationId,
                TrafficGenerationSpec,
                TrafficGenerationStatus,
            >(&values, keyspace, "TrafficGeneration")?,
            blocklists: decode_kind::<
                IngressBlocklistId,
                IngressBlocklistSpec,
                IngressBlocklistStatus,
            >(&values, keyspace, "IngressBlocklist")?,
        })
    }

    pub(crate) fn input(
        &self,
        cluster_id: kernel_api::ClusterId,
        now: kernel_api::Timestamp,
        settings: IngressSettings,
    ) -> IngressInput {
        IngressInput {
            cluster_id,
            now,
            settings,
            services: resources(&self.services),
            deployments: resources(&self.deployments),
            routes: resources(&self.routes),
            assignments: resources(&self.assignments),
            replicas: resources(&self.replicas),
            traffic_generations: resources(&self.generations),
            blocklists: resources(&self.blocklists),
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
            .chain(self.routes.values().map(|resource| &resource.stored))
            .chain(self.assignments.values().map(|resource| &resource.stored))
            .chain(self.replicas.values().map(|resource| &resource.stored))
            .chain(self.generations.values().map(|resource| &resource.stored))
            .chain(self.blocklists.values().map(|resource| &resource.stored))
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
) -> Result<StoredObjects<Id, Spec, Status>, IngressError>
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
            .map_err(|error| IngressError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(IngressError::ResourceIdentityMismatch {
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
            return Err(IngressError::DuplicateResource {
                kind: kind_name,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(resources)
}
