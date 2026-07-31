use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Build, BuildId, BuildSpec,
    BuildStatus, Deployment, DeploymentId, DeploymentSpec, DeploymentStatus, IngressRoute,
    IngressRouteId, IngressRouteSpec, IngressRouteStatus, Object, ReplicaState, ReplicaStateId,
    ReplicaStateSpec, ReplicaStateStatus, ResourceKind, ResourceName, Service, ServiceId,
    ServiceSpec, ServiceStatus, TrafficGeneration, TrafficGenerationId, TrafficGenerationSpec,
    TrafficGenerationStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{DeploymentError, DeploymentInput, LifecycleSettings};

pub(crate) struct ResourceSnapshot {
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) ingress_routes: BTreeMap<IngressRouteId, StoredResource<IngressRoute>>,
    pub(crate) deployments: BTreeMap<DeploymentId, StoredResource<Deployment>>,
    pub(crate) builds: BTreeMap<BuildId, StoredResource<Build>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) replicas: BTreeMap<ReplicaStateId, StoredResource<ReplicaState>>,
    pub(crate) traffic: BTreeMap<TrafficGenerationId, StoredResource<TrafficGeneration>>,
}

impl ResourceSnapshot {
    async fn load(store: &FencedStore, keyspace: &Keyspace) -> Result<Self, DeploymentError> {
        let values = store.list(&keyspace.resources()).await?.values;
        Ok(Self {
            services: decode_kind::<ServiceId, ServiceSpec, ServiceStatus>(
                &values, keyspace, "Service",
            )?,
            ingress_routes: decode_kind::<IngressRouteId, IngressRouteSpec, IngressRouteStatus>(
                &values,
                keyspace,
                "IngressRoute",
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

    pub(crate) async fn load_service(
        store: &FencedStore,
        keyspace: &Keyspace,
        service_id: &ServiceId,
    ) -> Result<Self, DeploymentError> {
        let mut snapshot = Self::load(store, keyspace).await?;
        snapshot.services.retain(|id, _| id == service_id);
        snapshot
            .ingress_routes
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        snapshot
            .deployments
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        snapshot
            .builds
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        snapshot
            .assignments
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        snapshot
            .replicas
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        snapshot
            .traffic
            .retain(|_, resource| resource.resource.spec.service_id == *service_id);
        Ok(snapshot)
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
            ingress_routes: resources(&self.ingress_routes),
            deployments: resources(&self.deployments),
            builds: resources(&self.builds),
            assignments: resources(&self.assignments),
            replicas: resources(&self.replicas),
            traffic_generations: resources(&self.traffic),
        }
    }

    pub(crate) fn primary_compares(&self) -> Vec<Compare> {
        self.services
            .values()
            .map(|resource| &resource.stored)
            .chain(
                self.ingress_routes
                    .values()
                    .map(|resource| &resource.stored),
            )
            .map(|stored| Compare {
                key: stored.key.clone(),
                expected: ExpectedVersion::Exact(stored.version),
            })
            .collect()
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
