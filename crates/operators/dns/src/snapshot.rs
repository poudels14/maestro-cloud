use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, DnsRecord, DnsRecordId,
    DnsRecordSpec, DnsRecordStatus, Object, ReplicaState, ReplicaStateId, ReplicaStateSpec,
    ReplicaStateStatus, ResourceKind, ResourceName, Service, ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{DnsError, DnsInput, DnsSettings};

pub(crate) struct ResourceSnapshot {
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) replicas: BTreeMap<ReplicaStateId, StoredResource<ReplicaState>>,
    pub(crate) records: BTreeMap<DnsRecordId, StoredResource<DnsRecord>>,
}

impl ResourceSnapshot {
    pub(crate) async fn load(store: &FencedStore, keyspace: &Keyspace) -> Result<Self, DnsError> {
        let values = store.list(&keyspace.resources()).await?.values;
        Ok(Self {
            services: decode_kind::<ServiceId, ServiceSpec, ServiceStatus>(
                &values, keyspace, "Service",
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
            records: decode_kind::<DnsRecordId, DnsRecordSpec, DnsRecordStatus>(
                &values,
                keyspace,
                "DnsRecord",
            )?,
        })
    }

    pub(crate) fn input(
        &self,
        cluster_id: kernel_api::ClusterId,
        settings: DnsSettings,
    ) -> DnsInput {
        DnsInput {
            cluster_id,
            settings,
            services: resources(&self.services),
            assignments: resources(&self.assignments),
            replicas: resources(&self.replicas),
            records: resources(&self.records),
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
            .chain(self.assignments.values().map(|resource| &resource.stored))
            .chain(self.replicas.values().map(|resource| &resource.stored))
            .chain(self.records.values().map(|resource| &resource.stored))
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
) -> Result<StoredObjects<Id, Spec, Status>, DnsError>
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
            .map_err(|error| DnsError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(DnsError::ResourceIdentityMismatch {
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
            return Err(DnsError::DuplicateResource {
                kind: kind_name,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(resources)
}
