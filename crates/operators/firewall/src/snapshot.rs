use std::collections::BTreeMap;
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, FirewallPolicy, FirewallPolicyId,
    FirewallPolicySpec, FirewallPolicyStatus, NodeFirewall, NodeFirewallId, NodeFirewallSpec,
    NodeFirewallStatus, NodeNetwork, NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus, Object,
    ResourceKind, ResourceName, Service, ServiceId, ServiceSpec, ServiceStatus,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{FirewallError, FirewallInput, FirewallSettings};

pub(crate) struct ResourceSnapshot {
    pub(crate) policies: BTreeMap<FirewallPolicyId, StoredResource<FirewallPolicy>>,
    pub(crate) services: BTreeMap<ServiceId, StoredResource<Service>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) node_networks: BTreeMap<NodeNetworkId, StoredResource<NodeNetwork>>,
    pub(crate) node_firewalls: BTreeMap<NodeFirewallId, StoredResource<NodeFirewall>>,
}

impl ResourceSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, FirewallError> {
        let values = store.list(&keyspace.resources()).await?.values;
        Ok(Self {
            policies: decode_kind::<FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus>(
                &values,
                keyspace,
                "FirewallPolicy",
            )?,
            services: decode_kind::<ServiceId, ServiceSpec, ServiceStatus>(
                &values, keyspace, "Service",
            )?,
            assignments: decode_kind::<AssignmentId, AssignmentSpec, AssignmentStatus>(
                &values,
                keyspace,
                "Assignment",
            )?,
            node_networks: decode_kind::<NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus>(
                &values,
                keyspace,
                "NodeNetwork",
            )?,
            node_firewalls: decode_kind::<NodeFirewallId, NodeFirewallSpec, NodeFirewallStatus>(
                &values,
                keyspace,
                "NodeFirewall",
            )?,
        })
    }

    pub(crate) fn input(&self, settings: FirewallSettings) -> FirewallInput {
        FirewallInput {
            settings,
            policies: resources(&self.policies),
            services: resources(&self.services),
            assignments: resources(&self.assignments),
            node_networks: resources(&self.node_networks),
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
        self.policies
            .values()
            .map(|resource| &resource.stored)
            .chain(self.services.values().map(|resource| &resource.stored))
            .chain(self.assignments.values().map(|resource| &resource.stored))
            .chain(self.node_networks.values().map(|resource| &resource.stored))
            .chain(
                self.node_firewalls
                    .values()
                    .map(|resource| &resource.stored),
            )
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
) -> Result<StoredObjects<Id, Spec, Status>, FirewallError>
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
            .map_err(|error| FirewallError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(FirewallError::ResourceIdentityMismatch {
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
            return Err(FirewallError::DuplicateResource {
                kind: kind_name,
                resource_id: resource_id.to_string(),
            });
        }
    }
    Ok(resources)
}
