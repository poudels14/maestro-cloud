use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Display;

use kernel_api::{
    Assignment, AssignmentId, AssignmentSpec, AssignmentStatus, Node, NodeId, NodeSpec, NodeStatus,
    Object, ResourceKind, ResourceName, Timestamp, UpgradeRun,
};
use kernel_controller::FencedStore;
use kernel_store::{Compare, ExpectedVersion, Keyspace, StoredValue};
use serde::de::DeserializeOwned;

use crate::{UpgradeError, UpgradeInput};

pub(crate) struct UpgradeSnapshot {
    pub(crate) nodes: BTreeMap<NodeId, StoredResource<Node>>,
    pub(crate) assignments: BTreeMap<AssignmentId, StoredResource<Assignment>>,
    pub(crate) live_nodes: BTreeSet<NodeId>,
    pub(crate) liveness_compares: Vec<Compare>,
}

impl UpgradeSnapshot {
    pub(crate) async fn load(
        store: &FencedStore,
        keyspace: &Keyspace,
    ) -> Result<Self, UpgradeError> {
        let listed = store.list(&keyspace.resources()).await?;
        let nodes = decode_kind::<NodeId, NodeSpec, NodeStatus>(&listed.values, keyspace, "Node")?;
        let assignments = decode_kind::<AssignmentId, AssignmentSpec, AssignmentStatus>(
            &listed.values,
            keyspace,
            "Assignment",
        )?;
        let mut live_nodes = BTreeSet::new();
        let mut liveness_compares = Vec::new();
        for node_id in nodes.keys() {
            let key = keyspace.node_liveness(node_id);
            match store.get(&key).await? {
                Some(stored) => {
                    live_nodes.insert(node_id.clone());
                    liveness_compares.push(Compare {
                        key,
                        expected: ExpectedVersion::Exact(stored.version),
                    });
                }
                None => liveness_compares.push(Compare {
                    key,
                    expected: ExpectedVersion::Missing,
                }),
            }
        }
        Ok(Self {
            nodes,
            assignments,
            live_nodes,
            liveness_compares,
        })
    }

    pub(crate) fn input(&self, run: UpgradeRun, leader_id: NodeId, now: Timestamp) -> UpgradeInput {
        UpgradeInput {
            run,
            nodes: self
                .nodes
                .values()
                .map(|stored| stored.resource.clone())
                .collect(),
            assignments: self
                .assignments
                .values()
                .map(|stored| stored.resource.clone())
                .collect(),
            live_nodes: self.live_nodes.clone(),
            leader_id,
            now,
        }
    }

    pub(crate) fn dependency_compares(&self) -> Vec<Compare> {
        let mut compares = self
            .nodes
            .values()
            .map(|stored| Compare {
                key: stored.stored.key.clone(),
                expected: ExpectedVersion::Exact(stored.stored.version),
            })
            .collect::<Vec<_>>();
        compares.extend(self.assignments.values().map(|stored| Compare {
            key: stored.stored.key.clone(),
            expected: ExpectedVersion::Exact(stored.stored.version),
        }));
        compares.extend(self.liveness_compares.clone());
        compares
    }
}

pub(crate) struct StoredResource<Resource> {
    pub(crate) resource: Resource,
    pub(crate) stored: StoredValue,
}

type ResourceMap<Id, Spec, Status> = BTreeMap<Id, StoredResource<Object<Id, Spec, Status>>>;

fn decode_kind<Id, Spec, Status>(
    values: &[StoredValue],
    keyspace: &Keyspace,
    kind_name: &'static str,
) -> Result<ResourceMap<Id, Spec, Status>, UpgradeError>
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
            .map_err(|error| UpgradeError::MalformedResource {
                kind: kind_name,
                key: stored.key.to_string(),
                message: error.to_string(),
            })?;
        let expected_key = keyspace.resource(&kind, &resource.meta.id.clone().into());
        if stored.key != expected_key {
            return Err(UpgradeError::ResourceIdentityMismatch {
                kind: kind_name,
                resource_id: resource.meta.id.to_string(),
                key: stored.key.to_string(),
            });
        }
        resource.meta.revision = stored.version.resource_revision();
        let id = resource.meta.id.clone();
        if resources
            .insert(
                id.clone(),
                StoredResource {
                    resource,
                    stored: stored.clone(),
                },
            )
            .is_some()
        {
            return Err(UpgradeError::DuplicateResource {
                kind: kind_name,
                resource_id: id.to_string(),
            });
        }
    }
    Ok(resources)
}
