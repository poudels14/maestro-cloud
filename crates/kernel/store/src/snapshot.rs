use std::fmt::{Debug, Formatter};

use async_trait::async_trait;
use kernel_api::{
    Assignment, Build, BuiltinResource, ClusterId, Deployment, DnsRecord, FirewallPolicy,
    IngressBlocklist, IngressRoute, InvalidIdentifier, Node, NodeFirewall, NodeNetwork,
    NodeTombstone, PlacementHistory, Preview, ReplicaState, ResourceKind, ResourceName, Service,
    TrafficGeneration, UpgradeRun, Webhook, decode_builtin,
};
use serde_json::Value;

use crate::{Keyspace, Store, StoreError, WatchCursor};

const REDACTED: &str = "[REDACTED]";

/// One resource whose open kind is not registered by this Maestro binary.
///
/// The payload stays available for lossless test inspection, but is omitted
/// from `Debug` and normalized serialization because its secret-bearing fields
/// are unknowable without a registered schema.
#[derive(Clone, PartialEq, Eq)]
pub struct UnregisteredResource {
    /// Open resource kind parsed from the canonical store key.
    pub kind: ResourceKind,
    /// Open resource identity parsed from the canonical store key.
    pub id: ResourceName,
    value: Value,
}

impl UnregisteredResource {
    /// Returns the preserved custom-resource JSON payload.
    pub const fn value(&self) -> &Value {
        &self.value
    }
}

impl Debug for UnregisteredResource {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("UnregisteredResource")
            .field("kind", &self.kind)
            .field("id", &self.id)
            .field("value", &REDACTED)
            .finish()
    }
}

/// Every typed resource visible in one linearizable cluster-store snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ClusterSnapshot {
    /// Cursor immediately after the linearizable resource listing.
    pub cursor: WatchCursor,
    /// Cluster node resources in key order.
    pub nodes: Vec<Node>,
    /// Removed-node identity guards in key order.
    pub node_tombstones: Vec<NodeTombstone>,
    /// WireGuard publications in key order.
    pub node_networks: Vec<NodeNetwork>,
    /// Desired node firewall rulesets in key order.
    pub node_firewalls: Vec<NodeFirewall>,
    /// Deployable services in key order.
    pub services: Vec<Service>,
    /// Immutable deployments in key order.
    pub deployments: Vec<Deployment>,
    /// Scheduled assignments in key order.
    pub assignments: Vec<Assignment>,
    /// Durable placement audit records in key order.
    pub placement_histories: Vec<PlacementHistory>,
    /// Observed replica states in key order.
    pub replica_states: Vec<ReplicaState>,
    /// Ingress routes in key order.
    pub ingress_routes: Vec<IngressRoute>,
    /// Ingress client-address blocklists in key order.
    pub ingress_blocklists: Vec<IngressBlocklist>,
    /// Blue/green traffic generations in key order.
    pub traffic_generations: Vec<TrafficGeneration>,
    /// Atomic firewall policies in key order.
    pub firewall_policies: Vec<FirewallPolicy>,
    /// Authoritative DNS records in key order.
    pub dns_records: Vec<DnsRecord>,
    /// Artifact builds in key order.
    pub builds: Vec<Build>,
    /// Pull-request previews in key order.
    pub previews: Vec<Preview>,
    /// Cluster maintenance runs in key order.
    pub upgrade_runs: Vec<UpgradeRun>,
    /// Outbound webhook configurations in key order.
    pub webhooks: Vec<Webhook>,
    /// Future or custom resource kinds preserved without schema assumptions.
    pub unregistered_resources: Vec<UnregisteredResource>,
}

impl ClusterSnapshot {
    fn empty(cursor: WatchCursor) -> Self {
        Self {
            cursor,
            nodes: Vec::new(),
            node_tombstones: Vec::new(),
            node_networks: Vec::new(),
            node_firewalls: Vec::new(),
            services: Vec::new(),
            deployments: Vec::new(),
            assignments: Vec::new(),
            placement_histories: Vec::new(),
            replica_states: Vec::new(),
            ingress_routes: Vec::new(),
            ingress_blocklists: Vec::new(),
            traffic_generations: Vec::new(),
            firewall_policies: Vec::new(),
            dns_records: Vec::new(),
            builds: Vec::new(),
            previews: Vec::new(),
            upgrade_runs: Vec::new(),
            webhooks: Vec::new(),
            unregistered_resources: Vec::new(),
        }
    }

    /// Returns the number of built-in and unregistered resources in the tree.
    pub fn resource_count(&self) -> usize {
        self.nodes.len()
            + self.node_tombstones.len()
            + self.node_networks.len()
            + self.node_firewalls.len()
            + self.services.len()
            + self.deployments.len()
            + self.assignments.len()
            + self.placement_histories.len()
            + self.replica_states.len()
            + self.ingress_routes.len()
            + self.ingress_blocklists.len()
            + self.traffic_generations.len()
            + self.firewall_policies.len()
            + self.dns_records.len()
            + self.builds.len()
            + self.previews.len()
            + self.upgrade_runs.len()
            + self.webhooks.len()
            + self.unregistered_resources.len()
    }

    fn push(&mut self, resource: BuiltinResource) {
        match resource {
            BuiltinResource::Node(value) => self.nodes.push(value),
            BuiltinResource::NodeTombstone(value) => self.node_tombstones.push(value),
            BuiltinResource::NodeNetwork(value) => self.node_networks.push(value),
            BuiltinResource::NodeFirewall(value) => self.node_firewalls.push(value),
            BuiltinResource::Service(value) => self.services.push(value),
            BuiltinResource::Deployment(value) => self.deployments.push(value),
            BuiltinResource::Assignment(value) => self.assignments.push(value),
            BuiltinResource::PlacementHistory(value) => self.placement_histories.push(value),
            BuiltinResource::ReplicaState(value) => self.replica_states.push(value),
            BuiltinResource::IngressRoute(value) => self.ingress_routes.push(value),
            BuiltinResource::IngressBlocklist(value) => self.ingress_blocklists.push(value),
            BuiltinResource::TrafficGeneration(value) => self.traffic_generations.push(value),
            BuiltinResource::FirewallPolicy(value) => self.firewall_policies.push(value),
            BuiltinResource::DnsRecord(value) => self.dns_records.push(value),
            BuiltinResource::Build(value) => self.builds.push(value),
            BuiltinResource::Preview(value) => self.previews.push(value),
            BuiltinResource::UpgradeRun(value) => self.upgrade_runs.push(value),
            BuiltinResource::Webhook(value) => self.webhooks.push(value),
        }
    }
}

/// Adds a point-in-time typed dump to every conforming store backend.
#[async_trait]
pub trait StoreSnapshotExt {
    /// Decodes every resource below one cluster's canonical resource prefix.
    async fn dump(&self, cluster_id: &ClusterId) -> Result<ClusterSnapshot, SnapshotError>;
}

#[async_trait]
impl<Backend> StoreSnapshotExt for Backend
where
    Backend: Store + ?Sized,
{
    async fn dump(&self, cluster_id: &ClusterId) -> Result<ClusterSnapshot, SnapshotError> {
        let prefix = Keyspace::new(cluster_id).resources();
        let listing = self.list(&prefix).await?;
        let mut snapshot = ClusterSnapshot::empty(listing.cursor);

        for stored in listing.values {
            let (kind, id) = parse_resource_key(prefix.as_str(), stored.key.as_str())?;
            let value = serde_json::from_slice::<Value>(&stored.value).map_err(|error| {
                SnapshotError::MalformedJson {
                    key: stored.key.to_string(),
                    message: error.to_string(),
                }
            })?;
            match decode_builtin(&kind, value.clone()) {
                Ok(mut resource) => {
                    if resource.id() != id.as_str() {
                        return Err(SnapshotError::IdentityMismatch {
                            key: stored.key.to_string(),
                            key_id: id.to_string(),
                            payload_id: resource.id().to_string(),
                        });
                    }
                    resource.set_observed_revision(stored.version.resource_revision());
                    snapshot.push(resource);
                }
                Err(kernel_api::DecodeResourceError::UnknownKind(_)) => {
                    snapshot
                        .unregistered_resources
                        .push(UnregisteredResource { kind, id, value });
                }
                Err(kernel_api::DecodeResourceError::Malformed { kind, message }) => {
                    return Err(SnapshotError::MalformedBuiltin {
                        key: stored.key.to_string(),
                        kind: kind.to_string(),
                        message,
                    });
                }
            }
        }
        Ok(snapshot)
    }
}

/// Matchable failure while constructing or normalizing a cluster snapshot.
#[derive(Debug, thiserror::Error)]
pub enum SnapshotError {
    /// The backend could not produce a linearizable resource listing.
    #[error(transparent)]
    Store(#[from] StoreError),
    /// A backend resource key did not have exactly a kind and identity suffix.
    #[error("resource store key `{key}` is malformed")]
    MalformedKey {
        /// Exact rejected backend key.
        key: String,
    },
    /// A key segment violated the open resource identifier contract.
    #[error("resource store key `{key}` contains an invalid identifier: {source}")]
    InvalidIdentifier {
        /// Exact rejected backend key.
        key: String,
        /// Identifier validation detail.
        #[source]
        source: InvalidIdentifier,
    },
    /// Stored bytes were not a JSON resource document.
    #[error("resource store value at `{key}` is malformed JSON: {message}")]
    MalformedJson {
        /// Exact resource key.
        key: String,
        /// JSON decoder detail.
        message: String,
    },
    /// A registered built-in resource violated its wire schema.
    #[error("built-in `{kind}` resource at `{key}` is malformed: {message}")]
    MalformedBuiltin {
        /// Exact resource key.
        key: String,
        /// Registered built-in kind.
        kind: String,
        /// Typed decoder detail.
        message: String,
    },
    /// The resource payload identity disagreed with its canonical key.
    #[error(
        "resource identity mismatch at `{key}`: key identifies `{key_id}`, payload identifies `{payload_id}`"
    )]
    IdentityMismatch {
        /// Exact resource key.
        key: String,
        /// Identity parsed from the key.
        key_id: String,
        /// Identity decoded from the payload.
        payload_id: String,
    },
    /// A typed resource could not be converted to normalized JSON.
    #[error("failed to normalize `{kind}` resource: {message}")]
    Normalize {
        /// Built-in resource kind.
        kind: &'static str,
        /// Serializer detail.
        message: String,
    },
}

fn parse_resource_key(
    prefix: &str,
    key: &str,
) -> Result<(ResourceKind, ResourceName), SnapshotError> {
    let Some(relative) = key.strip_prefix(prefix) else {
        return Err(SnapshotError::MalformedKey {
            key: key.to_string(),
        });
    };
    let mut segments = relative.split('/');
    let (Some(kind), Some(id), None) = (segments.next(), segments.next(), segments.next()) else {
        return Err(SnapshotError::MalformedKey {
            key: key.to_string(),
        });
    };
    let kind = ResourceKind::new(kind).map_err(|source| SnapshotError::InvalidIdentifier {
        key: key.to_string(),
        source,
    })?;
    let id = ResourceName::new(id).map_err(|source| SnapshotError::InvalidIdentifier {
        key: key.to_string(),
        source,
    })?;
    Ok((kind, id))
}
