use kernel_api::{BuiltinKind, BuiltinResource, ClusterId, ResourceName};

/// One exact canonical resource write produced by legacy-schema conversion.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationWrite {
    kind: BuiltinKind,
    id: ResourceName,
    value: Vec<u8>,
}

impl MigrationWrite {
    /// Serializes one typed resource into its canonical store representation.
    pub fn from_resource(resource: BuiltinResource) -> Result<Self, PlanError> {
        macro_rules! encode {
            ($kind:ident, $resource:ident) => {{
                let id = ResourceName::from($resource.meta.id.clone());
                let value = serde_json::to_vec(&$resource).map_err(|error| {
                    PlanError::SerializeResource {
                        kind: BuiltinKind::$kind,
                        id: id.clone(),
                        message: error.to_string(),
                    }
                })?;
                Self {
                    kind: BuiltinKind::$kind,
                    id,
                    value,
                }
            }};
        }

        Ok(match resource {
            BuiltinResource::Node(resource) => encode!(Node, resource),
            BuiltinResource::NodeTombstone(resource) => encode!(NodeTombstone, resource),
            BuiltinResource::NodeNetwork(resource) => encode!(NodeNetwork, resource),
            BuiltinResource::NodeFirewall(resource) => encode!(NodeFirewall, resource),
            BuiltinResource::Service(resource) => encode!(Service, resource),
            BuiltinResource::Deployment(resource) => encode!(Deployment, resource),
            BuiltinResource::Assignment(resource) => encode!(Assignment, resource),
            BuiltinResource::ReplicaState(resource) => encode!(ReplicaState, resource),
            BuiltinResource::IngressRoute(resource) => encode!(IngressRoute, resource),
            BuiltinResource::IngressBlocklist(resource) => encode!(IngressBlocklist, resource),
            BuiltinResource::TrafficGeneration(resource) => {
                encode!(TrafficGeneration, resource)
            }
            BuiltinResource::FirewallPolicy(resource) => encode!(FirewallPolicy, resource),
            BuiltinResource::DnsRecord(resource) => encode!(DnsRecord, resource),
            BuiltinResource::Build(resource) => encode!(Build, resource),
            BuiltinResource::Preview(resource) => encode!(Preview, resource),
            BuiltinResource::UpgradeRun(resource) => encode!(UpgradeRun, resource),
            BuiltinResource::Webhook(resource) => encode!(Webhook, resource),
        })
    }

    /// Returns the built-in destination kind.
    pub const fn kind(&self) -> BuiltinKind {
        self.kind
    }

    /// Returns the destination resource identity.
    pub fn id(&self) -> &ResourceName {
        &self.id
    }

    /// Returns the exact bytes reused across retries.
    pub fn value(&self) -> &[u8] {
        &self.value
    }
}

/// Deterministically ordered, snapshot-bound set of destination writes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct MigrationPlan {
    cluster_id: ClusterId,
    source_digest: [u8; 32],
    writes: Vec<MigrationWrite>,
}

impl MigrationPlan {
    /// Converts typed resources into a collision-free, deterministic write set.
    pub fn new(
        cluster_id: ClusterId,
        source_digest: [u8; 32],
        resources: impl IntoIterator<Item = BuiltinResource>,
    ) -> Result<Self, PlanError> {
        let writes = resources
            .into_iter()
            .map(MigrationWrite::from_resource)
            .collect::<Result<Vec<_>, _>>()?;
        Self::from_writes(cluster_id, source_digest, writes)
    }

    /// Validates and orders already serialized migration writes.
    pub fn from_writes(
        cluster_id: ClusterId,
        source_digest: [u8; 32],
        mut writes: Vec<MigrationWrite>,
    ) -> Result<Self, PlanError> {
        writes.sort_by(|left, right| (left.kind, &left.id).cmp(&(right.kind, &right.id)));
        if let Some((kind, id)) = writes.windows(2).find_map(|pair| match pair {
            [left, right] if left.kind == right.kind && left.id == right.id => {
                Some((left.kind, left.id.clone()))
            }
            _ => None,
        }) {
            return Err(PlanError::DuplicateResource { kind, id });
        }
        Ok(Self {
            cluster_id,
            source_digest,
            writes,
        })
    }

    /// Returns the legacy cluster identity that must own the destination keyspace.
    pub const fn cluster_id(&self) -> &ClusterId {
        &self.cluster_id
    }

    /// Returns the canonical legacy snapshot digest.
    pub const fn source_digest(&self) -> [u8; 32] {
        self.source_digest
    }

    /// Returns writes in stable kind-and-identity order.
    pub fn writes(&self) -> &[MigrationWrite] {
        &self.writes
    }
}

/// Typed resource conversion could not produce a safe destination plan.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PlanError {
    /// A resource failed canonical JSON serialization.
    #[error("could not serialize {kind}/{id}: {message}")]
    SerializeResource {
        /// Destination kind.
        kind: BuiltinKind,
        /// Destination identity.
        id: ResourceName,
        /// Serialization error detail.
        message: String,
    },
    /// Two legacy records mapped to the same destination resource.
    #[error("migration plan contains duplicate destination {kind}/{id}")]
    DuplicateResource {
        /// Colliding kind.
        kind: BuiltinKind,
        /// Colliding identity.
        id: ResourceName,
    },
}
