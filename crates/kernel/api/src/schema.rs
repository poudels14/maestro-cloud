use std::fmt::{Display, Formatter};

use schemars::{JsonSchema, SchemaGenerator, generate::SchemaSettings};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};

use crate::{
    ArtifactArchiveUploadResponse, Assignment, Build, CommandRequest, Deployment,
    DeploymentCommandResponse, DnsRecord, FirewallPolicy, IngressRoute, Node, NodeFirewall,
    NodeNetwork, Preview, ReplicaState, ResourceKind, Service, ServiceCommandResponse,
    ServiceDiffChange, ServiceDiffRequest, ServiceDiffResponse, ServiceDiffStatus,
    ServiceReplicaOverrideRequest, ServiceRolloutDiffRequest, ServiceRolloutDiffResponse,
    ServiceRolloutRequest, ServiceRolloutResponse, ServiceRolloutRevisions, ServiceRolloutSpec,
    ServiceWriteRequest, ServiceWriteResponse, TrafficGeneration, UpgradeRun, Webhook,
};

/// Every resource kind shipped by Maestro itself.
///
/// Store and watch contracts use open [`ResourceKind`] values; this registry
/// only supplies schemas and decoders for the built-ins known to this binary.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
#[serde(rename_all = "camelCase")]
pub enum BuiltinKind {
    /// A cluster node.
    Node,
    /// A node mesh publication.
    NodeNetwork,
    /// A node's desired and applied firewall ruleset.
    NodeFirewall,
    /// A deployable service.
    Service,
    /// An immutable deployment.
    Deployment,
    /// A scheduled workload assignment.
    Assignment,
    /// An observed replica state.
    ReplicaState,
    /// An ingress route.
    IngressRoute,
    /// A blue/green traffic generation.
    TrafficGeneration,
    /// An atomic firewall policy.
    FirewallPolicy,
    /// An authoritative DNS record.
    DnsRecord,
    /// An artifact build.
    Build,
    /// A pull-request preview.
    Preview,
    /// A cluster upgrade run.
    UpgradeRun,
    /// An outbound webhook.
    Webhook,
}

impl BuiltinKind {
    /// Built-in kinds in deterministic schema and snapshot order.
    pub const ALL: [Self; 15] = [
        Self::Node,
        Self::NodeNetwork,
        Self::Service,
        Self::Deployment,
        Self::Assignment,
        Self::ReplicaState,
        Self::IngressRoute,
        Self::TrafficGeneration,
        Self::FirewallPolicy,
        Self::DnsRecord,
        Self::Build,
        Self::Preview,
        Self::UpgradeRun,
        Self::Webhook,
        Self::NodeFirewall,
    ];

    /// Stable PascalCase component and registry name.
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Node => "Node",
            Self::NodeNetwork => "NodeNetwork",
            Self::NodeFirewall => "NodeFirewall",
            Self::Service => "Service",
            Self::Deployment => "Deployment",
            Self::Assignment => "Assignment",
            Self::ReplicaState => "ReplicaState",
            Self::IngressRoute => "IngressRoute",
            Self::TrafficGeneration => "TrafficGeneration",
            Self::FirewallPolicy => "FirewallPolicy",
            Self::DnsRecord => "DnsRecord",
            Self::Build => "Build",
            Self::Preview => "Preview",
            Self::UpgradeRun => "UpgradeRun",
            Self::Webhook => "Webhook",
        }
    }
}

impl Display for BuiltinKind {
    fn fmt(&self, formatter: &mut Formatter<'_>) -> std::fmt::Result {
        formatter.write_str(self.as_str())
    }
}

impl TryFrom<&ResourceKind> for BuiltinKind {
    type Error = UnknownBuiltinKind;

    fn try_from(kind: &ResourceKind) -> Result<Self, Self::Error> {
        match kind.as_str() {
            "Node" => Ok(Self::Node),
            "NodeNetwork" => Ok(Self::NodeNetwork),
            "NodeFirewall" => Ok(Self::NodeFirewall),
            "Service" => Ok(Self::Service),
            "Deployment" => Ok(Self::Deployment),
            "Assignment" => Ok(Self::Assignment),
            "ReplicaState" => Ok(Self::ReplicaState),
            "IngressRoute" => Ok(Self::IngressRoute),
            "TrafficGeneration" => Ok(Self::TrafficGeneration),
            "FirewallPolicy" => Ok(Self::FirewallPolicy),
            "DnsRecord" => Ok(Self::DnsRecord),
            "Build" => Ok(Self::Build),
            "Preview" => Ok(Self::Preview),
            "UpgradeRun" => Ok(Self::UpgradeRun),
            "Webhook" => Ok(Self::Webhook),
            _ => Err(UnknownBuiltinKind(kind.clone())),
        }
    }
}

/// A decoded resource known to the built-in type registry.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", content = "resource", rename_all = "camelCase")]
pub enum BuiltinResource {
    /// A cluster node.
    Node(Node),
    /// A node mesh publication.
    NodeNetwork(NodeNetwork),
    /// A node's desired and applied firewall ruleset.
    NodeFirewall(NodeFirewall),
    /// A deployable service.
    Service(Service),
    /// An immutable deployment.
    Deployment(Deployment),
    /// A scheduled workload assignment.
    Assignment(Assignment),
    /// An observed replica state.
    ReplicaState(ReplicaState),
    /// An ingress route.
    IngressRoute(IngressRoute),
    /// A blue/green traffic generation.
    TrafficGeneration(TrafficGeneration),
    /// An atomic firewall policy.
    FirewallPolicy(FirewallPolicy),
    /// An authoritative DNS record.
    DnsRecord(DnsRecord),
    /// An artifact build.
    Build(Build),
    /// A pull-request preview.
    Preview(Preview),
    /// A cluster upgrade run.
    UpgradeRun(UpgradeRun),
    /// An outbound webhook.
    Webhook(Webhook),
}

impl BuiltinResource {
    /// Returns the stable kind represented by this decoded value.
    pub const fn kind(&self) -> BuiltinKind {
        match self {
            Self::Node(_) => BuiltinKind::Node,
            Self::NodeNetwork(_) => BuiltinKind::NodeNetwork,
            Self::NodeFirewall(_) => BuiltinKind::NodeFirewall,
            Self::Service(_) => BuiltinKind::Service,
            Self::Deployment(_) => BuiltinKind::Deployment,
            Self::Assignment(_) => BuiltinKind::Assignment,
            Self::ReplicaState(_) => BuiltinKind::ReplicaState,
            Self::IngressRoute(_) => BuiltinKind::IngressRoute,
            Self::TrafficGeneration(_) => BuiltinKind::TrafficGeneration,
            Self::FirewallPolicy(_) => BuiltinKind::FirewallPolicy,
            Self::DnsRecord(_) => BuiltinKind::DnsRecord,
            Self::Build(_) => BuiltinKind::Build,
            Self::Preview(_) => BuiltinKind::Preview,
            Self::UpgradeRun(_) => BuiltinKind::UpgradeRun,
            Self::Webhook(_) => BuiltinKind::Webhook,
        }
    }
}

/// Failure to resolve an open resource kind through the built-in registry.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("resource kind `{0}` is not registered as a Maestro built-in")]
pub struct UnknownBuiltinKind(pub ResourceKind);

/// Failure to decode a built-in resource from store or API JSON.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeResourceError {
    /// The open kind is not part of the built-in registry.
    #[error(transparent)]
    UnknownKind(#[from] UnknownBuiltinKind),
    /// The payload does not satisfy the registered kind's wire contract.
    #[error("resource payload for `{kind}` is malformed: {message}")]
    Malformed {
        /// Registered kind selected for decoding.
        kind: BuiltinKind,
        /// Serde decoding detail suitable for a surfaced status condition.
        message: String,
    },
}

/// Decodes a JSON value through the built-in type registry.
///
/// Unknown kinds remain distinguishable so store layers can preserve future
/// custom resources instead of treating them as malformed built-ins.
pub fn decode_builtin(
    kind: &ResourceKind,
    value: Value,
) -> Result<BuiltinResource, DecodeResourceError> {
    let kind = BuiltinKind::try_from(kind)?;
    let decoded = match kind {
        BuiltinKind::Node => serde_json::from_value(value).map(BuiltinResource::Node),
        BuiltinKind::NodeNetwork => serde_json::from_value(value).map(BuiltinResource::NodeNetwork),
        BuiltinKind::NodeFirewall => {
            serde_json::from_value(value).map(BuiltinResource::NodeFirewall)
        }
        BuiltinKind::Service => serde_json::from_value(value).map(BuiltinResource::Service),
        BuiltinKind::Deployment => serde_json::from_value(value).map(BuiltinResource::Deployment),
        BuiltinKind::Assignment => serde_json::from_value(value).map(BuiltinResource::Assignment),
        BuiltinKind::ReplicaState => {
            serde_json::from_value(value).map(BuiltinResource::ReplicaState)
        }
        BuiltinKind::IngressRoute => {
            serde_json::from_value(value).map(BuiltinResource::IngressRoute)
        }
        BuiltinKind::TrafficGeneration => {
            serde_json::from_value(value).map(BuiltinResource::TrafficGeneration)
        }
        BuiltinKind::FirewallPolicy => {
            serde_json::from_value(value).map(BuiltinResource::FirewallPolicy)
        }
        BuiltinKind::DnsRecord => serde_json::from_value(value).map(BuiltinResource::DnsRecord),
        BuiltinKind::Build => serde_json::from_value(value).map(BuiltinResource::Build),
        BuiltinKind::Preview => serde_json::from_value(value).map(BuiltinResource::Preview),
        BuiltinKind::UpgradeRun => serde_json::from_value(value).map(BuiltinResource::UpgradeRun),
        BuiltinKind::Webhook => serde_json::from_value(value).map(BuiltinResource::Webhook),
    };
    decoded.map_err(|error| DecodeResourceError::Malformed {
        kind,
        message: error.to_string(),
    })
}

/// Generates the canonical OpenAPI 3 document for all built-in resources.
///
/// The returned document is deterministic and has no routes until the server
/// crate composes its path operations over these shared component schemas.
pub fn openapi_document() -> Value {
    let mut generator = SchemaGenerator::new(SchemaSettings::openapi3());
    register_schema::<Node>(&mut generator, BuiltinKind::Node);
    register_schema::<NodeNetwork>(&mut generator, BuiltinKind::NodeNetwork);
    register_schema::<Service>(&mut generator, BuiltinKind::Service);
    register_schema::<Deployment>(&mut generator, BuiltinKind::Deployment);
    register_schema::<Assignment>(&mut generator, BuiltinKind::Assignment);
    register_schema::<ReplicaState>(&mut generator, BuiltinKind::ReplicaState);
    register_schema::<IngressRoute>(&mut generator, BuiltinKind::IngressRoute);
    register_schema::<TrafficGeneration>(&mut generator, BuiltinKind::TrafficGeneration);
    register_schema::<FirewallPolicy>(&mut generator, BuiltinKind::FirewallPolicy);
    register_schema::<DnsRecord>(&mut generator, BuiltinKind::DnsRecord);
    register_schema::<Build>(&mut generator, BuiltinKind::Build);
    register_schema::<Preview>(&mut generator, BuiltinKind::Preview);
    register_schema::<UpgradeRun>(&mut generator, BuiltinKind::UpgradeRun);
    register_schema::<Webhook>(&mut generator, BuiltinKind::Webhook);
    register_schema::<NodeFirewall>(&mut generator, BuiltinKind::NodeFirewall);
    register_named_schema::<CommandRequest>(&mut generator, "CommandRequest");
    register_named_schema::<ArtifactArchiveUploadResponse>(
        &mut generator,
        "ArtifactArchiveUploadResponse",
    );
    register_named_schema::<ServiceReplicaOverrideRequest>(
        &mut generator,
        "ServiceReplicaOverrideRequest",
    );
    register_named_schema::<ServiceCommandResponse>(&mut generator, "ServiceCommandResponse");
    register_named_schema::<DeploymentCommandResponse>(&mut generator, "DeploymentCommandResponse");
    register_named_schema::<ServiceWriteRequest>(&mut generator, "ServiceWriteRequest");
    register_named_schema::<ServiceWriteResponse>(&mut generator, "ServiceWriteResponse");
    register_named_schema::<ServiceDiffRequest>(&mut generator, "ServiceDiffRequest");
    register_named_schema::<ServiceDiffResponse>(&mut generator, "ServiceDiffResponse");
    register_named_schema::<ServiceDiffStatus>(&mut generator, "ServiceDiffStatus");
    register_named_schema::<ServiceDiffChange>(&mut generator, "ServiceDiffChange");
    register_named_schema::<ServiceRolloutSpec>(&mut generator, "ServiceRolloutSpec");
    register_named_schema::<ServiceRolloutRevisions>(&mut generator, "ServiceRolloutRevisions");
    register_named_schema::<ServiceRolloutDiffRequest>(&mut generator, "ServiceRolloutDiffRequest");
    register_named_schema::<ServiceRolloutDiffResponse>(
        &mut generator,
        "ServiceRolloutDiffResponse",
    );
    register_named_schema::<ServiceRolloutRequest>(&mut generator, "ServiceRolloutRequest");
    register_named_schema::<ServiceRolloutResponse>(&mut generator, "ServiceRolloutResponse");

    let schemas = generator.take_definitions(true);
    json!({
        "openapi": "3.0.4",
        "info": {
            "title": "Maestro Cloud API",
            "version": env!("CARGO_PKG_VERSION")
        },
        "paths": {},
        "components": {
            "schemas": schemas
        }
    })
}

fn register_schema<Resource>(generator: &mut SchemaGenerator, kind: BuiltinKind)
where
    Resource: JsonSchema,
{
    let schema = generator.subschema_for::<Resource>().to_value();
    generator
        .definitions_mut()
        .insert(kind.as_str().to_string(), schema);
}

fn register_named_schema<Schema>(generator: &mut SchemaGenerator, name: &str)
where
    Schema: JsonSchema,
{
    let _ = generator.subschema_for::<Schema>();
    debug_assert!(generator.definitions().contains_key(name));
}
