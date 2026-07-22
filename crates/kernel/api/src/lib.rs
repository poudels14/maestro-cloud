//! Canonical resource and wire contracts for Maestro.
//!
//! This crate contains types and schemas only. It must not depend on stores,
//! controller runtimes, node implementations, operators, or applications.

mod automation;
mod command;
mod condition;
mod exec_stream;
mod identity;
mod metadata;
mod network;
mod node;
mod resource;
mod schema;
mod secret;
mod service_validation;
mod workload;

pub use automation::{
    NodeUpgradeStatus, Preview, PreviewPhase, PreviewSpec, PreviewStatus, UpgradeMode,
    UpgradePhase, UpgradeRun, UpgradeRunSpec, UpgradeRunStatus, Webhook, WebhookEvent,
    WebhookNodeAvailability, WebhookObservation, WebhookObservedState, WebhookSpec, WebhookStatus,
};
pub use command::{
    ArtifactArchiveUploadResponse, CommandRequest, DeploymentCommandResponse,
    MAXIMUM_ARTIFACT_ARCHIVE_BYTES, NodeCommandResponse, ServiceCommandResponse, ServiceDiffChange,
    ServiceDiffRequest, ServiceDiffResponse, ServiceDiffStatus, ServiceReplicaOverrideRequest,
    ServiceRolloutDiffRequest, ServiceRolloutDiffResponse, ServiceRolloutRequest,
    ServiceRolloutResponse, ServiceRolloutRevisions, ServiceRolloutSpec, ServiceWriteRequest,
    ServiceWriteResponse, UpgradeCommandResponse, UpgradeCreateRequest,
};
pub use condition::{Condition, ConditionReason, ConditionState, ConditionType};
pub use exec_stream::{ExecStreamFrame, ExecStreamProtocolError};
pub use identity::{
    ArtifactArchiveId, AssignmentId, BuildId, ClusterId, DeploymentId, DnsRecordId,
    FirewallPolicyId, IngressBlocklistId, IngressRouteId, InvalidIdentifier, NodeFirewallId,
    NodeId, NodeInstanceId, NodeNetworkId, PreviewId, ReplicaStateId, RequestId, ResourceId,
    ResourceKind, ResourceName, ServiceId, TrafficGenerationId, UpgradeRunId, WebhookId,
    WorkloadId,
};
pub use metadata::{
    AnnotationKey, FinalizerName, Generation, LabelKey, ObjectMeta, OwnerReference, Ownership,
    ResourceRevision, Timestamp,
};
pub use network::{
    DnsRecord, DnsRecordSpec, DnsRecordStatus, DnsRecordValue, FirewallDirection, FirewallPolicy,
    FirewallPolicySpec, FirewallPolicyStatus, FirewallRule, FirewallSubject, FirewallVerdict,
    IngressBlocklist, IngressBlocklistSpec, IngressBlocklistStatus, IngressRoute, IngressRouteSpec,
    IngressRouteStatus, NetworkAddress, PortRange, SessionAffinity, TrafficGeneration,
    TrafficGenerationPhase, TrafficGenerationSpec, TrafficGenerationStatus, TrafficRoute,
    TrafficTarget, TransportProtocol,
};
pub use node::{
    ClusterInfo, Node, NodeFirewall, NodeFirewallSpec, NodeFirewallStatus, NodeNetwork,
    NodeNetworkSpec, NodeNetworkStatus, NodeRole, NodeSpec, NodeStatus,
};
pub use resource::Object;
pub use schema::{
    BuiltinKind, BuiltinResource, DecodeResourceError, UnknownBuiltinKind, decode_builtin,
    openapi_document,
};
pub use secret::{MaskedSecret, SecretValue};
pub use service_validation::ServiceSpecError;
pub use workload::{
    ArtifactTemplate, Assignment, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    BUILD_WATCH_REVISION_ANNOTATION, Build, BuildPhase, BuildSource, BuildSpec, BuildStatus,
    BuildTemplate, CommandSpec, Deployment, DeploymentGoal, DeploymentPhase, DeploymentSpec,
    DeploymentStatus, ExecPolicy, HealthCheckSpec, HealthProbe, NodeApiAccess, PlacementConstraint,
    PreviewPolicy, ReplicaState, ReplicaStateSpec, ReplicaStateStatus, RolloutState,
    SecretMountSpec, Service, ServiceSpec, ServiceStatus, VolumeAccess, VolumeMountSpec,
    VolumeSource, WorkloadUserSpec, workload_hostname,
};

#[cfg(test)]
mod tests;
