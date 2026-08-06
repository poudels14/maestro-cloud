//! Canonical resource and wire contracts for Maestro.
//!
//! This crate contains types and schemas only. It must not depend on stores,
//! controller runtimes, node implementations, operators, or applications.

mod automation;
mod command;
mod condition;
mod config;
mod dns;
mod environment;
mod exec_stream;
mod identity;
mod metadata;
mod network;
mod node;
mod resource;
mod schema;
mod secret;
mod service_validation;
mod system_service;
mod value_source;
mod workload;

pub use crate::version::MAESTRO_VERSION;
pub use automation::{
    NodeUpgradeStatus, Preview, PreviewPhase, PreviewSpec, PreviewStatus, RESTART_TARGET_VERSION,
    UpgradeMode, UpgradeOperation, UpgradePhase, UpgradeRun, UpgradeRunSpec, UpgradeRunStatus,
    Webhook, WebhookCategory, WebhookEvent, WebhookFormat, WebhookNodeAvailability,
    WebhookObservation, WebhookObservedState, WebhookSpec, WebhookStatus,
};
pub use command::{
    ArtifactArchiveUploadResponse, CommandRequest, DeploymentCommandResponse,
    MAXIMUM_ARTIFACT_ARCHIVE_BYTES, NodeCommandResponse, NodeRemovalRequest, NodeRemovalResponse,
    NodeRemovalState, ServiceCommandResponse, ServiceDiffChange, ServiceDiffRequest,
    ServiceDiffResponse, ServiceDiffStatus, ServiceReplicaOverrideRequest,
    ServiceRolloutDiffRequest, ServiceRolloutDiffResponse, ServiceRolloutRequest,
    ServiceRolloutResponse, ServiceRolloutRevisions, ServiceRolloutSpec, ServiceWriteRequest,
    ServiceWriteResponse, TailscaleAuthKeyRotationRequest, TailscaleAuthKeyRotationResponse,
    TailscaleAuthKeyStatus, UpgradeCommandResponse, UpgradeCreateRequest,
};
pub use condition::{Condition, ConditionReason, ConditionState, ConditionType};
pub use config::{
    MaskedCloudflareConfig, MaskedCloudflareTunnelConfig, MaskedClusterConfig,
    MaskedClusterConfigNode, MaskedClusterConfigPorts, MaskedCrossClusterDnsRoute,
    MaskedTailscaleConfig, PreviewLaunchConfigUpdateRequest, PreviewLaunchConfigUpdateResponse,
};
pub use dns::{DnsLabel, DnsName, DnsNameError, WildcardDnsName};
pub use environment::{
    EnvironmentName, EnvironmentTemplateContext, EnvironmentTemplateError, InvalidEnvironmentName,
    MAESTRO_INGRESS_HOST, MAESTRO_INGRESS_PORT,
};
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
    IngressRouteStatus, IngressRouting, NetworkAddress, PortRange, SessionAffinity,
    TrafficGeneration, TrafficGenerationPhase, TrafficGenerationSpec, TrafficGenerationStatus,
    TrafficRoute, TrafficTarget, TransportProtocol,
};
pub use node::{
    ClusterInfo, Node, NodeFirewall, NodeFirewallSpec, NodeFirewallStatus, NodeNetwork,
    NodeNetworkSpec, NodeNetworkStatus, NodeRole, NodeSpec, NodeStatus, NodeTombstone,
    NodeTombstoneSpec, NodeTombstoneStatus, UnschedulableReplica, WorkloadNetworkMode,
};
pub use resource::Object;
pub use schema::{
    BuiltinKind, BuiltinResource, DecodeResourceError, UnknownBuiltinKind, decode_builtin,
    openapi_document,
};
pub use secret::{MaskedSecret, SecretValue};
pub use service_validation::ServiceSpecError;
pub use system_service::{
    CLOUDFLARE_SERVICE_ID, DNS_RESOLVER_SERVICE_ID, SYSTEM_RESOURCE_PREFIX,
    TAILSCALE_GATEWAY_SERVICE_ID, TRAEFIK_SERVICE_ID, is_system_resource_id, is_system_service,
};
pub use value_source::{ExternalValueSource, InvalidExternalValueSource};
pub use workload::{
    ArtifactTemplate, Assignment, AssignmentPhase, AssignmentSpec, AssignmentStatus,
    BUILD_WATCH_REVISION_ANNOTATION, Build, BuildPhase, BuildSource, BuildSpec, BuildStatus,
    BuildTemplate, CommandSpec, DEFAULT_MAX_RESTART_ATTEMPTS, Deployment, DeploymentGoal,
    DeploymentPhase, DeploymentSpec, DeploymentStatus, DepotBuildConfig, ExecPolicy, GitCommit,
    HealthCheckSpec, HealthProbe, NodeApiAccess, PlacementConstraint, PlacementHistory,
    PlacementHistorySpec, PlacementHistoryStatus, PreviewPolicy, ReplicaSpread, ReplicaState,
    ReplicaStateSpec, ReplicaStateStatus, RolloutState, SecretMountSpec, Service, ServiceSpec,
    ServiceStatus, VolumeAccess, VolumeMountSpec, VolumeSource, WorkloadUserSpec,
    assignment_workload_address, workload_hostname,
};

mod version;

#[cfg(test)]
mod tests;
