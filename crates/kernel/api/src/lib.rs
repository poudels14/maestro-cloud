//! Canonical resource and wire contracts for Maestro.
//!
//! This crate contains types and schemas only. It must not depend on stores,
//! controller runtimes, node implementations, operators, or applications.

mod condition;
mod identity;
mod metadata;
mod resource;

pub use condition::{Condition, ConditionReason, ConditionState, ConditionType};
pub use identity::{
    AssignmentId, BuildId, DeploymentId, DnsRecordId, FirewallPolicyId, IngressRouteId,
    InvalidIdentifier, NodeId, NodeNetworkId, PreviewId, ReplicaStateId, ResourceId, ServiceId,
    TrafficGenerationId, UpgradeRunId, WebhookId, WorkloadId,
};
pub use metadata::{
    AnnotationKey, FinalizerName, Generation, LabelKey, ObjectMeta, OwnerReference, Ownership,
    ResourceRevision, Timestamp,
};
pub use resource::Object;

#[cfg(test)]
mod tests;
