use std::collections::{BTreeMap, BTreeSet};

use schemars::JsonSchema;
use serde::{Deserialize, Serialize};

use crate::ResourceId;

/// Monotonic store revision observed for a resource.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct ResourceRevision(pub u64);

/// Monotonic desired-state generation assigned when a resource specification changes.
#[derive(
    Debug, Clone, Copy, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct Generation(pub u64);

/// A UTC Unix timestamp represented in milliseconds.
#[derive(
    Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema,
)]
pub struct Timestamp(pub i64);

/// A validated label key used for selection and grouping.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct LabelKey(pub String);

/// An annotation key for non-selecting resource metadata.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct AnnotationKey(pub String);

/// A finalizer registered by a controller before deletion can complete.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize, JsonSchema)]
#[serde(transparent)]
pub struct FinalizerName(pub String);

/// Whether an owner reference controls cascading deletion.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub enum Ownership {
    /// The referenced resource controls this resource's lifecycle.
    Controller,
    /// The reference records provenance without controlling deletion.
    Informational,
}

/// A typed reference to a resource that created or manages another resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct OwnerReference {
    /// Identity and kind of the owner.
    pub resource: ResourceId,
    /// Whether the owner controls cascading deletion.
    pub ownership: Ownership,
}

/// Metadata shared by every resource kind.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "camelCase")]
pub struct ObjectMeta<Id> {
    /// Kind-specific stable resource identity.
    pub id: Id,
    /// Labels used by selectors and grouping.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub labels: BTreeMap<LabelKey, String>,
    /// Non-selecting metadata interpreted by named consumers.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub annotations: BTreeMap<AnnotationKey, String>,
    /// Store revision used for optimistic concurrency.
    pub revision: ResourceRevision,
    /// Desired-state generation incremented when the specification changes.
    pub generation: Generation,
    /// Resources whose lifecycle or provenance relates to this object.
    #[serde(default, skip_serializing_if = "Vec::is_empty")]
    pub owner_refs: Vec<OwnerReference>,
    /// Controllers that must finish cleanup before physical deletion.
    #[serde(default, skip_serializing_if = "BTreeSet::is_empty")]
    pub finalizers: BTreeSet<FinalizerName>,
    /// Time deletion was requested, or `None` while the resource is active.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub deletion_timestamp: Option<Timestamp>,
}
