use std::collections::BTreeMap;

use kernel_api::{BuildId, ClusterId, NodeId, Timestamp, WorkloadId};
use runtime::WorkloadMetadata;
use serde::{Deserialize, Serialize};

/// Stable producer-local identity used to deduplicate replayed log delivery.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct LogRecordId {
    /// Node that received the record from its original producer.
    pub node_id: NodeId,
    /// Typed producer identity within the receiving node.
    pub producer: LogProducer,
    /// Opaque producer cursor; only equality and ordering within this producer are meaningful.
    pub cursor: OriginCursor,
}

/// Typed source whose local cursor defines replay identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(tag = "type", content = "id", rename_all = "camelCase")]
pub enum LogProducer {
    /// Runtime log stream for one workload instance.
    Workload(WorkloadId),
    /// Internal component whose name is stable within a node.
    System(String),
    /// Artifact build output.
    Build(BuildId),
}

/// Opaque producer-native resume position used only for exact replay identity.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(transparent)]
pub struct OriginCursor(String);

impl OriginCursor {
    /// Wraps a producer cursor without interpreting its format.
    pub fn new(value: impl Into<String>) -> Self {
        Self(value.into())
    }

    /// Returns the opaque cursor for diagnostics or backend serialization.
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

/// Typed ownership retained on every normalized record.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(
    tag = "type",
    rename_all = "camelCase",
    rename_all_fields = "camelCase"
)]
pub enum LogOrigin {
    /// Runtime output from one adopted workload.
    Workload { metadata: WorkloadMetadata },
    /// Node or cluster component output not owned by a workload.
    System {
        cluster_id: ClusterId,
        node_id: Option<NodeId>,
        component: String,
    },
    /// Output associated with one artifact build.
    Build {
        cluster_id: ClusterId,
        node_id: NodeId,
        build_id: BuildId,
    },
}

/// Logical stream from which the record was read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum LogStream {
    /// Primary process standard output.
    Stdout,
    /// Primary process standard error.
    Stderr,
    /// Authenticated OpenTelemetry log export.
    Otlp,
    /// Internal Maestro component output.
    System,
}

/// Log body retained without forcing arbitrary producer bytes through UTF-8.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "type", content = "value", rename_all = "camelCase")]
pub enum LogBody {
    /// Valid textual content used by parsers and full-text search.
    Text(String),
    /// Non-UTF-8 producer content preserved byte-for-byte.
    Bytes(Vec<u8>),
}

/// Single normalized representation accepted by stores, sinks, and query layers.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngestLogEntry {
    /// Replay-safe producer identity.
    pub id: LogRecordId,
    /// Node wall-clock time at which the record entered observability.
    pub observed_at: Timestamp,
    /// Producer event time when parsed, otherwise `observed_at`.
    pub event_at: Timestamp,
    /// Normalized severity name; unknown producers default to `info`.
    pub severity: String,
    /// Source stream independent of runtime transport framing.
    pub stream: LogStream,
    /// Typed cluster, workload, system, or build ownership.
    pub origin: LogOrigin,
    /// Searchable body preserving invalid UTF-8 as bytes.
    pub body: LogBody,
    /// Parser- or OTLP-supplied structured fields in stable key order.
    #[serde(default, skip_serializing_if = "BTreeMap::is_empty")]
    pub attributes: BTreeMap<String, String>,
}
