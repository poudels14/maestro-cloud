use async_trait::async_trait;
use kernel_api::{ServiceId, Timestamp};
use serde::{Deserialize, Serialize};

mod projection;

pub use projection::{
    merge_ingress_traffic, merge_service_traffic, project_ingress_traffic, project_service_traffic,
};

/// Fixed access-log aggregation interval matching the retired Traefik scraper cadence.
pub const TRAFFIC_BUCKET_MS: i64 = 5_000;
/// Largest per-dimension traffic breakdown accepted by a node.
pub const MAXIMUM_TRAFFIC_BREAKDOWN_LIMIT: usize = 500;
/// Largest service traffic page accepted by a node.
pub const MAXIMUM_TRAFFIC_METRIC_LIMIT: usize = 10_000;

const MAXIMUM_ROUTER_PREFIX_BYTES: usize = 256;

/// One status-code slice for a ranked IP address or request path.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficBreakdownEntry {
    /// IP address or sanitized path selected by the requested dimension.
    pub value: String,
    /// Downstream HTTP status.
    pub status_code: u16,
    /// Requests observed in the range.
    pub requests: u64,
    /// Latest matching access-log event time.
    pub last_seen_at_ms: i64,
}

/// Ranked access-log traffic grouped independently by client IP and path.
#[derive(Debug, Clone, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct IngressTrafficBreakdown {
    /// Top client addresses, with one row per status code.
    pub by_ip: Vec<TrafficBreakdownEntry>,
    /// Top sanitized paths, with one row per status code.
    pub by_path: Vec<TrafficBreakdownEntry>,
}

/// API-compatible service traffic interval derived from access logs.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct TrafficMetricPoint {
    /// Start of the fixed five-second interval.
    pub ts: i64,
    /// Service selected by the API scope.
    pub service_id: String,
    /// Deployment identity when the access-log backend can be resolved durably.
    pub deployment_id: Option<String>,
    /// Downstream HTTP status.
    pub status_code: u16,
    /// Normalized request method.
    pub method: String,
    /// Requests in this interval.
    pub requests: i64,
    /// Request body bytes in this interval.
    pub bytes_in: i64,
    /// Response body bytes in this interval.
    pub bytes_out: i64,
    /// Requests completed within one second.
    pub lat_le_1s: i64,
    /// Requests completed within five seconds.
    pub lat_le_5s: i64,
    /// Requests completed within ten seconds.
    pub lat_le_10s: i64,
    /// Requests carrying a valid duration.
    pub lat_total: i64,
}

/// Trusted router namespace selected for one breakdown query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum IngressTrafficScope {
    /// All service routers except the internal blocklist namespace.
    Cluster { blocked_router_prefix: String },
    /// Only internal blocklist routers.
    Blocked { router_prefix: String },
    /// Only routers owned by one service.
    Service { router_prefix: String },
}

impl IngressTrafficScope {
    fn prefix(&self) -> &str {
        match self {
            Self::Cluster {
                blocked_router_prefix,
            } => blocked_router_prefix,
            Self::Blocked { router_prefix } | Self::Service { router_prefix } => router_prefix,
        }
    }

    fn matches(&self, router: &str) -> bool {
        match self {
            Self::Cluster {
                blocked_router_prefix,
            } => !router.starts_with(blocked_router_prefix),
            Self::Blocked { router_prefix } | Self::Service { router_prefix } => {
                router.starts_with(router_prefix)
            }
        }
    }
}

/// Validated inclusive range and per-dimension ranking bound.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct IngressTrafficQuery {
    scope: IngressTrafficScope,
    from: Timestamp,
    to: Timestamp,
    limit: usize,
}

impl IngressTrafficQuery {
    /// Creates a bounded breakdown query.
    pub fn new(
        scope: IngressTrafficScope,
        from: Timestamp,
        to: Timestamp,
        limit: usize,
    ) -> Result<Self, TrafficQueryError> {
        validate_range(from, to)?;
        validate_prefix(scope.prefix())?;
        if !(1..=MAXIMUM_TRAFFIC_BREAKDOWN_LIMIT).contains(&limit) {
            return Err(rejected(format!(
                "traffic breakdown limit must be between 1 and {MAXIMUM_TRAFFIC_BREAKDOWN_LIMIT}"
            )));
        }
        Ok(Self {
            scope,
            from,
            to,
            limit,
        })
    }

    /// Returns the trusted router selection.
    pub fn scope(&self) -> &IngressTrafficScope {
        &self.scope
    }

    /// Returns the inclusive lower event-time bound.
    pub fn from(&self) -> Timestamp {
        self.from
    }

    /// Returns the inclusive upper event-time bound.
    pub fn to(&self) -> Timestamp {
        self.to
    }

    /// Returns the maximum ranked values in each dimension.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Validated service traffic time-series query.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceTrafficQuery {
    service_id: ServiceId,
    router_prefix: String,
    from: Timestamp,
    to: Timestamp,
    limit: usize,
}

impl ServiceTrafficQuery {
    /// Creates one inclusive access-log range for a trusted service router prefix.
    pub fn new(
        service_id: ServiceId,
        router_prefix: String,
        from: Timestamp,
        to: Timestamp,
        limit: usize,
    ) -> Result<Self, TrafficQueryError> {
        validate_range(from, to)?;
        validate_prefix(&router_prefix)?;
        if !(1..=MAXIMUM_TRAFFIC_METRIC_LIMIT).contains(&limit) {
            return Err(rejected(format!(
                "service traffic limit must be between 1 and {MAXIMUM_TRAFFIC_METRIC_LIMIT}"
            )));
        }
        Ok(Self {
            service_id,
            router_prefix,
            from,
            to,
            limit,
        })
    }

    /// Returns the selected service.
    pub fn service_id(&self) -> &ServiceId {
        &self.service_id
    }

    /// Returns the trusted Traefik router prefix for the service.
    pub fn router_prefix(&self) -> &str {
        &self.router_prefix
    }

    /// Returns the inclusive lower event-time bound.
    pub fn from(&self) -> Timestamp {
        self.from
    }

    /// Returns the inclusive upper event-time bound.
    pub fn to(&self) -> Timestamp {
        self.to
    }

    /// Returns the maximum interval rows.
    pub fn limit(&self) -> usize {
        self.limit
    }
}

/// Node-local access-log analytics boundary.
#[async_trait]
pub trait TrafficQueryStore: Send + Sync {
    /// Returns ranked client and path traffic.
    async fn query_ingress_traffic(
        &self,
        query: &IngressTrafficQuery,
    ) -> Result<IngressTrafficBreakdown, TrafficQueryError>;

    /// Returns five-second service intervals derived from the same access logs.
    async fn query_service_traffic(
        &self,
        query: &ServiceTrafficQuery,
    ) -> Result<Vec<TrafficMetricPoint>, TrafficQueryError>;
}

fn validate_range(from: Timestamp, to: Timestamp) -> Result<(), TrafficQueryError> {
    if from.0 > to.0 {
        return Err(rejected("traffic range must not be inverted"));
    }
    Ok(())
}

fn validate_prefix(prefix: &str) -> Result<(), TrafficQueryError> {
    if prefix.is_empty() {
        return Err(rejected("traffic router prefix must not be empty"));
    }
    if prefix.len() > MAXIMUM_ROUTER_PREFIX_BYTES {
        return Err(rejected(format!(
            "traffic router prefix exceeds {MAXIMUM_ROUTER_PREFIX_BYTES} bytes"
        )));
    }
    Ok(())
}

fn rejected(message: impl Into<String>) -> TrafficQueryError {
    TrafficQueryError::Rejected {
        message: message.into(),
    }
}

/// Invalid traffic query or unavailable access-log storage.
#[derive(Debug, thiserror::Error)]
pub enum TrafficQueryError {
    /// A query violates deterministic bounds or range rules.
    #[error("traffic query was rejected: {message}")]
    Rejected {
        /// Stable rejection detail.
        message: String,
    },
    /// The access-log store cannot serve a valid query.
    #[error("traffic query is unavailable: {message}")]
    Unavailable {
        /// Safe availability detail.
        message: String,
    },
}
