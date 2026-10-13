use std::fmt;

use serde::{Deserialize, Serialize};
use thiserror::Error;

const KIB: u64 = 1024;
const MIB: u64 = KIB * 1024;
const GIB: u64 = MIB * 1024;
const TIB: u64 = GIB * 1024;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "f64", into = "f64")]
pub struct Millicores(u32);

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(try_from = "RawQuantity", into = "String")]
pub struct ByteSize(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(try_from = "RawQuantity", into = "String")]
pub enum MemoryLimit {
    Size(ByteSize),
    PercentOfNode(u8),
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceRequest {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Millicores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<ByteSize>,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceSpec {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<Millicores>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub memory: Option<MemoryLimit>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub request: Option<ResourceRequest>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResourceDefaults {
    pub cpu: Millicores,
    pub memory: MemoryLimit,
    pub request: ResolvedRequest,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub struct ResolvedRequest {
    pub cpu: Millicores,
    pub memory: ByteSize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct NodeCapacity {
    pub cpu: Millicores,
    pub memory: ByteSize,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct EffectiveResources {
    pub cpu_limit: Millicores,
    pub memory_limit: ByteSize,
    pub request: ResolvedRequest,
}

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub enum OomPriority {
    Low,
    #[default]
    Normal,
    High,
}

#[derive(Debug, Error, PartialEq)]
pub enum ResourceError {
    #[error("invalid cpu value {0}: must be a positive number with at most 3 decimals")]
    InvalidCpu(f64),
    #[error("invalid memory value {0:?}: expected bytes or a size like 512Mi, 4Gi, or 50%")]
    InvalidMemory(String),
    #[error("cpu request {request} exceeds cpu limit {limit}")]
    CpuRequestAboveLimit {
        request: Millicores,
        limit: Millicores,
    },
    #[error("memory request {request} exceeds memory limit {limit}")]
    MemoryRequestAboveLimit { request: ByteSize, limit: ByteSize },
}

#[derive(Deserialize)]
#[serde(untagged)]
enum RawQuantity {
    Number(u64),
    Text(String),
}

impl ResourceSpec {
    pub fn resolve(
        &self,
        defaults: &ResourceDefaults,
        capacity: &NodeCapacity,
    ) -> Result<EffectiveResources, ResourceError> {
        let cpu_limit = self.cpu.unwrap_or(defaults.cpu);
        let memory_limit = self
            .memory
            .unwrap_or(defaults.memory)
            .to_bytes(capacity.memory);
        let explicit_request = self.request.clone().unwrap_or_default();
        let request = ResolvedRequest {
            cpu: explicit_request
                .cpu
                .unwrap_or(defaults.request.cpu)
                .min(cpu_limit),
            memory: explicit_request
                .memory
                .unwrap_or(defaults.request.memory)
                .min(memory_limit),
        };
        let resources = EffectiveResources {
            cpu_limit,
            memory_limit,
            request,
        };
        match (explicit_request.cpu, explicit_request.memory) {
            (Some(cpu), _) if cpu > cpu_limit => Err(ResourceError::CpuRequestAboveLimit {
                request: cpu,
                limit: cpu_limit,
            }),
            (_, Some(memory)) if memory > memory_limit => {
                Err(ResourceError::MemoryRequestAboveLimit {
                    request: memory,
                    limit: memory_limit,
                })
            }
            _ => Ok(resources),
        }
    }
}

impl Default for ResourceDefaults {
    fn default() -> Self {
        Self {
            cpu: Millicores(4000),
            memory: MemoryLimit::PercentOfNode(50),
            request: ResolvedRequest {
                cpu: Millicores(250),
                memory: ByteSize(512 * MIB),
            },
        }
    }
}

impl Millicores {
    pub const fn new(millicores: u32) -> Self {
        Self(millicores)
    }

    pub fn get(self) -> u32 {
        self.0
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

impl TryFrom<f64> for Millicores {
    type Error = ResourceError;

    fn try_from(cores: f64) -> Result<Self, Self::Error> {
        let millicores = (cores * 1000.0).round();
        let is_valid = cores.is_finite()
            && millicores >= 1.0
            && (cores * 1000.0 - millicores).abs() < 1e-6
            && millicores <= u32::MAX as f64;
        match is_valid {
            true => Ok(Self(millicores as u32)),
            false => Err(ResourceError::InvalidCpu(cores)),
        }
    }
}

impl From<Millicores> for f64 {
    fn from(value: Millicores) -> Self {
        value.0 as f64 / 1000.0
    }
}

impl fmt::Display for Millicores {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(formatter, "{}", f64::from(*self))
    }
}

impl ByteSize {
    pub const fn new(bytes: u64) -> Self {
        Self(bytes)
    }

    pub fn bytes(self) -> u64 {
        self.0
    }

    pub fn saturating_sub(self, other: Self) -> Self {
        Self(self.0.saturating_sub(other.0))
    }
}

impl std::str::FromStr for ByteSize {
    type Err = ResourceError;

    fn from_str(text: &str) -> Result<Self, Self::Err> {
        let trimmed = text.trim();
        let digits_end = trimmed
            .find(|character: char| !character.is_ascii_digit())
            .unwrap_or(trimmed.len());
        let (number, unit) = trimmed.split_at(digits_end);
        let multiplier = match unit {
            "" | "B" => Some(1),
            "K" | "KB" => Some(1000),
            "M" | "MB" => Some(1000_u64.pow(2)),
            "G" | "GB" => Some(1000_u64.pow(3)),
            "T" | "TB" => Some(1000_u64.pow(4)),
            "Ki" | "KiB" => Some(KIB),
            "Mi" | "MiB" => Some(MIB),
            "Gi" | "GiB" => Some(GIB),
            "Ti" | "TiB" => Some(TIB),
            _ => None,
        };
        number
            .parse::<u64>()
            .ok()
            .zip(multiplier)
            .and_then(|(value, multiplier)| value.checked_mul(multiplier))
            .filter(|bytes| *bytes > 0)
            .map(Self)
            .ok_or_else(|| ResourceError::InvalidMemory(text.to_string()))
    }
}

impl TryFrom<RawQuantity> for ByteSize {
    type Error = ResourceError;

    fn try_from(raw: RawQuantity) -> Result<Self, Self::Error> {
        match raw {
            RawQuantity::Number(0) => Err(ResourceError::InvalidMemory("0".into())),
            RawQuantity::Number(bytes) => Ok(Self(bytes)),
            RawQuantity::Text(text) => text.parse(),
        }
    }
}

impl From<ByteSize> for String {
    fn from(value: ByteSize) -> Self {
        value.to_string()
    }
}

impl fmt::Display for ByteSize {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        let unit = [(TIB, "Ti"), (GIB, "Gi"), (MIB, "Mi"), (KIB, "Ki")]
            .into_iter()
            .find(|(size, _)| self.0.is_multiple_of(*size));
        match unit {
            Some((size, suffix)) => write!(formatter, "{}{suffix}", self.0 / size),
            None => write!(formatter, "{}", self.0),
        }
    }
}

impl MemoryLimit {
    pub fn to_bytes(self, node_memory: ByteSize) -> ByteSize {
        match self {
            Self::Size(size) => size,
            Self::PercentOfNode(percent) => {
                ByteSize((node_memory.0 as u128 * percent as u128 / 100) as u64)
            }
        }
    }
}

impl TryFrom<RawQuantity> for MemoryLimit {
    type Error = ResourceError;

    fn try_from(raw: RawQuantity) -> Result<Self, Self::Error> {
        match raw {
            RawQuantity::Text(text) if text.trim().ends_with('%') => text
                .trim()
                .trim_end_matches('%')
                .parse::<u8>()
                .ok()
                .filter(|percent| (1..=100).contains(percent))
                .map(Self::PercentOfNode)
                .ok_or(ResourceError::InvalidMemory(text)),
            other => ByteSize::try_from(other).map(Self::Size),
        }
    }
}

impl From<MemoryLimit> for String {
    fn from(value: MemoryLimit) -> Self {
        match value {
            MemoryLimit::Size(size) => size.to_string(),
            MemoryLimit::PercentOfNode(percent) => format!("{percent}%"),
        }
    }
}

impl OomPriority {
    pub fn oom_score_adj(self) -> i32 {
        match self {
            Self::Low => 500,
            Self::Normal => 0,
            Self::High => -500,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn capacity() -> NodeCapacity {
        NodeCapacity {
            cpu: Millicores::new(16_000),
            memory: ByteSize::new(64 * GIB),
        }
    }

    #[test]
    fn parses_memory_units() {
        assert_eq!("512Mi".parse::<ByteSize>(), Ok(ByteSize::new(512 * MIB)));
        assert_eq!("4Gi".parse::<ByteSize>(), Ok(ByteSize::new(4 * GIB)));
        assert_eq!("2G".parse::<ByteSize>(), Ok(ByteSize::new(2_000_000_000)));
        assert!("4.5Gi".parse::<ByteSize>().is_err());
        assert!("0Mi".parse::<ByteSize>().is_err());
        assert!("lots".parse::<ByteSize>().is_err());
    }

    #[test]
    fn deserializes_spec_from_json() {
        let spec: ResourceSpec = serde_json::from_str(
            r#"{ "cpu": 0.5, "memory": "25%", "request": { "cpu": 0.25, "memory": 1073741824 } }"#,
        )
        .unwrap();
        assert_eq!(spec.cpu, Some(Millicores::new(500)));
        assert_eq!(spec.memory, Some(MemoryLimit::PercentOfNode(25)));
        assert_eq!(spec.request.unwrap().memory, Some(ByteSize::new(GIB)));
        assert!(serde_json::from_str::<ResourceSpec>(r#"{ "cpu": 0.0001 }"#).is_err());
        assert!(serde_json::from_str::<ResourceSpec>(r#"{ "memory": "150%" }"#).is_err());
    }

    #[test]
    fn resolves_defaults_against_node_capacity() {
        let resolved = ResourceSpec::default()
            .resolve(&ResourceDefaults::default(), &capacity())
            .unwrap();
        assert_eq!(resolved.cpu_limit, Millicores::new(4000));
        assert_eq!(resolved.memory_limit, ByteSize::new(32 * GIB));
        assert_eq!(resolved.request.cpu, Millicores::new(250));
        assert_eq!(resolved.request.memory, ByteSize::new(512 * MIB));
    }

    #[test]
    fn clamps_default_request_to_explicit_limit() {
        let spec = ResourceSpec {
            cpu: Some(Millicores::new(100)),
            memory: Some(MemoryLimit::Size(ByteSize::new(256 * MIB))),
            request: None,
        };
        let resolved = spec
            .resolve(&ResourceDefaults::default(), &capacity())
            .unwrap();
        assert_eq!(resolved.request.cpu, Millicores::new(100));
        assert_eq!(resolved.request.memory, ByteSize::new(256 * MIB));
    }

    #[test]
    fn rejects_explicit_request_above_limit() {
        let spec = ResourceSpec {
            cpu: Some(Millicores::new(1000)),
            memory: None,
            request: Some(ResourceRequest {
                cpu: Some(Millicores::new(2000)),
                memory: None,
            }),
        };
        assert!(matches!(
            spec.resolve(&ResourceDefaults::default(), &capacity()),
            Err(ResourceError::CpuRequestAboveLimit { .. })
        ));
    }

    #[test]
    fn round_trips_through_json() {
        let spec = ResourceSpec {
            cpu: Some(Millicores::new(2500)),
            memory: Some(MemoryLimit::Size(ByteSize::new(8 * GIB))),
            request: None,
        };
        let json = serde_json::to_string(&spec).unwrap();
        assert_eq!(json, r#"{"cpu":2.5,"memory":"8Gi"}"#);
        assert_eq!(serde_json::from_str::<ResourceSpec>(&json).unwrap(), spec);
    }
}
