use std::collections::{BTreeMap, BTreeSet};
use std::sync::{Arc, Weak};

use hickory_server::proto::rr::{LowerName, Name};
use kernel_api::{DnsRecord, DnsRecordValue};
use serde::Serialize;
use tokio::sync::RwLock;

use crate::{DnsResolverPlugin, DnsResolverPluginError};

/// Authoritative zone served by every Linux node resolver.
pub const MAESTRO_DNS_ZONE: &str = "maestro.internal.";
const MAX_TXT_BYTES: usize = 255;

/// Supported authoritative query types.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DnsQueryType {
    /// IPv4 address records.
    A,
    /// IPv6 address records.
    Aaaa,
    /// Canonical-name aliases.
    Cname,
    /// Text records.
    Txt,
    /// Service location records.
    Srv,
    /// Every record set at a name.
    Any,
    /// A syntactically valid type not published by Maestro.
    Other(u16),
}

/// Response classification produced without recursive lookup.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub enum DnsResponseCode {
    /// The name and selected record set were processed successfully.
    NoError,
    /// The queried name does not exist inside the authoritative zone.
    NameError,
    /// The query is outside the zone and recursion is unavailable.
    Refused,
}

/// One deterministic answer returned from the compiled zone.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DnsAnswer {
    /// Lowercase, fully qualified owner name.
    pub name: String,
    /// Cache lifetime from the source record set.
    pub ttl_secs: u32,
    /// Typed record value with normalized DNS targets.
    pub value: DnsRecordValue,
}

/// Result of one in-process authoritative lookup.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct DnsLookup {
    /// Whether Maestro owns the queried name's zone.
    pub authoritative: bool,
    /// DNS response classification.
    pub response_code: DnsResponseCode,
    /// Ordered records returned in the answer section.
    pub answers: Vec<DnsAnswer>,
}

/// Size of one successfully compiled immutable zone.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct DnsZoneSummary {
    /// Distinct `(name, type)` record sets.
    pub record_sets: usize,
    /// Individual records across all sets.
    pub records: usize,
}

/// Store-independent resolver backed by immutable full snapshots.
#[derive(Clone)]
pub struct AuthoritativeDnsResolver {
    zone: Arc<RwLock<Arc<CompiledZone>>>,
    plugin: Option<Arc<dyn DnsResolverPlugin>>,
}

impl AuthoritativeDnsResolver {
    /// Constructs an empty authoritative `maestro.internal.` zone.
    pub fn new() -> Result<Self, DnsResolverError> {
        Ok(Self {
            zone: Arc::new(RwLock::new(Arc::new(CompiledZone::empty()?))),
            plugin: None,
        })
    }

    /// Returns a non-owning view used by plugins to discover local service endpoints.
    pub fn zone_reader(&self) -> DnsZoneReader {
        DnsZoneReader {
            zone: Arc::downgrade(&self.zone),
        }
    }

    /// Attaches one optional lookup path for names absent from the local zone snapshot.
    pub fn with_plugin(mut self, plugin: Arc<dyn DnsResolverPlugin>) -> Self {
        self.plugin = Some(plugin);
        self
    }

    /// Validates a complete resource snapshot and replaces the live zone atomically.
    pub async fn replace(
        &self,
        resources: &[DnsRecord],
    ) -> Result<DnsZoneSummary, DnsResolverError> {
        let compiled = CompiledZone::compile(resources)?;
        let summary = compiled.summary;
        *self.zone.write().await = Arc::new(compiled);
        Ok(summary)
    }

    /// Answers one query from the current immutable snapshot without store access.
    pub async fn lookup(
        &self,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<DnsLookup, DnsResolverError> {
        let name =
            parse_fully_qualified(name).map_err(|message| DnsResolverError::InvalidQueryName {
                name: name.to_owned(),
                message,
            })?;
        let name = LowerName::new(&name);
        let local = self.zone.read().await.clone().lookup(&name, query_type);
        if local.response_code != DnsResponseCode::NameError {
            return Ok(local);
        }
        let Some(plugin) = &self.plugin else {
            return Ok(local);
        };
        plugin
            .lookup(&name.to_string(), query_type)
            .await
            .map(|lookup| lookup.unwrap_or(local))
            .map_err(DnsResolverError::Plugin)
    }
}

/// Non-owning authoritative snapshot view available to resolver plugins.
#[derive(Clone)]
pub struct DnsZoneReader {
    zone: Weak<RwLock<Arc<CompiledZone>>>,
}

impl DnsZoneReader {
    /// Reads only the current local zone and never invokes another plugin.
    pub async fn lookup(
        &self,
        name: &str,
        query_type: DnsQueryType,
    ) -> Result<DnsLookup, DnsResolverPluginError> {
        let name = parse_fully_qualified(name).map_err(|message| {
            DnsResolverPluginError::new(format!("invalid local name: {message}"))
        })?;
        let zone = self
            .zone
            .upgrade()
            .ok_or_else(|| DnsResolverPluginError::new("authoritative zone is unavailable"))?;
        Ok(zone
            .read()
            .await
            .clone()
            .lookup(&LowerName::new(&name), query_type))
    }
}

struct CompiledZone {
    origin: LowerName,
    record_sets: BTreeMap<LowerName, BTreeMap<RecordKind, Vec<DnsAnswer>>>,
    summary: DnsZoneSummary,
}

impl CompiledZone {
    fn empty() -> Result<Self, DnsResolverError> {
        let origin = parse_fully_qualified(MAESTRO_DNS_ZONE).map_err(|message| {
            DnsResolverError::InvalidZone {
                zone: MAESTRO_DNS_ZONE.to_owned(),
                message,
            }
        })?;
        Ok(Self {
            origin: LowerName::new(&origin),
            record_sets: BTreeMap::new(),
            summary: DnsZoneSummary::default(),
        })
    }

    fn compile(resources: &[DnsRecord]) -> Result<Self, DnsResolverError> {
        let mut zone = Self::empty()?;
        for resource in resources
            .iter()
            .filter(|resource| resource.meta.deletion_timestamp.is_none())
        {
            zone.insert(resource)?;
        }
        zone.validate_aliases()?;
        zone.summary = DnsZoneSummary {
            record_sets: zone.record_sets.values().map(BTreeMap::len).sum(),
            records: zone
                .record_sets
                .values()
                .flat_map(BTreeMap::values)
                .map(Vec::len)
                .sum(),
        };
        Ok(zone)
    }

    fn insert(&mut self, resource: &DnsRecord) -> Result<(), DnsResolverError> {
        let resource_id = resource.meta.id.as_str().to_owned();
        let name = parse_fully_qualified(&resource.spec.name).map_err(|message| {
            DnsResolverError::InvalidRecordName {
                resource_id: resource_id.clone(),
                name: resource.spec.name.clone(),
                message,
            }
        })?;
        let name = LowerName::new(&name);
        if !self.origin.zone_of(&name) {
            return Err(DnsResolverError::OutsideZone {
                resource_id,
                name: name.to_string(),
            });
        }
        let Some(first) = resource.spec.values.first() else {
            return Err(DnsResolverError::EmptyRecordSet { resource_id });
        };
        let kind = RecordKind::of(first);
        if resource
            .spec
            .values
            .iter()
            .any(|value| RecordKind::of(value) != kind)
        {
            return Err(DnsResolverError::MixedRecordSet { resource_id });
        }
        if kind == RecordKind::Cname && resource.spec.values.len() != 1 {
            return Err(DnsResolverError::MultipleAliases { resource_id });
        }
        let mut answers = resource
            .spec
            .values
            .iter()
            .map(|value| normalize_answer(&resource_id, &name, resource.spec.ttl_secs, value))
            .collect::<Result<Vec<_>, _>>()?;
        answers.sort_by_key(answer_sort_key);

        let sets = self.record_sets.entry(name.clone()).or_default();
        if sets.contains_key(&kind) {
            return Err(DnsResolverError::DuplicateRecordSet {
                resource_id,
                name: name.to_string(),
                record_type: kind.label(),
            });
        }
        if (kind == RecordKind::Cname && !sets.is_empty()) || sets.contains_key(&RecordKind::Cname)
        {
            return Err(DnsResolverError::AliasDataConflict {
                resource_id,
                name: name.to_string(),
            });
        }
        sets.insert(kind, answers);
        Ok(())
    }

    fn validate_aliases(&self) -> Result<(), DnsResolverError> {
        for (name, sets) in &self.record_sets {
            if !sets.contains_key(&RecordKind::Cname) {
                continue;
            }
            let mut visited = BTreeSet::new();
            let mut current = name.clone();
            while let Some(target) = self.alias_target(&current) {
                if !visited.insert(current.clone()) {
                    return Err(DnsResolverError::AliasCycle {
                        name: name.to_string(),
                    });
                }
                current = target;
            }
        }
        Ok(())
    }

    fn lookup(&self, name: &LowerName, query_type: DnsQueryType) -> DnsLookup {
        if !self.origin.zone_of(name) {
            return DnsLookup {
                authoritative: false,
                response_code: DnsResponseCode::Refused,
                answers: Vec::new(),
            };
        }
        let Some(sets) = self.record_sets.get(name) else {
            return DnsLookup {
                authoritative: true,
                response_code: if name == &self.origin {
                    DnsResponseCode::NoError
                } else {
                    DnsResponseCode::NameError
                },
                answers: Vec::new(),
            };
        };
        let answers = match RecordKind::from_query(query_type) {
            None if query_type == DnsQueryType::Any => sets.values().flatten().cloned().collect(),
            Some(kind) if sets.contains_key(&kind) => {
                sets.get(&kind).into_iter().flatten().cloned().collect()
            }
            Some(kind) if sets.contains_key(&RecordKind::Cname) => self.resolve_alias(name, kind),
            None if sets.contains_key(&RecordKind::Cname) => sets
                .get(&RecordKind::Cname)
                .into_iter()
                .flatten()
                .cloned()
                .collect(),
            _ => Vec::new(),
        };
        DnsLookup {
            authoritative: true,
            response_code: DnsResponseCode::NoError,
            answers,
        }
    }

    fn resolve_alias(&self, name: &LowerName, requested: RecordKind) -> Vec<DnsAnswer> {
        let mut answers = Vec::new();
        let mut current = name.clone();
        let mut visited = BTreeSet::new();
        while visited.insert(current.clone()) {
            let Some(sets) = self.record_sets.get(&current) else {
                break;
            };
            if let Some(records) = sets.get(&requested) {
                answers.extend(records.iter().cloned());
                break;
            }
            let Some(aliases) = sets.get(&RecordKind::Cname) else {
                break;
            };
            answers.extend(aliases.iter().cloned());
            let Some(target) = self.alias_target(&current) else {
                break;
            };
            current = target;
        }
        answers
    }

    fn alias_target(&self, name: &LowerName) -> Option<LowerName> {
        let value = &self
            .record_sets
            .get(name)?
            .get(&RecordKind::Cname)?
            .first()?
            .value;
        let DnsRecordValue::Cname(target) = value else {
            return None;
        };
        parse_fully_qualified(target)
            .ok()
            .map(|target| LowerName::new(&target))
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
enum RecordKind {
    A,
    Aaaa,
    Cname,
    Txt,
    Srv,
}

impl RecordKind {
    const fn of(value: &DnsRecordValue) -> Self {
        match value {
            DnsRecordValue::A(_) => Self::A,
            DnsRecordValue::Aaaa(_) => Self::Aaaa,
            DnsRecordValue::Cname(_) => Self::Cname,
            DnsRecordValue::Txt(_) => Self::Txt,
            DnsRecordValue::Srv { .. } => Self::Srv,
        }
    }

    const fn from_query(query_type: DnsQueryType) -> Option<Self> {
        match query_type {
            DnsQueryType::A => Some(Self::A),
            DnsQueryType::Aaaa => Some(Self::Aaaa),
            DnsQueryType::Cname => Some(Self::Cname),
            DnsQueryType::Txt => Some(Self::Txt),
            DnsQueryType::Srv => Some(Self::Srv),
            DnsQueryType::Any | DnsQueryType::Other(_) => None,
        }
    }

    const fn label(self) -> &'static str {
        match self {
            Self::A => "A",
            Self::Aaaa => "AAAA",
            Self::Cname => "CNAME",
            Self::Txt => "TXT",
            Self::Srv => "SRV",
        }
    }
}

fn normalize_answer(
    resource_id: &str,
    name: &LowerName,
    ttl_secs: u32,
    value: &DnsRecordValue,
) -> Result<DnsAnswer, DnsResolverError> {
    let value = match value {
        DnsRecordValue::Cname(target) => {
            DnsRecordValue::Cname(normalize_target(resource_id, "CNAME", target)?)
        }
        DnsRecordValue::Txt(text) if text.len() > MAX_TXT_BYTES => {
            return Err(DnsResolverError::TextTooLong {
                resource_id: resource_id.to_owned(),
                bytes: text.len(),
            });
        }
        DnsRecordValue::Srv {
            priority,
            weight,
            port,
            target,
        } => DnsRecordValue::Srv {
            priority: *priority,
            weight: *weight,
            port: *port,
            target: normalize_target(resource_id, "SRV", target)?,
        },
        value => value.clone(),
    };
    Ok(DnsAnswer {
        name: name.to_string(),
        ttl_secs,
        value,
    })
}

fn normalize_target(
    resource_id: &str,
    record_type: &'static str,
    target: &str,
) -> Result<String, DnsResolverError> {
    parse_fully_qualified(target)
        .map(|name| LowerName::new(&name).to_string())
        .map_err(|message| DnsResolverError::InvalidTarget {
            resource_id: resource_id.to_owned(),
            record_type,
            target: target.to_owned(),
            message,
        })
}

fn parse_fully_qualified(value: &str) -> Result<Name, String> {
    let name = Name::from_ascii(value).map_err(|error| error.to_string())?;
    if name.is_fqdn() {
        Ok(name)
    } else {
        Err("name must end with the DNS root label".to_owned())
    }
}

fn answer_sort_key(answer: &DnsAnswer) -> String {
    match &answer.value {
        DnsRecordValue::A(address) => format!("a:{address}"),
        DnsRecordValue::Aaaa(address) => format!("aaaa:{address}"),
        DnsRecordValue::Cname(target) => format!("cname:{target}"),
        DnsRecordValue::Txt(text) => format!("txt:{text}"),
        DnsRecordValue::Srv {
            priority,
            weight,
            port,
            target,
        } => format!("srv:{priority:05}:{weight:05}:{port:05}:{target}"),
    }
}

/// Invalid resource snapshot or in-process query.
#[derive(Debug, thiserror::Error)]
pub enum DnsResolverError {
    /// The compile-time authoritative origin was invalid.
    #[error("invalid authoritative DNS zone `{zone}`: {message}")]
    InvalidZone { zone: String, message: String },
    /// A query name was malformed or relative.
    #[error("invalid DNS query name `{name}`: {message}")]
    InvalidQueryName { name: String, message: String },
    /// A resource owner name was malformed or relative.
    #[error("DnsRecord `{resource_id}` has invalid name `{name}`: {message}")]
    InvalidRecordName {
        resource_id: String,
        name: String,
        message: String,
    },
    /// A resource attempted to publish outside Maestro's zone.
    #[error("DnsRecord `{resource_id}` name `{name}` is outside `{MAESTRO_DNS_ZONE}`")]
    OutsideZone { resource_id: String, name: String },
    /// A record set contained no values.
    #[error("DnsRecord `{resource_id}` has an empty record set")]
    EmptyRecordSet { resource_id: String },
    /// Values within one record set had different DNS types.
    #[error("DnsRecord `{resource_id}` mixes record types")]
    MixedRecordSet { resource_id: String },
    /// More than one CNAME was assigned to one owner.
    #[error("DnsRecord `{resource_id}` contains more than one CNAME")]
    MultipleAliases { resource_id: String },
    /// Two resources published the same owner and record type.
    #[error("DnsRecord `{resource_id}` duplicates {record_type} set at `{name}`")]
    DuplicateRecordSet {
        resource_id: String,
        name: String,
        record_type: &'static str,
    },
    /// A CNAME owner also had another record type.
    #[error("DnsRecord `{resource_id}` makes CNAME owner `{name}` contain other data")]
    AliasDataConflict { resource_id: String, name: String },
    /// A CNAME chain cycles within the authoritative snapshot.
    #[error("CNAME chain beginning at `{name}` contains a cycle")]
    AliasCycle { name: String },
    /// A CNAME or SRV target was malformed or relative.
    #[error("DnsRecord `{resource_id}` has invalid {record_type} target `{target}`: {message}")]
    InvalidTarget {
        resource_id: String,
        record_type: &'static str,
        target: String,
        message: String,
    },
    /// A single DNS TXT character string exceeded its wire limit.
    #[error("DnsRecord `{resource_id}` TXT value is {bytes} bytes; maximum is {MAX_TXT_BYTES}")]
    TextTooLong { resource_id: String, bytes: usize },
    /// An optional resolver plugin failed while handling a routed query.
    #[error("DNS resolver plugin failed: {0}")]
    Plugin(#[from] DnsResolverPluginError),
}
