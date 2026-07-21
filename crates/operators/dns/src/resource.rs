use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    AnnotationKey, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, Generation, Object,
    ObjectMeta, OwnerReference, Ownership, ResourceId, ResourceKind, ResourceName, Service,
    ServiceId,
};
use sha2::{Digest, Sha256};

use crate::DnsPlanError;

const MANAGED_ANNOTATION: &str = "dns.maestro.dev/managed";
const HASH_DOMAIN: &[u8] = b"maestro-dns-record-v1\0";

pub(crate) fn is_managed(record: &DnsRecord) -> bool {
    record
        .meta
        .annotations
        .get(&managed_annotation())
        .is_some_and(|value| value == "true")
}

pub(crate) fn managed_owner(record: &DnsRecord) -> Result<Option<ServiceId>, DnsPlanError> {
    if !is_managed(record) {
        return Ok(None);
    }
    let mut owners = record.meta.owner_refs.iter().filter(|owner| {
        owner.ownership == Ownership::Controller && owner.resource.kind.as_str() == "Service"
    });
    let owner = owners
        .next()
        .ok_or_else(|| DnsPlanError::InvalidManagedOwnership {
            record_id: record.meta.id.clone(),
        })?;
    if owners.next().is_some() || record.meta.owner_refs.len() != 1 {
        return Err(DnsPlanError::InvalidManagedOwnership {
            record_id: record.meta.id.clone(),
        });
    }
    ServiceId::new(owner.resource.id.as_str())
        .map(Some)
        .map_err(DnsPlanError::InvalidIdentifier)
}

pub(crate) fn new_record(
    cluster_id: &kernel_api::ClusterId,
    service: &Service,
    spec: DnsRecordSpec,
) -> Result<DnsRecord, DnsPlanError> {
    Ok(Object {
        meta: ObjectMeta {
            id: record_id(cluster_id, &spec)?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::from([(managed_annotation(), "true".to_string())]),
            revision: Default::default(),
            generation: Generation(1),
            owner_refs: vec![OwnerReference {
                resource: ResourceId::new(
                    ResourceKind::new("Service")?,
                    ResourceName::from(service.meta.id.clone()),
                ),
                ownership: Ownership::Controller,
            }],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec,
        status: DnsRecordStatus {
            applied_generation: Generation::default(),
            published_nodes: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

pub(crate) fn replace_record(
    current: &DnsRecord,
    spec: DnsRecordSpec,
) -> Result<DnsRecord, DnsPlanError> {
    let mut replacement = current.clone();
    replacement.meta.generation =
        Generation(current.meta.generation.0.checked_add(1).ok_or_else(|| {
            DnsPlanError::GenerationExhausted {
                record_id: current.meta.id.clone(),
            }
        })?);
    replacement.spec = spec;
    replacement.status.published_nodes.clear();
    replacement.status.conditions.clear();
    Ok(replacement)
}

fn record_id(
    cluster_id: &kernel_api::ClusterId,
    spec: &DnsRecordSpec,
) -> Result<DnsRecordId, kernel_api::InvalidIdentifier> {
    let mut hash = Sha256::new();
    hash.update(HASH_DOMAIN);
    hash.update(cluster_id.as_str().as_bytes());
    hash.update([0]);
    hash.update(spec.name.as_bytes());
    hash.update([0]);
    hash.update(record_kind(spec));
    let suffix = hash
        .finalize()
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    DnsRecordId::new(format!("dns-{suffix}"))
}

fn record_kind(spec: &DnsRecordSpec) -> &'static [u8] {
    match spec.values.first() {
        Some(kernel_api::DnsRecordValue::A(_)) => b"a",
        Some(kernel_api::DnsRecordValue::Aaaa(_)) => b"aaaa",
        Some(kernel_api::DnsRecordValue::Cname(_)) => b"cname",
        Some(kernel_api::DnsRecordValue::Txt(_)) => b"txt",
        Some(kernel_api::DnsRecordValue::Srv { .. }) => b"srv",
        None => b"empty",
    }
}

fn managed_annotation() -> AnnotationKey {
    AnnotationKey(MANAGED_ANNOTATION.to_string())
}
