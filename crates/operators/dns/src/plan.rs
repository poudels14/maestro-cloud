use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{DnsRecord, DnsRecordId, ServiceId};

use crate::resource::{is_managed, managed_owner, new_record, replace_record};
use crate::target::desired_specs;
use crate::{DnsInput, DnsPlan};

/// Computes one deterministic service DNS resource generation.
pub fn plan(input: DnsInput) -> Result<DnsPlan, DnsPlanError> {
    if input.settings.ttl_secs == 0 {
        return Err(DnsPlanError::ZeroTtl);
    }
    let services = index(input.services, |service| service.meta.id.clone(), "Service")?;
    let assignments = index(
        input.assignments,
        |assignment| assignment.meta.id.clone(),
        "Assignment",
    )?;
    let replicas = index(
        input.replicas,
        |replica| replica.meta.id.clone(),
        "ReplicaState",
    )?;
    let records = index(input.records, |record| record.meta.id.clone(), "DnsRecord")?;
    let mut records_by_service = BTreeMap::<ServiceId, Vec<&DnsRecord>>::new();
    let mut orphaned = Vec::new();
    for record in records.values() {
        if let Some(owner) = managed_owner(record)? {
            if services.contains_key(&owner) {
                records_by_service.entry(owner).or_default().push(record);
            } else {
                orphaned.push(record.meta.id.clone());
            }
        }
    }

    let assignment_values = assignments.values().cloned().collect::<Vec<_>>();
    let replica_values = replicas.values().cloned().collect::<Vec<_>>();
    let mut output = DnsPlan::default();
    let mut deleted = orphaned.into_iter().collect::<BTreeSet<_>>();
    let mut desired_owners = BTreeMap::<DnsRecordId, ServiceId>::new();
    for service in services.values() {
        let existing = records_by_service
            .get(&service.meta.id)
            .cloned()
            .unwrap_or_default();
        let desired = if service.meta.deletion_timestamp.is_some() {
            Some(Vec::new())
        } else {
            desired_specs(
                &input.cluster_id,
                input.settings,
                service,
                &assignment_values,
                &replica_values,
            )?
        };
        let Some(specs) = desired else {
            continue;
        };
        let mut desired_ids = BTreeSet::new();
        for spec in specs {
            let desired = new_record(&input.cluster_id, service, spec)?;
            if desired_owners
                .insert(desired.meta.id.clone(), service.meta.id.clone())
                .is_some_and(|owner| owner != service.meta.id)
            {
                return Err(DnsPlanError::RecordIdentityCollision {
                    record_id: desired.meta.id,
                });
            }
            desired_ids.insert(desired.meta.id.clone());
            if let Some(current) = records.get(&desired.meta.id) {
                if !is_managed(current)
                    || managed_owner(current)?.as_ref() != Some(&service.meta.id)
                {
                    return Err(DnsPlanError::RecordIdentityCollision {
                        record_id: desired.meta.id,
                    });
                }
                if current.spec != desired.spec {
                    output
                        .replace_records
                        .push(replace_record(current, desired.spec)?);
                }
            } else {
                output.create_records.push(desired);
            }
        }
        deleted.extend(
            existing
                .into_iter()
                .filter(|record| !desired_ids.contains(&record.meta.id))
                .map(|record| record.meta.id.clone()),
        );
    }
    output.create_records.sort_by(record_order);
    output.replace_records.sort_by(record_order);
    output.delete_records = deleted.into_iter().collect();
    Ok(output)
}

fn index<Id, Resource>(
    resources: Vec<Resource>,
    id: impl Fn(&Resource) -> Id,
    kind: &'static str,
) -> Result<BTreeMap<Id, Resource>, DnsPlanError>
where
    Id: Ord + ToString,
{
    let mut indexed = BTreeMap::new();
    for resource in resources {
        let resource_id = id(&resource);
        let resource_name = resource_id.to_string();
        if indexed.insert(resource_id, resource).is_some() {
            return Err(DnsPlanError::DuplicateResource {
                kind,
                resource_id: resource_name,
            });
        }
    }
    Ok(indexed)
}

fn record_order(left: &DnsRecord, right: &DnsRecord) -> std::cmp::Ordering {
    left.spec
        .name
        .cmp(&right.spec.name)
        .then_with(|| left.meta.id.cmp(&right.meta.id))
}

/// Invalid DNS configuration or inconsistent orchestration snapshot.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DnsPlanError {
    /// DNS records with no cache lifetime are rejected by operator policy.
    #[error("DNS TTL must be greater than zero")]
    ZeroTtl,
    /// A generated authoritative name was not canonical and DNS-safe.
    #[error("invalid authoritative DNS name `{name}`: {message}")]
    InvalidDnsName { name: String, message: String },
    /// Two resources had the same typed identity in one input snapshot.
    #[error("{kind} `{resource_id}` occurs more than once in one DNS snapshot")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// Two current assignments claimed the same placement epoch and replica slot.
    #[error(
        "service `{service_id}` replica {replica_index} has multiple assignments at epoch {placement_epoch}"
    )]
    AmbiguousAssignment {
        service_id: ServiceId,
        replica_index: u32,
        placement_epoch: u64,
    },
    /// An operator-managed record did not have exactly one controlling Service owner.
    #[error("managed DnsRecord `{record_id}` has invalid Service ownership")]
    InvalidManagedOwnership { record_id: DnsRecordId },
    /// A desired deterministic record identity is occupied outside this operator's ownership.
    #[error("desired DnsRecord identity `{record_id}` is already owned by another writer")]
    RecordIdentityCollision { record_id: DnsRecordId },
    /// A record specification changed after its generation counter reached its limit.
    #[error("DnsRecord `{record_id}` exhausted its generation counter")]
    GenerationExhausted { record_id: DnsRecordId },
    /// A static or stored typed identity was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
}
