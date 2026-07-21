use std::collections::BTreeMap;
use std::net::IpAddr;

use kernel_api::{
    Assignment, DeploymentPhase, DnsRecordSpec, DnsRecordValue, ReplicaState, Service,
};

use crate::validation::{replica_name, service_name};
use crate::{DnsPlanError, DnsSettings};

pub(crate) fn desired_specs(
    cluster_id: &kernel_api::ClusterId,
    settings: DnsSettings,
    service: &Service,
    assignments: &[Assignment],
    replicas: &[ReplicaState],
) -> Result<Option<Vec<DnsRecordSpec>>, DnsPlanError> {
    let Some(deployment_id) = service.status.active_deployment_id.as_ref() else {
        return Ok(Some(Vec::new()));
    };
    let count = service
        .status
        .replica_override
        .unwrap_or(service.spec.replicas);
    let mut slots: BTreeMap<u32, &Assignment> = BTreeMap::new();
    for assignment in assignments.iter().filter(|assignment| {
        assignment.meta.deletion_timestamp.is_none()
            && assignment.spec.service_id == service.meta.id
            && assignment.spec.deployment_id == *deployment_id
            && assignment.spec.replica_index < count
    }) {
        match slots.get(&assignment.spec.replica_index) {
            Some(current)
                if current.spec.placement_epoch == assignment.spec.placement_epoch
                    && current.meta.id != assignment.meta.id =>
            {
                return Err(DnsPlanError::AmbiguousAssignment {
                    service_id: service.meta.id.clone(),
                    replica_index: assignment.spec.replica_index,
                    placement_epoch: assignment.spec.placement_epoch,
                });
            }
            Some(current) if current.spec.placement_epoch > assignment.spec.placement_epoch => {}
            _ => {
                slots.insert(assignment.spec.replica_index, assignment);
            }
        }
    }
    if slots.len() != usize::try_from(count).unwrap_or(usize::MAX)
        || !slots.values().all(|assignment| {
            replicas.iter().any(|replica| {
                replica.meta.deletion_timestamp.is_none()
                    && replica.spec.service_id == service.meta.id
                    && replica.spec.deployment_id == *deployment_id
                    && replica.spec.assignment_id == assignment.meta.id
                    && replica.spec.replica_index == assignment.spec.replica_index
                    && replica.status.phase == DeploymentPhase::Ready
            })
        })
    {
        return Ok(None);
    }

    let mut addresses = slots
        .values()
        .map(|assignment| assignment.spec.workload_address)
        .collect::<Vec<_>>();
    addresses.sort();
    addresses.dedup();
    let mut specs = Vec::new();
    let service_name = service_name(&service.meta.id, cluster_id)?;
    let ipv4 = addresses
        .iter()
        .filter_map(|address| match address {
            IpAddr::V4(address) => Some(DnsRecordValue::A(*address)),
            IpAddr::V6(_) => None,
        })
        .collect::<Vec<_>>();
    if !ipv4.is_empty() {
        specs.push(DnsRecordSpec {
            name: service_name.clone(),
            values: ipv4,
            ttl_secs: settings.ttl_secs,
        });
    }
    let ipv6 = addresses
        .into_iter()
        .filter_map(|address| match address {
            IpAddr::V4(_) => None,
            IpAddr::V6(address) => Some(DnsRecordValue::Aaaa(address)),
        })
        .collect::<Vec<_>>();
    if !ipv6.is_empty() {
        specs.push(DnsRecordSpec {
            name: service_name,
            values: ipv6,
            ttl_secs: settings.ttl_secs,
        });
    }
    for assignment in slots.into_values() {
        specs.push(DnsRecordSpec {
            name: replica_name(&service.meta.id, assignment.spec.replica_index, cluster_id)?,
            values: vec![record_value(assignment.spec.workload_address)],
            ttl_secs: settings.ttl_secs,
        });
    }
    specs.sort_by(|left, right| {
        left.name
            .cmp(&right.name)
            .then_with(|| record_kind(left).cmp(record_kind(right)))
    });
    Ok(Some(specs))
}

fn record_kind(spec: &DnsRecordSpec) -> &'static str {
    match spec.values.first() {
        Some(DnsRecordValue::A(_)) => "a",
        Some(DnsRecordValue::Aaaa(_)) => "aaaa",
        Some(DnsRecordValue::Cname(_)) => "cname",
        Some(DnsRecordValue::Txt(_)) => "txt",
        Some(DnsRecordValue::Srv { .. }) => "srv",
        None => "empty",
    }
}

fn record_value(address: IpAddr) -> DnsRecordValue {
    match address {
        IpAddr::V4(address) => DnsRecordValue::A(address),
        IpAddr::V6(address) => DnsRecordValue::Aaaa(address),
    }
}
