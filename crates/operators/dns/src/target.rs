use std::collections::{BTreeMap, BTreeSet};
use std::net::IpAddr;

use kernel_api::{
    AnnotationKey, Assignment, AssignmentPhase, DeploymentPhase, DnsRecordSpec, DnsRecordValue,
    NodeId, ReplicaState, Service, assignment_workload_address,
};

use crate::validation::{alias_name, replica_name, service_name};
use crate::{DNS_ALIASES_ANNOTATION, DnsPlanError, DnsSettings};

pub(crate) fn desired_specs(
    cluster_id: &kernel_api::ClusterId,
    settings: DnsSettings,
    service: &Service,
    assignments: &[Assignment],
    live_nodes: &BTreeSet<NodeId>,
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
    slots.retain(|_, assignment| {
        assignment.status.phase == AssignmentPhase::Running
            && live_nodes.contains(&assignment.spec.node_id)
            && replicas.iter().any(|replica| {
                replica.meta.deletion_timestamp.is_none()
                    && replica.spec.service_id == service.meta.id
                    && replica.spec.deployment_id == *deployment_id
                    && replica.spec.assignment_id == assignment.meta.id
                    && replica.spec.replica_index == assignment.spec.replica_index
                    && replica.status.phase == DeploymentPhase::Ready
            })
    });

    let addressed = slots
        .values()
        .filter_map(|assignment| {
            assignment_workload_address(assignment).map(|address| (*assignment, address))
        })
        .collect::<Vec<_>>();
    if addressed.len() != slots.len() {
        return Ok(None);
    }
    let mut addresses = addressed
        .iter()
        .map(|(_assignment, address)| *address)
        .collect::<Vec<_>>();
    addresses.sort();
    addresses.dedup();
    let mut specs = Vec::new();
    let service_name = service_name(&service.meta.id, cluster_id)?;
    let aliases = service
        .meta
        .annotations
        .get(&AnnotationKey(DNS_ALIASES_ANNOTATION.to_owned()))
        .into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|alias| !alias.is_empty())
        .map(|alias| alias_name(alias, cluster_id))
        .collect::<Result<BTreeSet<_>, _>>()?;
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
            values: ipv4.clone(),
            ttl_secs: settings.ttl_secs,
        });
        specs.extend(aliases.iter().cloned().map(|name| DnsRecordSpec {
            name,
            values: ipv4.clone(),
            ttl_secs: settings.ttl_secs,
        }));
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
            values: ipv6.clone(),
            ttl_secs: settings.ttl_secs,
        });
        specs.extend(aliases.into_iter().map(|name| DnsRecordSpec {
            name,
            values: ipv6.clone(),
            ttl_secs: settings.ttl_secs,
        }));
    }
    for (assignment, address) in addressed {
        specs.push(DnsRecordSpec {
            name: replica_name(&service.meta.id, assignment.spec.replica_index, cluster_id)?,
            values: vec![record_value(address)],
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
