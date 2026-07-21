use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    Generation, Object, ObjectMeta, OwnerReference, Ownership, ResourceId, ResourceKind,
    ResourceName, Service, Timestamp, TrafficGeneration, TrafficGenerationId,
    TrafficGenerationPhase, TrafficGenerationSpec, TrafficGenerationStatus,
};
use sha2::{Digest, Sha256};

use crate::IngressPlanError;

pub(crate) fn new_generation(
    cluster_id: &kernel_api::ClusterId,
    service: &Service,
    spec: TrafficGenerationSpec,
    now: Timestamp,
) -> Result<TrafficGeneration, IngressPlanError> {
    let id = TrafficGenerationId::new(generation_id(cluster_id, &spec));
    Ok(Object {
        meta: ObjectMeta {
            id: id?,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
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
        status: TrafficGenerationStatus {
            phase: TrafficGenerationPhase::Staged,
            staged_at: now,
            activated_at: None,
            retired_at: None,
            conditions: Vec::new(),
        },
    })
}

fn generation_id(cluster_id: &kernel_api::ClusterId, spec: &TrafficGenerationSpec) -> String {
    let mut hash = Sha256::new();
    field(&mut hash, cluster_id.as_str());
    field(&mut hash, spec.service_id.as_str());
    field(&mut hash, spec.deployment_id.as_str());
    field(&mut hash, &spec.epoch.to_string());
    for route in &spec.routes {
        field(&mut hash, route.route_id.as_str());
        field(&mut hash, &route.route_generation.0.to_string());
        for host in &route.hosts {
            field(&mut hash, host);
        }
        field(&mut hash, route.path_prefix.as_deref().unwrap_or_default());
        field(&mut hash, &route.target_port.to_string());
        field(
            &mut hash,
            route
                .session_affinity
                .as_ref()
                .map(|affinity| affinity.header.as_str())
                .unwrap_or_default(),
        );
    }
    for target in &spec.targets {
        field(&mut hash, target.assignment_id.as_str());
        field(&mut hash, &target.endpoint.to_string());
    }
    let suffix = hash
        .finalize()
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("traffic-{suffix}")
}

fn field(hash: &mut Sha256, value: &str) {
    hash.update(value.as_bytes());
    hash.update([0]);
}
