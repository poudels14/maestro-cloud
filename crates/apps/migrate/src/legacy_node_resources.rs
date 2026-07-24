use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    AnnotationKey, BuiltinResource, Condition, ConditionReason, ConditionState, ConditionType,
    Generation, Node, NodeId, NodeInstanceId, NodeSpec, NodeStatus, Object, ObjectMeta,
    ResourceRevision, Timestamp,
};
use serde::Serialize;

use crate::legacy_convert::LegacyPlanError;
use crate::legacy_node_schema::convert_role;
use crate::legacy_nodes::NodeBundle;

pub(crate) fn convert_nodes(
    nodes: &BTreeMap<NodeId, NodeBundle>,
) -> Result<Vec<BuiltinResource>, LegacyPlanError> {
    nodes
        .iter()
        .map(|(node_id, bundle)| convert_node(node_id, bundle).map(BuiltinResource::Node))
        .collect()
}

fn convert_node(node_id: &NodeId, bundle: &NodeBundle) -> Result<Node, LegacyPlanError> {
    let info = &bundle.record.last_info;
    let mut annotations = BTreeMap::new();
    preserve_annotation(
        &mut annotations,
        "migration.maestro.dev/legacy-node-record",
        &bundle.record,
        node_id,
    )?;
    preserve_annotation(
        &mut annotations,
        "migration.maestro.dev/legacy-subnet-reservation",
        &bundle.subnet,
        node_id,
    )?;
    preserve_annotation(
        &mut annotations,
        "migration.maestro.dev/legacy-control-reservation",
        &bundle.control,
        node_id,
    )?;
    if let Some(state) = &bundle.state {
        preserve_annotation(
            &mut annotations,
            "migration.maestro.dev/legacy-node-state",
            state,
            node_id,
        )?;
    }
    Ok(Object {
        meta: ObjectMeta {
            id: node_id.clone(),
            labels: BTreeMap::new(),
            annotations,
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: NodeSpec {
            hostname: info.hostname.clone(),
            host_address: info.cluster_host_ip.into(),
            role: convert_role(info.role),
            workload_network_mode: kernel_api::WorkloadNetworkMode::ClusterRouted,
            scheduling_labels: info.labels.clone(),
        },
        status: NodeStatus {
            instance_id: NodeInstanceId::new(&info.instance_id).map_err(|error| {
                LegacyPlanError::InvalidIdentifier {
                    field: "node instance id",
                    value: info.instance_id.clone(),
                    message: error.to_string(),
                }
            })?,
            version: info.version.clone(),
            last_seen: Timestamp(bundle.record.last_seen_at_ms),
            conditions: conditions(bundle),
        },
    })
}

fn preserve_annotation<Value: Serialize>(
    annotations: &mut BTreeMap<AnnotationKey, String>,
    key: &'static str,
    value: &Value,
    node_id: &NodeId,
) -> Result<(), LegacyPlanError> {
    let value =
        serde_json::to_string(value).map_err(|error| LegacyPlanError::InvalidClusterState {
            resource_id: node_id.to_string(),
            message: format!("could not preserve legacy node state: {error}"),
        })?;
    annotations.insert(AnnotationKey(key.to_owned()), value);
    Ok(())
}

fn conditions(bundle: &NodeBundle) -> Vec<Condition> {
    let info = &bundle.record.last_info;
    let mut conditions = vec![
        Condition {
            condition_type: ConditionType("Maintenance".to_owned()),
            state: ConditionState::True,
            reason: ConditionReason("CutoverPending".to_owned()),
            message: "scheduling is frozen until cutover verification completes".to_owned(),
            observed_generation: Generation(1),
            last_transition_time: Timestamp(bundle.record.last_seen_at_ms),
        },
        Condition {
            condition_type: ConditionType("LegacyDataPlaneReady".to_owned()),
            state: if info.data_plane_ready {
                ConditionState::True
            } else {
                ConditionState::False
            },
            reason: ConditionReason(if info.data_plane_ready {
                "MigratedReady".to_owned()
            } else {
                "MigratedUnavailable".to_owned()
            }),
            message: info
                .data_plane_error
                .clone()
                .unwrap_or_else(|| "legacy data-plane status captured at cutover".to_owned()),
            observed_generation: Generation(1),
            last_transition_time: Timestamp(if info.data_plane_checked_at_ms > 0 {
                info.data_plane_checked_at_ms
            } else {
                bundle.record.last_seen_at_ms
            }),
        },
    ];
    if let Some(state) = &bundle.state
        && state.unschedulable
    {
        conditions.push(Condition {
            condition_type: ConditionType("Draining".to_owned()),
            state: ConditionState::True,
            reason: ConditionReason("LegacyUnschedulable".to_owned()),
            message: state
                .reason
                .clone()
                .unwrap_or_else(|| "legacy node was unschedulable at cutover".to_owned()),
            observed_generation: Generation(1),
            last_transition_time: Timestamp(
                state.drained_at_ms.unwrap_or(bundle.record.last_seen_at_ms),
            ),
        });
    }
    conditions
}
