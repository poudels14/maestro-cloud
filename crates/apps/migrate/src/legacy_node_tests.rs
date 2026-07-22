use kernel_api::{AnnotationKey, BuiltinKind, ConditionState, Node, NodeRole};
use serde_json::{Value, json};

use crate::legacy_fixtures::{CLUSTER_ID, cluster_meta, cluster_state, cluster_state_for};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[test]
fn cutover_plan_converts_durable_node_identity_and_drain_state() -> TestResult {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state());
    let info = node_info("node-a", "master", 10, 1);
    entries.push(json_entry("/maetro/cluster/nodes/node-a", info.clone()));
    entries.push(json_entry(
        "/maetro/cluster/node-state/node-a",
        json!({
            "unschedulable": true,
            "drainedAtMs": 1_800,
            "reason": "operator maintenance"
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let node: Node = decode_node(&plan, "node-a")?;

    assert_eq!(plan.cluster_id().as_str(), CLUSTER_ID);
    assert_eq!(node.spec.hostname, "node-a.internal");
    assert_eq!(node.spec.host_address.to_string(), "10.0.0.10");
    assert_eq!(node.spec.role, NodeRole::Master);
    assert_eq!(
        node.spec
            .scheduling_labels
            .get("region")
            .map(String::as_str),
        Some("west")
    );
    assert_eq!(node.status.instance_id.as_str(), "instance-node-a");
    assert_eq!(node.status.last_seen.0, 2_000);
    assert!(node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "Maintenance"
            && condition.state == ConditionState::True
            && condition.reason.0 == "CutoverPending"
    }));
    assert!(node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "LegacyDataPlaneReady"
            && condition.state == ConditionState::False
    }));
    assert!(node.status.conditions.iter().any(|condition| {
        condition.condition_type.0 == "Draining"
            && condition.state == ConditionState::True
            && condition.message == "operator maintenance"
    }));
    for key in [
        "migration.maestro.dev/legacy-node-record",
        "migration.maestro.dev/legacy-node-state",
        "migration.maestro.dev/legacy-subnet-reservation",
        "migration.maestro.dev/legacy-control-reservation",
        "migration.maestro.dev/legacy-cluster-meta",
        "migration.maestro.dev/legacy-store-member",
    ] {
        assert!(
            node.meta
                .annotations
                .contains_key(&AnnotationKey(key.to_owned()))
        );
    }
    Ok(())
}

#[test]
fn cutover_plan_rejects_cluster_identity_that_disagrees_with_nodes() -> TestResult {
    let mut entries = node_entries("node-a", "master", 10, 1);
    let meta = cluster_meta();
    let mut value: Value = serde_json::from_slice(meta.value())?;
    value
        .as_object_mut()
        .ok_or("missing cluster-meta fixture")?
        .insert("bootstrapHostIp".to_owned(), json!("10.0.0.11"));
    entries.push(json_entry(meta.key(), value));
    let snapshot = LegacySnapshot::new(entries)?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_maps_legacy_voters_into_control_plane_nodes() -> TestResult {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    entries.extend(cluster_state_for(&[10, 11, 12]));
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;

    assert_eq!(
        decode_node(&plan, "node-b")?.spec.role,
        NodeRole::ControlPlane
    );
    assert_eq!(
        decode_node(&plan, "node-c")?.spec.role,
        NodeRole::ControlPlane
    );
    Ok(())
}

#[test]
fn cutover_plan_rejects_incomplete_or_orphan_node_state() -> TestResult {
    let mut missing_control = node_entries("node-a", "master", 10, 1);
    missing_control.push(cluster_meta());
    missing_control.retain(|entry| !entry.key().contains("/control-addresses/"));
    let snapshot = LegacySnapshot::new(missing_control)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let snapshot = LegacySnapshot::new(vec![json_entry(
        "/maetro/cluster/node-state/node-a",
        json!({"unschedulable": false, "drainedAtMs": null, "reason": null}),
    )])?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_rejects_topology_that_cannot_preflight() -> TestResult {
    let mut overlapping = node_entries("node-a", "master", 10, 1);
    overlapping.extend(node_entries("node-b", "worker", 11, 1));
    overlapping.push(cluster_meta());
    let snapshot = LegacySnapshot::new(overlapping)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let mut noncanonical = node_entries("node-a", "master", 10, 1);
    noncanonical.push(cluster_meta());
    for entry in &mut noncanonical {
        let key = entry.key().to_owned();
        if key.contains("/node-records/") {
            let mut value: Value = serde_json::from_slice(entry.value())?;
            value
                .get_mut("lastInfo")
                .and_then(Value::as_object_mut)
                .ok_or("missing node-info fixture")?
                .insert("subnet".to_owned(), json!("10.42.1.5/24"));
            *entry = json_entry(&key, value);
        } else if key.contains("/subnets/") {
            let mut value: Value = serde_json::from_slice(entry.value())?;
            value
                .as_object_mut()
                .ok_or("missing subnet fixture")?
                .insert("cidr".to_owned(), json!("10.42.1.5/24"));
            *entry = json_entry(&key, value);
        }
    }
    let snapshot = LegacySnapshot::new(noncanonical)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let mut mismatched_ports = node_entries("node-a", "master", 10, 1);
    mismatched_ports.push(cluster_meta());
    let mut worker = node_entries("node-b", "worker", 11, 2);
    let control = worker
        .iter_mut()
        .find(|entry| entry.key().contains("/control-addresses/"))
        .ok_or("missing control fixture")?;
    let control_key = control.key().to_owned();
    *control = json_entry(
        &control_key,
        json!({
            "hostIp": "10.0.0.11",
            "apiPort": 3001,
            "gatewayPort": 3003,
            "etcdClientPort": 2379,
            "etcdPeerPort": 2380,
            "nodeId": "node-b",
            "state": "active"
        }),
    );
    mismatched_ports.extend(worker);
    let snapshot = LegacySnapshot::new(mismatched_ports)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

pub(crate) fn node_entries(
    node_id: &str,
    role: &str,
    host_octet: u8,
    subnet_octet: u8,
) -> Vec<LegacyEntry> {
    let info = node_info(node_id, role, host_octet, subnet_octet);
    let api_port = 3_000;
    let host_ip = format!("10.0.0.{host_octet}");
    let suffix = format!(
        "{:08x}-{api_port:04x}",
        u32::from_be_bytes([10, 0, 0, host_octet])
    );
    vec![
        json_entry(
            &format!("/maetro/cluster/node-records/{node_id}"),
            json!({
                "lastInfo": info,
                "lastSeenAtMs": 2_000,
                "lostAtMs": null,
                "dataPlaneLostAtMs": 1_900,
                "controlPlaneAlertedAtMs": null,
                "dataPlaneAlertedAtMs": 1_950
            }),
        ),
        json_entry(
            &format!("/maetro/cluster/subnets/{node_id}"),
            json!({
                "cidr": format!("10.42.{subnet_octet}.0/24"),
                "nodeId": node_id,
                "state": "active"
            }),
        ),
        json_entry(
            &format!("/maetro/cluster/control-addresses/{suffix}"),
            json!({
                "hostIp": host_ip,
                "apiPort": api_port,
                "gatewayPort": 3002,
                "etcdClientPort": 2379,
                "etcdPeerPort": 2380,
                "nodeId": node_id,
                "state": "active"
            }),
        ),
    ]
}

fn node_info(node_id: &str, role: &str, host_octet: u8, subnet_octet: u8) -> Value {
    json!({
        "nodeId": node_id,
        "instanceId": format!("instance-{node_id}"),
        "hostname": format!("{node_id}.internal"),
        "role": role,
        "clusterHostIp": format!("10.0.0.{host_octet}"),
        "clusterApiPort": 3000,
        "clusterGatewayPort": 3002,
        "subnet": format!("10.42.{subnet_octet}.0/24"),
        "tailscaleIp": format!("100.64.0.{host_octet}"),
        "dataPlaneReady": false,
        "dataPlaneCheckedAtMs": 1_900,
        "dataPlaneError": "legacy gateway probe failed",
        "version": "0.9.0",
        "startedAtMs": 1_000,
        "labels": {"region": "west"}
    })
}

pub(crate) fn json_entry(key: &str, value: Value) -> LegacyEntry {
    LegacyEntry::new(key, value.to_string().into_bytes())
}

fn decode_node(plan: &crate::MigrationPlan, id: &str) -> Result<Node, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == BuiltinKind::Node && write.id().as_str() == id)
        .ok_or_else(|| std::io::Error::other(format!("missing Node/{id}")))?;
    Ok(serde_json::from_slice(write.value())?)
}
