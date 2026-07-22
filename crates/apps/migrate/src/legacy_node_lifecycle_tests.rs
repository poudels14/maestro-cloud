use kernel_api::{AnnotationKey, BuiltinKind, Node, NodeRole, NodeTombstone};
use serde_json::json;

use crate::legacy_fixtures::{cluster_state, cluster_state_for};
use crate::legacy_node_tests::{json_entry, node_entries};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";
const JOIN_ANNOTATION: &str = "migration.maestro.dev/legacy-join-state";

#[test]
fn cutover_plan_preserves_a_settled_voter_join() -> TestResult {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(node_entries("node-b", "voter", 11, 2));
    entries.extend(node_entries("node-c", "voter", 12, 3));
    entries.extend(node_entries("node-d", "worker", 13, 4));
    entries.extend(cluster_state_for(&[10, 11, 12]));
    entries.push(join_intent("node-b", "voter", 11, 2, Some(11)));
    entries.push(join_intent("node-d", "worker", 13, 4, None));
    entries.push(json_entry(
        "/maetro/cluster/admissions/node-b",
        json!({
            "nodeId": "node-b",
            "role": "voter",
            "clusterHostIp": "10.0.0.11",
            "clusterApiPort": 3000,
            "subnet": "10.42.2.0/24",
            "publicKeySha256": "ab".repeat(32),
            "createdAtMs": 1_500
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let node: Node = decode_write(&plan, BuiltinKind::Node, "node-b")?;
    let state = node
        .meta
        .annotations
        .get(&AnnotationKey(JOIN_ANNOTATION.to_owned()))
        .ok_or("missing settled join state")?;
    let state: serde_json::Value = serde_json::from_str(state)?;

    assert_eq!(node.spec.role, NodeRole::ControlPlane);
    assert_eq!(state.pointer("/intent/memberId"), Some(&json!(11)));
    assert_eq!(
        state.pointer("/admission/publicKeySha256"),
        Some(&json!("ab".repeat(32)))
    );
    let worker: Node = decode_write(&plan, BuiltinKind::Node, "node-d")?;
    let worker_state = worker
        .meta
        .annotations
        .get(&AnnotationKey(JOIN_ANNOTATION.to_owned()))
        .ok_or("missing settled worker join state")?;
    let worker_state: serde_json::Value = serde_json::from_str(worker_state)?;
    assert!(worker_state.get("admission").is_none());
    assert_eq!(worker_state.pointer("/intent/memberId"), Some(&json!(null)));
    Ok(())
}

#[test]
fn cutover_plan_converts_removed_node_identity_into_a_tombstone() -> TestResult {
    let mut entries = complete_cluster();
    entries.push(json_entry(
        "/maetro/cluster/membership-history/old-worker",
        json!({
            "nodeId": "old-worker",
            "hostIp": "10.0.0.20",
            "role": "worker",
            "removedAtMs": 5_000
        }),
    ));
    entries.push(json_entry(
        "/maetro/cluster/removed/old-worker",
        json!({
            "nodeId": "old-worker",
            "hostIp": "10.0.0.20",
            "role": "worker",
            "requestedAtMs": 4_000
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let tombstone: NodeTombstone = decode_write(&plan, BuiltinKind::NodeTombstone, "old-worker")?;

    assert_eq!(tombstone.spec.host_address.to_string(), "10.0.0.20");
    assert_eq!(tombstone.spec.role, NodeRole::Worker);
    assert_eq!(tombstone.spec.requested_at.0, 4_000);
    assert_eq!(tombstone.status.removed_at.0, 5_000);
    assert!(tombstone.meta.annotations.contains_key(&AnnotationKey(
        "migration.maestro.dev/legacy-membership-history".to_owned()
    )));
    Ok(())
}

#[test]
fn cutover_plan_rejects_inflight_node_lifecycle_state() -> TestResult {
    let active_entries = [
        LegacyEntry::new(
            format!("/maetro/cluster/join-nonces/{}", "01".repeat(16)),
            b"node-b".to_vec(),
        ),
        json_entry(
            "/maetro/cluster/removals/node-a",
            json!({
                "nodeId": "node-a",
                "hostIp": "10.0.0.10",
                "role": "master",
                "requestedAtMs": 2_000
            }),
        ),
        join_intent("missing", "worker", 21, 3, None),
        join_intent("node-a", "master", 10, 1, Some(99)),
        json_entry(
            "/maetro/cluster/admissions/pending",
            json!({
                "nodeId": "pending",
                "role": "voter",
                "clusterHostIp": "10.0.0.22",
                "clusterApiPort": 3000,
                "subnet": "10.42.4.0/24",
                "publicKeySha256": "cd".repeat(32),
                "createdAtMs": 2_000
            }),
        ),
    ];
    for active in active_entries {
        let mut entries = complete_cluster();
        entries.push(active);
        let snapshot = LegacySnapshot::new(entries)?;
        assert!(matches!(
            plan_legacy_snapshot(&snapshot, MASTER_SECRET),
            Err(LegacyPlanError::DecodeLegacyState { .. })
        ));
    }
    Ok(())
}

#[test]
fn cutover_plan_rejects_unpaired_removed_node_state() -> TestResult {
    let mut entries = complete_cluster();
    entries.push(json_entry(
        "/maetro/cluster/removed/old-worker",
        json!({
            "nodeId": "old-worker",
            "hostIp": "10.0.0.20",
            "role": "worker",
            "requestedAtMs": 4_000
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

fn complete_cluster() -> Vec<LegacyEntry> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state());
    entries
}

fn join_intent(
    node_id: &str,
    role: &str,
    host_octet: u8,
    subnet_octet: u8,
    member_id: Option<u64>,
) -> LegacyEntry {
    json_entry(
        &format!("/maetro/cluster/join-intents/{node_id}"),
        json!({
            "nodeId": node_id,
            "role": role,
            "clusterHostIp": format!("10.0.0.{host_octet}"),
            "clusterApiPort": 3000,
            "etcdPeerPort": 2380,
            "subnet": format!("10.42.{subnet_octet}.0/24"),
            "publicKeySha256": "ab".repeat(32),
            "memberId": member_id
        }),
    )
}

fn decode_write<Value: serde::de::DeserializeOwned>(
    plan: &crate::MigrationPlan,
    kind: BuiltinKind,
    id: &str,
) -> Result<Value, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == kind && write.id().as_str() == id)
        .ok_or_else(|| std::io::Error::other(format!("missing {kind}/{id}")))?;
    Ok(serde_json::from_slice(write.value())?)
}
