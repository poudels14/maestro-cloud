use kernel_api::{BuiltinKind, Ownership, PlacementHistory};
use serde_json::json;

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::{json_entry, node_entries};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[test]
fn cutover_converts_indexed_placement_history() -> TestResult {
    let snapshot = placement_snapshot(vec![
        placement("assignment-1", 0, None),
        index("assignment-1", 0),
        placement("assignment-2", 1, Some(4_000)),
        index("assignment-2", 1),
    ])?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let mut placements = plan
        .writes()
        .iter()
        .filter(|write| write.kind() == BuiltinKind::PlacementHistory)
        .map(|write| serde_json::from_slice::<PlacementHistory>(write.value()))
        .collect::<Result<Vec<_>, _>>()?;
    placements.sort_by(|left, right| left.meta.id.cmp(&right.meta.id));

    assert_eq!(placements.len(), 2);
    let active = placements.first().ok_or("active placement is missing")?;
    assert_eq!(active.meta.id.as_str(), "assignment-1");
    assert_eq!(active.spec.service_id.as_str(), "api");
    assert_eq!(active.spec.deployment_id.as_str(), "deploy-1");
    assert_eq!(active.spec.node_id.as_str(), "node-a");
    assert_eq!(active.spec.cluster_host_address.to_string(), "10.0.0.10");
    assert_eq!(active.spec.cluster_api_port, 3000);
    assert_eq!(active.status.started_at.0, 2_000);
    assert_eq!(active.status.ended_at, None);
    assert_eq!(
        active.meta.owner_refs.first().map(|owner| owner.ownership),
        Some(Ownership::Informational)
    );
    assert_eq!(
        placements
            .get(1)
            .and_then(|item| item.status.ended_at)
            .map(|time| time.0),
        Some(4_000)
    );
    Ok(())
}

#[test]
fn cutover_requires_one_exact_index_per_placement() -> TestResult {
    for entries in [
        vec![placement("assignment-1", 0, None)],
        vec![index("assignment-1", 0)],
        vec![
            placement("assignment-1", 0, None),
            json_entry(
                "/maetro/cluster/placement-index/api/deploy-1/1/assignment-1",
                json!("/maetro/cluster/placements/node-a/assignment-1"),
            ),
        ],
    ] {
        let snapshot = placement_snapshot(entries)?;
        assert!(matches!(
            plan_legacy_snapshot(&snapshot, MASTER_SECRET),
            Err(LegacyPlanError::DecodeLegacyState { .. })
        ));
    }
    Ok(())
}

#[test]
fn cutover_rejects_inverted_placement_timestamps() -> TestResult {
    let snapshot = placement_snapshot(vec![
        json_entry(
            "/maetro/cluster/placements/node-a/assignment-1",
            placement_value("assignment-1", 0, Some(1_999)),
        ),
        index("assignment-1", 0),
    ])?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

fn placement_snapshot(extra: Vec<LegacyEntry>) -> Result<LegacySnapshot, crate::SnapshotError> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state());
    entries.extend(extra);
    LegacySnapshot::new(entries)
}

fn placement(assignment_id: &str, replica_index: u32, ended_at_ms: Option<i64>) -> LegacyEntry {
    json_entry(
        &format!("/maetro/cluster/placements/node-a/{assignment_id}"),
        placement_value(assignment_id, replica_index, ended_at_ms),
    )
}

fn placement_value(
    assignment_id: &str,
    replica_index: u32,
    ended_at_ms: Option<i64>,
) -> serde_json::Value {
    json!({
        "assignmentId": assignment_id,
        "serviceId": "api",
        "deploymentId": "deploy-1",
        "replicaIndex": replica_index,
        "nodeId": "node-a",
        "clusterHostIp": "10.0.0.10",
        "clusterApiPort": 3000,
        "containerHostname": format!("api-{replica_index}"),
        "startedAtMs": 2_000,
        "endedAtMs": ended_at_ms
    })
}

fn index(assignment_id: &str, replica_index: u32) -> LegacyEntry {
    LegacyEntry::new(
        format!("/maetro/cluster/placement-index/api/deploy-1/{replica_index}/{assignment_id}"),
        format!("/maetro/cluster/placements/node-a/{assignment_id}").into_bytes(),
    )
}
