use kernel_api::{AnnotationKey, BuiltinKind, Node};
use serde_json::{Value, json};

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::{json_entry, node_entries};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";
const ARCHIVE_ANNOTATION: &str = "migration.maestro.dev/legacy-maintenance-state";

#[test]
fn cutover_plan_archives_terminal_maintenance_and_progress() -> TestResult {
    let mut entries = complete_cluster();
    entries.push(json_entry(
        "/maetro/cluster/upgrade/current",
        maintenance_run("succeeded"),
    ));
    entries.push(json_entry(
        "/maetro/system/upgrade-progress/node-a",
        json!({
            "runId": "upgrade-run",
            "attemptId": "upgrade-run:node-a:150",
            "targetVersion": "2.0.0",
            "stage": "restarting",
            "updatedAtMs": 200
        }),
    ));
    entries.push(json_entry(
        "/maetro/system/upgrade-progress",
        json!({
            "targetVersion": "1.9.0",
            "stage": "failed",
            "updatedAtMs": 90,
            "error": "standalone upgrade failed"
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let node = decode_node(&plan, "node-a")?;
    let archive = node
        .meta
        .annotations
        .get(&AnnotationKey(ARCHIVE_ANNOTATION.to_owned()))
        .ok_or("missing maintenance archive")?;
    let archive: Value = serde_json::from_str(archive)?;

    assert_eq!(archive.pointer("/run/runId"), Some(&json!("upgrade-run")));
    assert_eq!(
        archive.pointer("/progress/node-a/stage"),
        Some(&json!("restarting"))
    );
    assert_eq!(
        archive.pointer("/progress/$cluster/stage"),
        Some(&json!("failed"))
    );
    Ok(())
}

#[test]
fn cutover_plan_rejects_actionable_maintenance_keys() -> TestResult {
    let active_entries = [
        json_entry(
            "/maetro/cluster/upgrade/current",
            maintenance_run("verifying"),
        ),
        json_entry(
            "/maetro/system/cluster-freeze",
            json!({"reason": "upgrade", "upgradeRunId": "run", "atMs": 100}),
        ),
        json_entry(
            "/maetro/system/upgrade-request/node-a",
            json!({"systemType": "nixos", "targetVersion": "2.0.0"}),
        ),
        LegacyEntry::new("/maetro/system/restart-request/node-a", b"1".to_vec()),
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
fn cutover_plan_rejects_a_maintenance_owned_node_drain() -> TestResult {
    let mut entries = complete_cluster();
    entries.push(json_entry(
        "/maetro/cluster/node-state/node-a",
        json!({
            "unschedulable": true,
            "drainedAtMs": 250,
            "reason": "upgrade"
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

fn maintenance_run(phase: &str) -> Value {
    let terminal = phase == "succeeded";
    json!({
        "runId": "upgrade-run",
        "kind": "upgrade",
        "batch": "rolling",
        "targetVersion": "2.0.0",
        "requestedAtMs": 100,
        "updatedAtMs": if terminal { 300 } else { 200 },
        "requestedByNodeId": "node-a",
        "phase": phase,
        "phaseStartedAtMs": if terminal { 300 } else { 200 },
        "currentNodeIndex": if terminal { 1 } else { 0 },
        "nodes": [{
            "nodeId": "node-a",
            "hostname": "node-a.internal",
            "role": "master",
            "fromVersion": "1.0.0",
            "fromInstanceId": "instance-node-a",
            "status": if terminal { "succeeded" } else { "verifying" },
            "startedAtMs": 100,
            "completedAtMs": if terminal { Some(250) } else { None },
            "upgradeStartedAtMs": 150,
            "lastUpgradeRequestAtMs": 150,
            "upgradeStage": "restarting",
            "restartStartedAtMs": 200,
            "retryNotBeforeMs": null,
            "error": null
        }],
        "history": [
            {"atMs": 100, "phase": "draining", "nodeId": null, "message": "started"},
            {"atMs": if terminal { 300 } else { 200 }, "phase": phase, "nodeId": null,
             "message": if terminal { "completed" } else { "verifying" }}
        ],
        "failure": null
    })
}

fn decode_node(plan: &crate::MigrationPlan, id: &str) -> Result<Node, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == BuiltinKind::Node && write.id().as_str() == id)
        .ok_or_else(|| std::io::Error::other(format!("missing Node/{id}")))?;
    Ok(serde_json::from_slice(write.value())?)
}
