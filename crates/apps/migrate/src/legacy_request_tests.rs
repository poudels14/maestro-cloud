use kernel_api::{BuiltinKind, Node};
use serde_json::{Value, json};

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::{json_entry, node_entries};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[test]
fn cutover_reserves_rewrite_compatible_completed_requests() -> TestResult {
    let snapshot = request_snapshot(vec![
        receipt("request-1", "complete", Some(201)),
        receipt(".legacy-only.", "complete", Some(204)),
    ])?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    assert_eq!(plan.request_claims().len(), 1);
    assert_eq!(
        plan.request_claims()
            .first()
            .map(|claim| claim.request_id().as_str()),
        Some("request-1")
    );

    let master = master(&plan)?;
    let summary = master
        .meta
        .annotations
        .get(&kernel_api::AnnotationKey(
            "migration.maestro.dev/legacy-request-receipts".to_owned(),
        ))
        .ok_or("legacy request summary is missing")?;
    assert_eq!(
        serde_json::from_str::<Value>(summary)?,
        json!({
            "completedReceipts": 2,
            "collisionBarriers": 1,
            "rejectedByRewrite": 1
        })
    );
    Ok(())
}

#[test]
fn cutover_blocks_while_a_request_is_in_progress() -> TestResult {
    let snapshot = request_snapshot(vec![receipt("request-1", "in-progress", None)])?;
    let Err(error) = plan_legacy_snapshot(&snapshot, MASTER_SECRET) else {
        return Err("cutover accepted an in-progress request receipt".into());
    };
    assert!(matches!(error, LegacyPlanError::DecodeLegacyState { .. }));
    assert!(error.to_string().contains("cutover is not quiescent"));
    Ok(())
}

#[test]
fn cutover_rejects_malformed_request_receipts() -> TestResult {
    for entry in [
        json_entry(
            "/maetro/cluster/requests/request-1",
            receipt_value("complete", Some(99), "ab".repeat(32)),
        ),
        json_entry(
            "/maetro/cluster/requests/request-1",
            receipt_value("unknown", Some(200), "ab".repeat(32)),
        ),
        json_entry(
            "/maetro/cluster/requests/request-1",
            receipt_value("complete", Some(200), "AB".repeat(32)),
        ),
    ] {
        let snapshot = request_snapshot(vec![entry])?;
        assert!(matches!(
            plan_legacy_snapshot(&snapshot, MASTER_SECRET),
            Err(LegacyPlanError::DecodeLegacyState { .. })
        ));
    }
    Ok(())
}

fn request_snapshot(extra: Vec<LegacyEntry>) -> Result<LegacySnapshot, crate::SnapshotError> {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state());
    entries.extend(extra);
    LegacySnapshot::new(entries)
}

fn receipt(request_id: &str, state: &str, status_code: Option<u16>) -> LegacyEntry {
    json_entry(
        &format!("/maetro/cluster/requests/{request_id}"),
        receipt_value(state, status_code, "ab".repeat(32)),
    )
}

fn receipt_value(state: &str, status_code: Option<u16>, fingerprint: String) -> Value {
    json!({
        "fingerprint": fingerprint,
        "state": state,
        "statusCode": status_code,
        "contentType": status_code.map(|_| "application/json"),
        "body": status_code.map_or_else(Vec::new, |_| br#"{"ok":true}"#.to_vec()),
        "updatedAtMs": 4_000
    })
}

fn master(plan: &crate::MigrationPlan) -> Result<Node, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == BuiltinKind::Node && write.id().as_str() == "node-a")
        .ok_or("converted master is missing")?;
    Ok(serde_json::from_slice(write.value())?)
}
