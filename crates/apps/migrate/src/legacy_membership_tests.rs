use kernel_api::{AnnotationKey, BuiltinKind, Node};

use crate::legacy_fixtures::{cluster_meta, cluster_state, cluster_state_for, voter};
use crate::legacy_node_tests::node_entries;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";
const MEMBER_ANNOTATION: &str = "migration.maestro.dev/legacy-store-member";

#[test]
fn cutover_plan_binds_current_store_member_to_its_node() -> TestResult {
    let mut entries = node_entries("node-a", "master", 10, 1);
    entries.extend(cluster_state());
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let node = decode_node(&plan, "node-a")?;
    let member = node
        .meta
        .annotations
        .get(&AnnotationKey(MEMBER_ANNOTATION.to_owned()))
        .ok_or("missing store member annotation")?;
    let member: serde_json::Value = serde_json::from_str(member)?;

    assert_eq!(member.get("memberId"), Some(&serde_json::json!(10)));
    assert_eq!(
        member.get("name"),
        Some(&serde_json::json!("maestro-0a00000a-0bb8"))
    );
    assert_eq!(
        member.get("peerUrls"),
        Some(&serde_json::json!(["https://10.0.0.10:2380"]))
    );
    Ok(())
}

#[test]
fn cutover_plan_requires_exact_voter_coverage() -> TestResult {
    let mut missing = node_entries("node-a", "master", 10, 1);
    missing.extend(node_entries("node-b", "voter", 11, 2));
    missing.extend(node_entries("node-c", "voter", 12, 3));
    missing.push(cluster_meta());
    missing.push(voter(10, 10));
    let snapshot = LegacySnapshot::new(missing)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let mut complete = node_entries("node-a", "master", 10, 1);
    complete.extend(node_entries("node-b", "voter", 11, 2));
    complete.extend(node_entries("node-c", "voter", 12, 3));
    complete.extend(cluster_state_for(&[10, 11, 12]));
    let snapshot = LegacySnapshot::new(complete)?;
    plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    Ok(())
}

#[test]
fn cutover_plan_rejects_mismatched_voter_identity_or_endpoint() -> TestResult {
    let record = voter(10, 10);
    let mut wrong_id = node_entries("node-a", "master", 10, 1);
    wrong_id.push(cluster_meta());
    wrong_id.push(LegacyEntry::new(
        "/maetro/cluster/voters/000000000000000b",
        record.value().to_vec(),
    ));
    let snapshot = LegacySnapshot::new(wrong_id)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let mut wrong_endpoint = node_entries("node-a", "master", 10, 1);
    wrong_endpoint.push(cluster_meta());
    wrong_endpoint.push(voter(11, 11));
    let snapshot = LegacySnapshot::new(wrong_endpoint)?;
    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

fn decode_node(plan: &crate::MigrationPlan, id: &str) -> Result<Node, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == BuiltinKind::Node && write.id().as_str() == id)
        .ok_or_else(|| std::io::Error::other(format!("missing Node/{id}")))?;
    Ok(serde_json::from_slice(write.value())?)
}
