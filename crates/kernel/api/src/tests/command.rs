use serde_json::json;

use crate::{
    ResourceRevision, ServiceReplicaOverrideRequest, UpgradeCreateRequest, UpgradeMode,
    UpgradeOperation, UpgradeRunId, UpgradeRunSpec, openapi_document,
};

#[test]
fn upgrade_force_is_sent_only_when_requested() {
    let request = UpgradeCreateRequest {
        upgrade_run_id: UpgradeRunId::new("upgrade-1").expect("valid upgrade id"),
        spec: UpgradeRunSpec {
            operation: UpgradeOperation::Upgrade,
            target_version: "0.6.45".to_owned(),
            mode: UpgradeMode::Rolling,
            node_ids: Vec::new(),
        },
        force: false,
    };

    let ordinary = serde_json::to_value(&request).expect("serialize ordinary upgrade");
    assert_eq!(ordinary.get("force"), None);
    let decoded: UpgradeCreateRequest =
        serde_json::from_value(ordinary).expect("decode request without force");
    assert!(!decoded.force);

    let forced = serde_json::to_value(UpgradeCreateRequest {
        force: true,
        ..request
    })
    .expect("serialize forced upgrade");
    assert_eq!(forced.get("force"), Some(&json!(true)));
}

#[test]
fn replica_override_distinguishes_clear_from_a_missing_field() {
    let clear = serde_json::from_value::<ServiceReplicaOverrideRequest>(json!({
        "expectedRevision": 7,
        "replicas": null
    }))
    .expect("nullable replicas should decode");
    assert_eq!(clear.expected_revision, ResourceRevision(7));
    assert_eq!(clear.replicas, None);

    let missing = serde_json::from_value::<ServiceReplicaOverrideRequest>(json!({
        "expectedRevision": 7
    }));
    assert!(missing.is_err());
}

#[test]
fn replica_override_schema_is_required_and_nullable() {
    let document = openapi_document();
    let schema = document
        .pointer("/components/schemas/ServiceReplicaOverrideRequest")
        .expect("replica override schema should be registered");
    assert_eq!(schema.pointer("/required/1"), Some(&json!("replicas")),);
    assert_eq!(
        schema.pointer("/properties/replicas/nullable"),
        Some(&json!(true)),
    );
}
