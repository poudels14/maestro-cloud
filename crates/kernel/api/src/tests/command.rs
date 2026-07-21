use serde_json::json;

use crate::{ResourceRevision, ServiceReplicaOverrideRequest, openapi_document};

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
