use crate::{BuiltinKind, DecodeResourceError, ResourceKind, decode_builtin, openapi_document};

#[test]
fn openapi_contains_a_named_schema_for_every_builtin_kind() {
    let document = openapi_document();
    let schemas = document
        .pointer("/components/schemas")
        .and_then(serde_json::Value::as_object)
        .expect("OpenAPI component schemas");

    for kind in BuiltinKind::ALL {
        assert!(schemas.contains_key(kind.as_str()), "missing {kind} schema");
    }
}

#[test]
fn registry_distinguishes_unknown_kinds_from_malformed_builtins() {
    let unknown = ResourceKind::new("Widget").expect("custom resource kind");
    let known = ResourceKind::new("Service").expect("built-in resource kind");

    assert!(matches!(
        decode_builtin(&unknown, serde_json::json!({})),
        Err(DecodeResourceError::UnknownKind(_))
    ));
    assert!(matches!(
        decode_builtin(&known, serde_json::json!({})),
        Err(DecodeResourceError::Malformed { .. })
    ));
}

#[test]
fn openapi_document_matches_the_reviewed_contract() {
    insta::assert_json_snapshot!(openapi_document());
}
