use std::str::FromStr;

use crate::{InvalidIdentifier, ResourceId, ServiceId};

#[test]
fn identifiers_accept_key_and_url_safe_values() {
    let service_id = ServiceId::from_str("api.preview_42").expect("valid service id");

    assert_eq!(service_id.as_str(), "api.preview_42");
    assert_eq!(service_id.to_string(), "api.preview_42");
}

#[test]
fn identifiers_reject_invalid_boundaries_and_characters() {
    assert_eq!(
        ServiceId::new("-api"),
        Err(InvalidIdentifier::InvalidBoundary)
    );
    assert_eq!(
        ServiceId::new("api/service"),
        Err(InvalidIdentifier::UnsupportedCharacter { character: '/' })
    );
}

#[test]
fn deserialization_validates_identifiers() {
    let error = serde_json::from_str::<ResourceId>(r#"{"kind":"Service","id":"bad/id"}"#)
        .expect_err("invalid resource identity should fail");

    assert!(error.to_string().contains("unsupported character"));
}
