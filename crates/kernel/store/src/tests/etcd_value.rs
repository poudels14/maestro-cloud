use kernel_api::{ClusterId, ResourceKind, ResourceName};

use crate::etcd_value::ValueProtector;
use crate::{Keyspace, StoreError, derive_key};

#[test]
fn internal_values_are_context_bound_and_traefik_values_remain_external()
-> Result<(), Box<dyn std::error::Error>> {
    let values = ValueProtector::new(Some(derive_key(
        "store-protector-test-secret-with-32-characters",
    )?));
    let wrong_values = ValueProtector::new(Some(derive_key(
        "different-protector-test-secret-with-32-characters",
    )?));
    let keys = Keyspace::new(&ClusterId::new("test")?);
    let service = keys.resource(&ResourceKind::new("Service")?, &ResourceName::new("api")?);
    let protected = values.protect(&service, b"database-password")?;

    assert_ne!(protected, b"database-password");
    assert!(protected.starts_with(b"MAE1"));
    assert_eq!(
        values.unprotect(&service, &protected)?,
        b"database-password"
    );
    assert!(matches!(
        wrong_values.unprotect(&service, &protected),
        Err(StoreError::Protection { .. })
    ));

    let authority = keys.certificate_authority();
    let authority_record = br#"{"privateKeyPem":"cluster-ca-private-key"}"#;
    let protected_authority = values.protect(&authority, authority_record)?;
    assert_ne!(protected_authority, authority_record);
    assert!(protected_authority.starts_with(b"MAE1"));
    assert_eq!(
        values.unprotect(&authority, &protected_authority)?,
        authority_record
    );

    let traefik = keys.traefik_entry("http/routers/api/rule")?;
    assert_eq!(
        values.protect(&traefik, b"Host(`api.example.test`)")?,
        b"Host(`api.example.test`)"
    );
    Ok(())
}
