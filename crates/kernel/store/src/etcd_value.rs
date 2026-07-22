use std::sync::Arc;

use crate::{
    EncryptedValue, EncryptionError, EncryptionKey, StoreError, StoreKey, open_with_context,
    seal_with_context,
};

const TRAEFIK_INTEGRATION_SEGMENT: &str = "/integrations/traefik/";

/// Application-level protection policy around etcd's opaque value bytes.
#[derive(Clone)]
pub(crate) struct ValueProtector {
    encryption_key: Option<Arc<EncryptionKey>>,
}

impl ValueProtector {
    pub(crate) fn new(encryption_key: Option<EncryptionKey>) -> Self {
        Self {
            encryption_key: encryption_key.map(Arc::new),
        }
    }

    pub(crate) fn protect(&self, key: &StoreKey, value: &[u8]) -> Result<Vec<u8>, StoreError> {
        match self.encryption_key.as_deref() {
            Some(encryption_key) if requires_value_protection(key) => {
                seal_with_context(encryption_key, value, key.as_str().as_bytes())
                    .map(EncryptedValue::into_bytes)
                    .map_err(protection_error)
            }
            Some(_) | None => Ok(value.to_vec()),
        }
    }

    pub(crate) fn unprotect(&self, key: &StoreKey, value: &[u8]) -> Result<Vec<u8>, StoreError> {
        match self.encryption_key.as_deref() {
            Some(encryption_key) if requires_value_protection(key) => {
                EncryptedValue::from_bytes(value.to_vec())
                    .and_then(|encrypted| {
                        open_with_context(encryption_key, &encrypted, key.as_str().as_bytes())
                    })
                    .map_err(protection_error)
            }
            Some(_) | None => Ok(value.to_vec()),
        }
    }
}

fn requires_value_protection(key: &StoreKey) -> bool {
    !key.as_str().contains(TRAEFIK_INTEGRATION_SEGMENT)
}

fn protection_error(error: EncryptionError) -> StoreError {
    StoreError::Protection {
        message: error.to_string(),
    }
}

#[cfg(test)]
mod tests {
    use kernel_api::{ClusterId, ResourceKind, ResourceName};

    use super::ValueProtector;
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

        let traefik = keys.traefik_entry("http/routers/api/rule")?;
        assert_eq!(
            values.protect(&traefik, b"Host(`api.example.test`)")?,
            b"Host(`api.example.test`)"
        );
        Ok(())
    }
}
