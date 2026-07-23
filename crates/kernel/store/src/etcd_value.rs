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
