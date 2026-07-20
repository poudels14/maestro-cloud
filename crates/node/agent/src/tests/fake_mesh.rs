use std::sync::Mutex;

use async_trait::async_trait;

use crate::{MeshBackend, MeshBackendError, MeshConfiguration};

#[derive(Default)]
pub(crate) struct FakeMeshBackend {
    applied: Mutex<Vec<MeshConfiguration>>,
}

impl FakeMeshBackend {
    pub(crate) fn applied(&self) -> Vec<MeshConfiguration> {
        self.applied
            .lock()
            .map(|applied| applied.clone())
            .unwrap_or_default()
    }
}

#[async_trait]
impl MeshBackend for FakeMeshBackend {
    async fn apply(&self, desired: &MeshConfiguration) -> Result<(), MeshBackendError> {
        self.applied
            .lock()
            .map_err(|_| MeshBackendError::new("fake backend lock was poisoned"))?
            .push(desired.clone());
        Ok(())
    }
}
