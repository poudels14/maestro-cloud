use kernel_api::{Build, ResourceKind, ResourceName};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome};

pub(crate) struct BuildStatusWriter {
    keyspace: Keyspace,
    kind: ResourceKind,
}

impl BuildStatusWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            kind: ResourceKind::new("Build")?,
        })
    }

    /// Replaces one Build only while its exact observed store version remains current.
    pub(crate) async fn replace(
        &self,
        store: &FencedStore,
        observed: kernel_store::Version,
        build: &Build,
    ) -> Result<bool, BuildWriteError> {
        let key = self
            .keyspace
            .resource(&self.kind, &ResourceName::from(build.meta.id.clone()));
        let value = serde_json::to_vec(build).map_err(|error| BuildWriteError::Serialize {
            build_id: build.meta.id.to_string(),
            message: error.to_string(),
        })?;
        let outcome = store
            .txn(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(observed),
                }],
                mutations: vec![Mutation::Put {
                    key,
                    value,
                    session: None,
                }],
            })
            .await?;
        Ok(matches!(outcome, TransactionOutcome::Applied { .. }))
    }
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum BuildWriteError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error("failed to serialize Build `{build_id}`: {message}")]
    Serialize { build_id: String, message: String },
}
