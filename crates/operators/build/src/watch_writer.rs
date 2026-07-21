use kernel_api::{ResourceKind, ResourceName, Service};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome};

pub(crate) struct BuildWatchWriter {
    keyspace: Keyspace,
    kind: ResourceKind,
}

impl BuildWatchWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            kind: ResourceKind::new("Service")?,
        })
    }

    pub(crate) async fn replace(
        &self,
        store: &FencedStore,
        observed: kernel_store::Version,
        service: &Service,
    ) -> Result<bool, BuildWatchWriteError> {
        let key = self
            .keyspace
            .resource(&self.kind, &ResourceName::from(service.meta.id.clone()));
        let value =
            serde_json::to_vec(service).map_err(|error| BuildWatchWriteError::Serialize {
                service_id: service.meta.id.to_string(),
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
pub(crate) enum BuildWatchWriteError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error("failed to serialize Service `{service_id}`: {message}")]
    Serialize { service_id: String, message: String },
}
