use kernel_api::{ResourceKind, ResourceName, Webhook};
use kernel_controller::{ControllerError, FencedStore};
use kernel_store::{Compare, ExpectedVersion, Keyspace, Mutation, Transaction, TransactionOutcome};

pub(crate) struct WebhookStatusWriter {
    keyspace: Keyspace,
    kind: ResourceKind,
}

impl WebhookStatusWriter {
    pub(crate) fn new(
        cluster_id: &kernel_api::ClusterId,
    ) -> Result<Self, kernel_api::InvalidIdentifier> {
        Ok(Self {
            keyspace: Keyspace::new(cluster_id),
            kind: ResourceKind::new("Webhook")?,
        })
    }

    pub(crate) async fn replace(
        &self,
        store: &FencedStore,
        observed: kernel_store::Version,
        webhook: &Webhook,
        mut dependency_compares: Vec<Compare>,
    ) -> Result<bool, WebhookWriteError> {
        let key = self
            .keyspace
            .resource(&self.kind, &ResourceName::from(webhook.meta.id.clone()));
        dependency_compares.insert(
            0,
            Compare {
                key: key.clone(),
                expected: ExpectedVersion::Exact(observed),
            },
        );
        let value = serde_json::to_vec(webhook).map_err(|error| WebhookWriteError::Serialize {
            webhook_id: webhook.meta.id.to_string(),
            message: error.to_string(),
        })?;
        let outcome = store
            .txn(Transaction {
                compares: dependency_compares,
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
pub(crate) enum WebhookWriteError {
    #[error(transparent)]
    Controller(#[from] ControllerError),
    #[error("failed to serialize Webhook `{webhook_id}`: {message}")]
    Serialize { webhook_id: String, message: String },
}
