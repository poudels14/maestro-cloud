use std::fmt::Display;

use kernel_api::{FinalizerName, Object};
use kernel_store::{
    Compare, ExpectedVersion, Mutation, StoreKey, StoredValue, Transaction, TransactionOutcome,
    Version,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use tracing::Instrument;

use super::ControllerRuntime;
#[cfg(feature = "test-util")]
use crate::JournalEntry;
use crate::{Action, ControllerError, ReconcileContext, ReconcileError, Reconciler};

impl<R> ControllerRuntime<R>
where
    R: Reconciler + 'static,
    R::Id: DeserializeOwned + Serialize + Display,
    R::Spec: DeserializeOwned + Serialize,
    R::Status: DeserializeOwned + Serialize,
{
    pub(super) async fn process(
        &self,
        stored: StoredValue,
        attempt: u32,
    ) -> Result<ProcessResult, ControllerError> {
        self.fenced_store.verify_leadership().await?;
        let mut resource: Object<R::Id, R::Spec, R::Status> = serde_json::from_slice(&stored.value)
            .map_err(|error| ControllerError::MalformedResource {
                kind: R::KIND,
                message: error.to_string(),
            })?;
        resource.meta.revision = stored.version.resource_revision();

        let finalizer = R::FINALIZER.map(|name| FinalizerName(name.to_string()));
        if resource.meta.deletion_timestamp.is_none()
            && let Some(finalizer) = finalizer.as_ref()
            && !resource.meta.finalizers.contains(finalizer)
        {
            resource.meta.finalizers.insert(finalizer.clone());
            self.persist_resource(
                &stored.key,
                stored.version,
                &resource,
                PersistenceAction::Store,
            )
            .await?;
            return Ok(ProcessResult::changed());
        }

        if resource.meta.deletion_timestamp.is_some() {
            return self
                .process_deletion(stored.key, stored.version, resource, finalizer, attempt)
                .await;
        }

        self.invoke(resource, stored.version, attempt, ReconcilePhase::Active)
            .await
    }

    async fn process_deletion(
        &self,
        key: StoreKey,
        version: Version,
        mut resource: Object<R::Id, R::Spec, R::Status>,
        finalizer: Option<FinalizerName>,
        attempt: u32,
    ) -> Result<ProcessResult, ControllerError> {
        let Some(finalizer) = finalizer else {
            if resource.meta.finalizers.is_empty() {
                self.persist_resource(&key, version, &resource, PersistenceAction::Delete)
                    .await?;
            }
            return Ok(ProcessResult::changed());
        };
        if !resource.meta.finalizers.contains(&finalizer) {
            if resource.meta.finalizers.is_empty() {
                self.persist_resource(&key, version, &resource, PersistenceAction::Delete)
                    .await?;
            }
            return Ok(ProcessResult::changed());
        }

        let outcome = self
            .invoke(
                resource.clone(),
                version,
                attempt,
                ReconcilePhase::Finalizing,
            )
            .await?;
        if outcome.action == Some(Action::Done) && outcome.successful {
            resource.meta.finalizers.remove(&finalizer);
            let persistence = if resource.meta.finalizers.is_empty() {
                PersistenceAction::Delete
            } else {
                PersistenceAction::Store
            };
            self.persist_resource(&key, version, &resource, persistence)
                .await?;
            Ok(ProcessResult {
                action: None,
                invoked: true,
                next_attempt: 0,
                successful: true,
            })
        } else {
            Ok(outcome)
        }
    }

    async fn invoke(
        &self,
        resource: Object<R::Id, R::Spec, R::Status>,
        version: Version,
        attempt: u32,
        phase: ReconcilePhase,
    ) -> Result<ProcessResult, ControllerError> {
        #[cfg(feature = "test-util")]
        let started_at = self.clock.now();
        #[cfg(feature = "test-util")]
        let resource_id = resource.meta.id.to_string();
        #[cfg(feature = "test-util")]
        let observed_revision = version.resource_revision();
        let deleting = phase.is_deleting();
        let span = tracing::info_span!(
            "reconcile",
            kind = R::KIND,
            resource_id = %resource.meta.id,
            deleting,
            attempt,
        );
        let context = ReconcileContext::new(
            self.fenced_store.clone(),
            self.clock.clone(),
            version,
            attempt,
        );
        let result = match phase {
            ReconcilePhase::Active => {
                self.reconciler
                    .reconcile(resource, context)
                    .instrument(span.clone())
                    .await
            }
            ReconcilePhase::Finalizing => {
                self.reconciler
                    .finalize(resource, context)
                    .instrument(span.clone())
                    .await
            }
        };
        let outcome = match result {
            Ok(action) => ProcessResult {
                action: Some(action),
                invoked: true,
                next_attempt: 0,
                successful: true,
            },
            Err(ReconcileError::Retryable { message }) => {
                span.in_scope(|| {
                    tracing::warn!(kind = R::KIND, %message, "reconcile will be retried");
                });
                ProcessResult {
                    action: Some(Action::Requeue(self.config.retry_backoff.delay(attempt))),
                    invoked: true,
                    next_attempt: attempt.saturating_add(1),
                    successful: false,
                }
            }
            Err(ReconcileError::Terminal { reason, message }) => {
                span.in_scope(|| {
                    tracing::error!(
                        kind = R::KIND,
                        %reason,
                        %message,
                        "reconcile requires an external change"
                    );
                });
                ProcessResult {
                    action: Some(Action::Done),
                    invoked: true,
                    next_attempt: 0,
                    successful: false,
                }
            }
            Err(ReconcileError::Infrastructure(error)) => return Err(error),
        };
        #[cfg(feature = "test-util")]
        if let Some(action) = outcome.action {
            self.journal.record(JournalEntry {
                sequence: 0,
                kind: R::KIND,
                resource_id,
                observed_revision,
                action: action.into(),
                duration: self
                    .clock
                    .now()
                    .as_duration()
                    .saturating_sub(started_at.as_duration()),
                deleting,
            });
        }
        Ok(outcome)
    }

    async fn persist_resource(
        &self,
        key: &StoreKey,
        version: Version,
        resource: &Object<R::Id, R::Spec, R::Status>,
        action: PersistenceAction,
    ) -> Result<(), ControllerError> {
        let mutation = match action {
            PersistenceAction::Store => Mutation::Put {
                key: key.clone(),
                value: serde_json::to_vec(resource).map_err(|error| {
                    ControllerError::SerializeResource {
                        kind: R::KIND,
                        message: error.to_string(),
                    }
                })?,
                session: None,
            },
            PersistenceAction::Delete => Mutation::Delete { key: key.clone() },
        };
        let outcome = self
            .fenced_store
            .txn(Transaction {
                compares: vec![Compare {
                    key: key.clone(),
                    expected: ExpectedVersion::Exact(version),
                }],
                mutations: vec![mutation],
            })
            .await?;
        match outcome {
            TransactionOutcome::Applied { .. } | TransactionOutcome::Conflict => Ok(()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ReconcilePhase {
    Active,
    Finalizing,
}

impl ReconcilePhase {
    const fn is_deleting(self) -> bool {
        matches!(self, Self::Finalizing)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PersistenceAction {
    Store,
    Delete,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(super) struct ProcessResult {
    pub(super) action: Option<Action>,
    pub(super) invoked: bool,
    pub(super) next_attempt: u32,
    successful: bool,
}

impl ProcessResult {
    fn changed() -> Self {
        Self {
            action: None,
            invoked: false,
            next_attempt: 0,
            successful: false,
        }
    }
}
