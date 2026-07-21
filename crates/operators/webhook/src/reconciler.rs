use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Object, ResourceKind, Webhook,
    WebhookId, WebhookSpec, WebhookStatus,
};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace};

use crate::snapshot::{WebhookSnapshot, WebhookSnapshotError};
use crate::writer::{WebhookStatusWriter, WebhookWriteError};
use crate::{WebhookDelivery, WebhookDeliveryBackend};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);
const READY_CONDITION: &str = "Ready";

/// Retry policy for failed outbound webhook deliveries.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct WebhookSettings {
    /// Delay before retrying a delivery rejected by or unavailable at the endpoint.
    pub retry_delay: Duration,
}

impl WebhookSettings {
    /// Rejects a retry policy that could create a hot loop.
    pub fn validate(self) -> Result<Self, WebhookError> {
        if self.retry_delay.is_zero() {
            Err(WebhookError::ZeroRetryDelay)
        } else {
            Ok(self)
        }
    }
}

/// Watch-driven reconciler for typed outbound webhook transitions.
pub struct WebhookReconciler {
    cluster_id: kernel_api::ClusterId,
    keyspace: Keyspace,
    backend: Arc<dyn WebhookDeliveryBackend>,
    timestamp_clock: Arc<dyn TimestampClock>,
    settings: WebhookSettings,
    writer: WebhookStatusWriter,
    prefix: kernel_store::StorePrefix,
    trigger_prefix: kernel_store::StorePrefix,
}

impl WebhookReconciler {
    /// Creates the reconciler without reading state or starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        backend: Arc<dyn WebhookDeliveryBackend>,
        timestamp_clock: Arc<dyn TimestampClock>,
        settings: WebhookSettings,
    ) -> Result<Self, WebhookError> {
        let settings = settings.validate()?;
        let keyspace = Keyspace::new(&cluster_id);
        let kind = ResourceKind::new(<Self as Reconciler>::KIND)?;
        Ok(Self {
            cluster_id: cluster_id.clone(),
            writer: WebhookStatusWriter::new(&cluster_id)?,
            prefix: keyspace.resource_kind(&kind),
            trigger_prefix: keyspace.cluster(),
            keyspace,
            backend,
            timestamp_clock,
            settings,
        })
    }

    /// Wraps delivery in shared watches, fencing, resync, and backoff.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        monotonic_clock: Arc<dyn Clock>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.prefix.clone(),
            self.trigger_prefix.clone(),
            store,
            monotonic_clock,
            config,
        )
    }

    async fn converge(
        &self,
        mut webhook: Webhook,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        webhook.meta.revision = context.observed_version().resource_revision();
        let snapshot = WebhookSnapshot::load(context.store(), &self.keyspace, &webhook.spec.events)
            .await
            .map_err(classify_snapshot)?;
        if webhook.status.observed_generation != Some(webhook.meta.generation) {
            webhook.status.observed_generation = Some(webhook.meta.generation);
            webhook.status.observations = snapshot.baseline();
            webhook.status.retry_at = None;
            self.set_condition(
                &mut webhook,
                ConditionState::True,
                "BaselineReady",
                "subscribed resource states are baselined",
            );
            return self
                .persist(context, &webhook, Vec::new(), Action::Done)
                .await;
        }
        if let Some(retry_at) = webhook.status.retry_at {
            let now = self.timestamp_clock.now();
            if retry_at.0 > now.0 {
                return Ok(Action::Requeue(Duration::from_millis(
                    u64::try_from(retry_at.0.saturating_sub(now.0)).unwrap_or(u64::MAX),
                )));
            }
        }
        if let Some((current, previous)) = snapshot.first_transition(&webhook.status.observations) {
            let delivery = WebhookDelivery::new(
                self.cluster_id.clone(),
                webhook.meta.id.clone(),
                current.observation.resource_id.clone(),
                previous,
                current.observation.state,
                current.observation.resource_revision,
                self.timestamp_clock.now(),
            )
            .map_err(|error| ReconcileError::Terminal {
                reason: "DeliveryEncodingFailed".to_string(),
                message: error.to_string(),
            })?;
            return match self
                .backend
                .deliver(
                    &webhook.spec.endpoint,
                    &webhook.spec.signing_secret,
                    &delivery,
                )
                .await
            {
                Ok(()) => {
                    webhook.status.last_success_at = Some(self.timestamp_clock.now());
                    webhook.status.consecutive_failures = 0;
                    webhook.status.retry_at = None;
                    webhook.status.observations =
                        snapshot.acknowledge(&webhook.status.observations, &current.observation);
                    self.set_condition(
                        &mut webhook,
                        ConditionState::True,
                        "DeliverySucceeded",
                        "the latest transition was accepted",
                    );
                    self.persist(
                        context,
                        &webhook,
                        vec![current.source_compare.clone()],
                        Action::Requeue(Duration::ZERO),
                    )
                    .await
                }
                Err(error) => {
                    webhook.status.consecutive_failures =
                        webhook.status.consecutive_failures.saturating_add(1);
                    webhook.status.retry_at = Some(kernel_api::Timestamp(
                        self.timestamp_clock
                            .now()
                            .0
                            .saturating_add(duration_millis(self.settings.retry_delay)),
                    ));
                    self.set_condition(
                        &mut webhook,
                        ConditionState::False,
                        "DeliveryFailed",
                        &error.to_string(),
                    );
                    self.persist(
                        context,
                        &webhook,
                        vec![current.source_compare.clone()],
                        Action::Requeue(self.settings.retry_delay),
                    )
                    .await
                }
            };
        }
        if snapshot.retain_present(&mut webhook.status.observations) {
            self.persist(context, &webhook, Vec::new(), Action::Done)
                .await
        } else {
            Ok(Action::Done)
        }
    }

    async fn persist(
        &self,
        context: &ReconcileContext,
        webhook: &Webhook,
        dependency_compares: Vec<kernel_store::Compare>,
        applied: Action,
    ) -> Result<Action, ReconcileError> {
        match self
            .writer
            .replace(
                context.store(),
                context.observed_version(),
                webhook,
                dependency_compares,
            )
            .await
        {
            Ok(true) => Ok(applied),
            Ok(false) => Ok(Action::Requeue(CONFLICT_RETRY)),
            Err(WebhookWriteError::Controller(error)) => Err(ReconcileError::Infrastructure(error)),
            Err(error) => Err(ReconcileError::Terminal {
                reason: "WebhookStatusWriteFailed".to_string(),
                message: error.to_string(),
            }),
        }
    }

    fn set_condition(
        &self,
        webhook: &mut Webhook,
        state: ConditionState,
        reason: &str,
        message: &str,
    ) {
        let previous = webhook
            .status
            .conditions
            .iter()
            .find(|condition| condition.condition_type.0 == READY_CONDITION);
        let transitioned_at = previous
            .filter(|condition| condition.state == state)
            .map_or_else(
                || self.timestamp_clock.now(),
                |condition| condition.last_transition_time,
            );
        webhook
            .status
            .conditions
            .retain(|condition| condition.condition_type.0 != READY_CONDITION);
        webhook.status.conditions.push(Condition {
            condition_type: ConditionType(READY_CONDITION.to_string()),
            state,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: webhook.meta.generation,
            last_transition_time: transitioned_at,
        });
    }
}

#[async_trait]
impl Reconciler for WebhookReconciler {
    type Id = WebhookId;
    type Spec = WebhookSpec;
    type Status = WebhookStatus;

    const KIND: &'static str = "Webhook";

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(resource, &context).await
    }
}

fn classify_snapshot(error: WebhookSnapshotError) -> ReconcileError {
    match error {
        WebhookSnapshotError::Controller(error) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: "WebhookSnapshotInvalid".to_string(),
            message: error.to_string(),
        },
    }
}

fn duration_millis(duration: Duration) -> i64 {
    i64::try_from(duration.as_millis()).unwrap_or(i64::MAX)
}

/// Matchable webhook operator construction failure.
#[derive(Debug, thiserror::Error)]
pub enum WebhookError {
    /// A zero retry delay would hot-loop a failing external endpoint.
    #[error("webhook retry delay must be greater than zero")]
    ZeroRetryDelay,
    /// A static resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
}
