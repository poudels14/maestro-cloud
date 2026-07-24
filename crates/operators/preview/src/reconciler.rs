use std::collections::BTreeSet;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::{
    Condition, ConditionReason, ConditionState, ConditionType, Object, Preview, PreviewId,
    PreviewPhase, PreviewSpec, PreviewStatus, ResourceKind, Timestamp,
};
use kernel_controller::{
    Action, ControllerRuntime, FencedStore, ReconcileContext, ReconcileError, Reconciler,
    RuntimeConfig, TimestampClock,
};
use kernel_store::{Clock, Keyspace, StorePrefix};

use crate::error::PreviewError;
use crate::resource::{desired_route, desired_service, route_id};
use crate::settings::PreviewSettings;
use crate::snapshot::PreviewSnapshot;
use crate::writer::{PreviewWriteOutcome, PreviewWriter};

const CONFLICT_RETRY: Duration = Duration::from_millis(100);
const CLEANUP_RETRY: Duration = Duration::from_secs(1);
const READY_CONDITION: &str = "Ready";

/// Reconciles Preview resources into isolated Services and ingress routes.
pub struct PreviewReconciler {
    keyspace: Keyspace,
    prefix: StorePrefix,
    timestamp_clock: Arc<dyn TimestampClock>,
    monotonic_clock: Arc<dyn Clock>,
    settings: PreviewSettings,
    writer: PreviewWriter,
}

impl PreviewReconciler {
    /// Constructs a preview reconciler without starting background work.
    pub fn new(
        cluster_id: kernel_api::ClusterId,
        timestamp_clock: Arc<dyn TimestampClock>,
        monotonic_clock: Arc<dyn Clock>,
        settings: PreviewSettings,
    ) -> Result<Self, PreviewError> {
        let keyspace = Keyspace::new(&cluster_id);
        let kind = ResourceKind::new("Preview")?;
        Ok(Self {
            prefix: keyspace.resource_kind(&kind),
            writer: PreviewWriter::new(&cluster_id)?,
            keyspace,
            timestamp_clock,
            monotonic_clock,
            settings,
        })
    }

    /// Wraps this operator in the shared watch, resync, and retry runtime.
    pub fn runtime(
        self: Arc<Self>,
        store: Arc<FencedStore>,
        config: RuntimeConfig,
    ) -> ControllerRuntime<Self> {
        ControllerRuntime::new_with_trigger_prefix(
            self.clone(),
            self.prefix.clone(),
            self.keyspace.cluster(),
            store,
            self.monotonic_clock.clone(),
            config,
        )
    }

    async fn converge(
        &self,
        mut preview: Preview,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        preview.meta.revision = context.observed_version().resource_revision();
        let now = self.timestamp_clock.now();
        let snapshot = PreviewSnapshot::load(context.store(), &self.keyspace, &preview)
            .await
            .map_err(classify)?;
        if now.0 >= preview.spec.expires_at.0 {
            preview.meta.deletion_timestamp.get_or_insert(now);
            preview.status.phase = PreviewPhase::Closing;
            preview.status.teardown_at = Some(preview.spec.expires_at);
            self.set_condition(
                &mut preview,
                ConditionState::False,
                "PreviewExpired",
                "preview lifetime elapsed",
            );
            return self
                .persist(
                    context,
                    &preview,
                    &snapshot,
                    None,
                    &[],
                    &[],
                    Action::Requeue(Duration::ZERO),
                )
                .await;
        }
        let Some(base) = snapshot.base.as_ref() else {
            let message = format!(
                "base service `{}` does not exist",
                preview.spec.base_service_id
            );
            return self
                .fail(preview, context, &snapshot, "BaseServiceMissing", message)
                .await;
        };
        if snapshot.base_routes.is_empty() {
            return self
                .fail(
                    preview,
                    context,
                    &snapshot,
                    "BaseIngressMissing",
                    format!(
                        "base service `{}` has no ingress route",
                        base.resource.meta.id
                    ),
                )
                .await;
        }
        let service = match desired_service(
            &preview,
            &base.resource,
            snapshot.child.as_ref().map(|child| &child.resource),
        ) {
            Ok(service) => service,
            Err(error) => {
                return self
                    .fail(
                        preview,
                        context,
                        &snapshot,
                        "InvalidPreviewDefinition",
                        error.to_string(),
                    )
                    .await;
            }
        };
        let mut desired_routes = Vec::new();
        for base_route in &snapshot.base_routes {
            let desired_route_id =
                route_id(&preview, &base_route.resource.meta.id).map_err(classify)?;
            let route = desired_route(
                &preview,
                &base_route.resource,
                snapshot
                    .routes
                    .get(&desired_route_id)
                    .map(|route| &route.resource),
                &self.settings.preview_domain,
            )
            .map_err(classify)?;
            desired_routes.push(route);
        }
        let desired_ids = desired_routes
            .iter()
            .map(|route| route.meta.id.clone())
            .collect::<BTreeSet<_>>();
        let stale_routes = snapshot
            .child_routes
            .values()
            .filter(|route| !desired_ids.contains(&route.resource.meta.id))
            .cloned()
            .collect::<Vec<_>>();

        let ready = service.status.active_deployment_id.is_some();
        let phase = if ready {
            PreviewPhase::Active
        } else {
            PreviewPhase::Pending
        };
        if !preview.status.phase.can_transition_to(phase) {
            return Err(ReconcileError::Terminal {
                reason: "InvalidPreviewTransition".to_string(),
                message: format!(
                    "preview cannot transition from {:?} to {phase:?}",
                    preview.status.phase
                ),
            });
        }
        preview.status.phase = phase;
        preview.status.teardown_at = None;
        self.set_condition(
            &mut preview,
            if ready {
                ConditionState::True
            } else {
                ConditionState::False
            },
            if ready {
                "PreviewReady"
            } else {
                "PreviewPending"
            },
            if ready {
                "derived service is active"
            } else {
                "derived service is converging"
            },
        );
        self.persist(
            context,
            &preview,
            &snapshot,
            Some(&service),
            &desired_routes,
            &stale_routes,
            Action::Done,
        )
        .await
    }

    async fn finalize_preview(
        &self,
        mut preview: Preview,
        context: &ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        preview.meta.revision = context.observed_version().resource_revision();
        let now = self.timestamp_clock.now();
        let closed_at = preview.meta.deletion_timestamp.unwrap_or(now);
        let grace_millis =
            i64::try_from(preview.spec.close_grace_period_secs.saturating_mul(1_000))
                .unwrap_or(i64::MAX);
        let teardown_at = Timestamp(
            closed_at
                .0
                .saturating_add(grace_millis)
                .min(preview.spec.expires_at.0),
        );
        let snapshot = PreviewSnapshot::load(context.store(), &self.keyspace, &preview)
            .await
            .map_err(classify)?;
        if now.0 < teardown_at.0 {
            preview.status.phase = PreviewPhase::Closing;
            preview.status.teardown_at = Some(teardown_at);
            self.set_condition(
                &mut preview,
                ConditionState::False,
                "PreviewClosing",
                "preview is retained during its close grace period",
            );
            let delay_millis =
                u64::try_from(teardown_at.0.saturating_sub(now.0)).unwrap_or(u64::MAX);
            let wake = self
                .monotonic_clock
                .now()
                .saturating_add(Duration::from_millis(delay_millis));
            return self
                .persist(
                    context,
                    &preview,
                    &snapshot,
                    None,
                    &[],
                    &[],
                    Action::RequeueAt(wake),
                )
                .await;
        }
        if let Some(child) = &snapshot.child {
            if child.resource.meta.deletion_timestamp.is_none() {
                let mut deleting = child.resource.clone();
                deleting.meta.deletion_timestamp = Some(now);
                preview.status.phase = PreviewPhase::Closing;
                preview.status.teardown_at = Some(teardown_at);
                self.set_condition(
                    &mut preview,
                    ConditionState::False,
                    "PreviewTeardown",
                    "derived service deletion is in progress",
                );
                return self
                    .persist(
                        context,
                        &preview,
                        &snapshot,
                        Some(&deleting),
                        &[],
                        &[],
                        Action::Requeue(CLEANUP_RETRY),
                    )
                    .await;
            }
            return Ok(Action::Requeue(CLEANUP_RETRY));
        }
        if preview.status.phase != PreviewPhase::Expired || !snapshot.child_routes.is_empty() {
            preview.status.phase = PreviewPhase::Expired;
            preview.status.teardown_at = Some(teardown_at);
            self.set_condition(
                &mut preview,
                ConditionState::False,
                "PreviewExpired",
                "derived preview resources were removed",
            );
            let routes = snapshot.child_routes.values().cloned().collect::<Vec<_>>();
            return self
                .persist(
                    context,
                    &preview,
                    &snapshot,
                    None,
                    &[],
                    &routes,
                    Action::Requeue(Duration::ZERO),
                )
                .await;
        }
        Ok(Action::Done)
    }

    async fn fail(
        &self,
        mut preview: Preview,
        context: &ReconcileContext,
        snapshot: &PreviewSnapshot,
        reason: &str,
        message: String,
    ) -> Result<Action, ReconcileError> {
        preview.status.phase = PreviewPhase::Failed;
        preview.status.teardown_at = None;
        self.set_condition(&mut preview, ConditionState::False, reason, &message);
        self.write(context, &preview, snapshot, None, &[], &[], Action::Done)
            .await
    }

    async fn persist(
        &self,
        context: &ReconcileContext,
        preview: &Preview,
        snapshot: &PreviewSnapshot,
        service: Option<&kernel_api::Service>,
        routes: &[kernel_api::IngressRoute],
        stale_routes: &[crate::snapshot::StoredResource<kernel_api::IngressRoute>],
        applied: Action,
    ) -> Result<Action, ReconcileError> {
        self.write(
            context,
            preview,
            snapshot,
            service,
            routes,
            stale_routes,
            applied,
        )
        .await
    }

    #[allow(clippy::too_many_arguments)]
    async fn write(
        &self,
        context: &ReconcileContext,
        preview: &Preview,
        snapshot: &PreviewSnapshot,
        service: Option<&kernel_api::Service>,
        routes: &[kernel_api::IngressRoute],
        stale_routes: &[crate::snapshot::StoredResource<kernel_api::IngressRoute>],
        applied: Action,
    ) -> Result<Action, ReconcileError> {
        match self
            .writer
            .apply(
                context.store(),
                context.observed_version(),
                &preview.meta.id,
                snapshot,
                Some(preview),
                service,
                routes,
                stale_routes,
            )
            .await
            .map_err(classify)?
        {
            PreviewWriteOutcome::Applied | PreviewWriteOutcome::Noop => Ok(applied),
            PreviewWriteOutcome::Conflict => Ok(Action::Requeue(CONFLICT_RETRY)),
        }
    }

    fn set_condition(
        &self,
        preview: &mut Preview,
        state: ConditionState,
        reason: &str,
        message: &str,
    ) {
        let previous = preview
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
        preview
            .status
            .conditions
            .retain(|condition| condition.condition_type.0 != READY_CONDITION);
        preview.status.conditions.push(Condition {
            condition_type: ConditionType(READY_CONDITION.to_string()),
            state,
            reason: ConditionReason(reason.to_string()),
            message: message.to_string(),
            observed_generation: preview.meta.generation,
            last_transition_time: transitioned_at,
        });
    }
}

#[async_trait]
impl Reconciler for PreviewReconciler {
    type Id = PreviewId;
    type Spec = PreviewSpec;
    type Status = PreviewStatus;

    const KIND: &'static str = "Preview";
    const FINALIZER: Option<&'static str> = Some("preview.maestro.dev/resources");

    async fn reconcile(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.converge(resource, &context).await
    }

    async fn finalize(
        &self,
        resource: Object<Self::Id, Self::Spec, Self::Status>,
        context: ReconcileContext,
    ) -> Result<Action, ReconcileError> {
        self.finalize_preview(resource, &context).await
    }
}

fn classify(error: PreviewError) -> ReconcileError {
    match error {
        PreviewError::Controller(error) => ReconcileError::Infrastructure(error),
        error => ReconcileError::Terminal {
            reason: "PreviewReconcileFailed".to_string(),
            message: error.to_string(),
        },
    }
}
