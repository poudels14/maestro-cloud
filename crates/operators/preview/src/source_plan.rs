use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    Generation, Object, ObjectMeta, OwnerReference, Ownership, Preview, PreviewId, PreviewPhase,
    PreviewPolicy, PreviewSpec, PreviewStatus, PullRequestState, ResourceId, ResourceKind,
    ResourceName, Service, ServiceId, Timestamp,
};
use sha2::{Digest, Sha256};

use crate::{PullRequest, PullRequestReadiness};

mod input;

use input::{PreviewBase, existing_previews, preview_bases, repository_snapshots};

/// One successfully fetched repository snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RepositoryPullRequests {
    /// Repository identity in `owner/name` form.
    pub repository: String,
    /// Every currently open pull request returned by the source API.
    pub pull_requests: Vec<PullRequest>,
}

/// Native GitHub deployment state to publish after preview state is persisted.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PreviewFeedbackKind {
    /// A new preview was admitted and will be derived.
    Creating,
    /// An existing preview is being updated to its current head.
    Updating,
    /// The current preview deployment is active.
    Ready,
    /// The current preview deployment failed to converge.
    Failed,
    /// A closing preview was restored after its pull request reopened.
    Reopened,
    /// The pull request closed and grace-period teardown was requested.
    Closing,
    /// An existing preview became ineligible and is being removed.
    Ineligible,
    /// Eligible work was skipped because all global preview slots are occupied.
    QuotaExceeded,
}

/// GitHub deployment feedback emitted by the planner.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewFeedback {
    /// Base service whose preview lifecycle owns this feedback.
    pub base_service_id: ServiceId,
    /// Repository identity in `owner/name` form.
    pub repository: String,
    /// Repository-local pull-request number.
    pub pull_request_number: u64,
    /// Immutable pull-request head commit represented by this deployment.
    pub head_revision: String,
    /// Derived service identity used to construct the preview URL.
    pub service_id: ServiceId,
    /// Current source lifecycle outcome.
    pub kind: PreviewFeedbackKind,
}

/// A malformed preview-enabled service skipped during source discovery.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSourceDiagnostic {
    /// Service whose source could not be interpreted.
    pub service_id: ServiceId,
    /// Operator-facing validation detail.
    pub message: String,
}

/// Deterministic mutations and feedback for one complete source snapshot.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSourcePlan {
    /// New preview resources admitted by the global quota.
    pub creates: Vec<Preview>,
    /// Existing preview resources whose source lifecycle changed.
    pub updates: Vec<Preview>,
    /// GitHub deployment feedback to publish only after corresponding writes succeed.
    pub feedback: Vec<PreviewFeedback>,
    /// Isolated service validation failures that did not stop other repositories.
    pub diagnostics: Vec<PreviewSourceDiagnostic>,
}

/// Whole-snapshot invariants that make a source plan unsafe to apply.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum PreviewSourcePlanError {
    /// The configured global quota cannot admit any preview.
    #[error("maximum concurrent previews must be greater than zero")]
    EmptyQuota,
    /// More than one Preview claims the same base-service pull request.
    #[error("duplicate previews claim service `{service_id}` pull request #{pull_request_number}")]
    DuplicatePreview {
        /// Base service identity.
        service_id: ServiceId,
        /// Conflicting pull-request number.
        pull_request_number: u64,
    },
    /// More than one fetched result claims the same repository.
    #[error("duplicate pull-request snapshot for repository `{repository}`")]
    DuplicateRepository {
        /// Conflicting normalized repository identity.
        repository: String,
    },
    /// A generated owner reference or preview identity was invalid.
    #[error("cannot construct preview resource: {message}")]
    InvalidGeneratedResource {
        /// Validation detail.
        message: String,
    },
}

#[derive(Clone)]
struct Candidate<'a> {
    base: PreviewBase<'a>,
    pull_request: &'a PullRequest,
}

/// Plans source-driven preview creation, updates, closing, and global quota.
pub fn plan_preview_sources(
    services: &[Service],
    previews: &[Preview],
    repositories: &[RepositoryPullRequests],
    now: Timestamp,
    max_concurrent_previews: usize,
) -> Result<PreviewSourcePlan, PreviewSourcePlanError> {
    if max_concurrent_previews == 0 {
        return Err(PreviewSourcePlanError::EmptyQuota);
    }
    let (bases, diagnostics) = preview_bases(services);
    let repository_snapshots = repository_snapshots(repositories)?;
    let existing = existing_previews(previews)?;
    let mut plan = PreviewSourcePlan {
        creates: Vec::new(),
        updates: Vec::new(),
        feedback: Vec::new(),
        diagnostics,
    };

    reconcile_existing(&bases, &repository_snapshots, &existing, now, &mut plan);
    let mut candidates = new_candidates(&bases, &repository_snapshots, &existing, now);
    candidates.sort_by(|left, right| {
        left.pull_request
            .created_at
            .cmp(&right.pull_request.created_at)
            .then_with(|| left.base.service.meta.id.cmp(&right.base.service.meta.id))
            .then_with(|| left.pull_request.number.cmp(&right.pull_request.number))
    });
    let available_slots = max_concurrent_previews.saturating_sub(existing.len());
    for (index, candidate) in candidates.into_iter().enumerate() {
        let preview = new_preview(&candidate)?;
        if index < available_slots {
            plan.feedback
                .push(feedback(&preview, PreviewFeedbackKind::Creating));
            plan.creates.push(preview);
        } else {
            plan.feedback
                .push(feedback(&preview, PreviewFeedbackKind::QuotaExceeded));
        }
    }
    Ok(plan)
}

fn reconcile_existing(
    bases: &BTreeMap<ServiceId, PreviewBase<'_>>,
    snapshots: &BTreeMap<String, &RepositoryPullRequests>,
    existing: &BTreeMap<(ServiceId, u64), &Preview>,
    now: Timestamp,
    plan: &mut PreviewSourcePlan,
) {
    for (key, preview) in existing {
        let Some(base) = bases.get(&key.0) else {
            let desired = expire_preview(preview, now);
            if desired != **preview {
                plan.updates.push(desired);
            }
            plan.feedback
                .push(feedback(preview, PreviewFeedbackKind::Ineligible));
            continue;
        };
        let Some(snapshot) = snapshots.get(&base.repository) else {
            continue;
        };
        let open = snapshot
            .pull_requests
            .iter()
            .find(|pull_request| pull_request.number == key.1);
        let Some(pull_request) = open else {
            let desired = close_preview(preview, now);
            if desired != **preview {
                plan.updates.push(desired);
            }
            plan.feedback
                .push(feedback(preview, PreviewFeedbackKind::Closing));
            continue;
        };
        if !eligible(pull_request, &base.repository) || expires_at(pull_request, base.policy) <= now
        {
            let desired = expire_preview(preview, now);
            if desired != **preview {
                plan.updates.push(desired);
            }
            plan.feedback
                .push(feedback(preview, PreviewFeedbackKind::Ineligible));
            continue;
        }
        let was_closing = preview.meta.deletion_timestamp.is_some();
        let desired = update_open_preview(preview, base, pull_request);
        let revision_changed = desired.spec.head_revision != preview.spec.head_revision;
        let kind = if was_closing {
            PreviewFeedbackKind::Reopened
        } else if revision_changed {
            PreviewFeedbackKind::Updating
        } else {
            match desired.status.phase {
                PreviewPhase::Active => PreviewFeedbackKind::Ready,
                PreviewPhase::Failed => PreviewFeedbackKind::Failed,
                PreviewPhase::Pending
                | PreviewPhase::Closing
                | PreviewPhase::Expired
                | PreviewPhase::Canceled => PreviewFeedbackKind::Updating,
            }
        };
        plan.feedback.push(feedback(&desired, kind));
        if desired != **preview {
            plan.updates.push(desired);
        }
    }
}

fn new_candidates<'a>(
    bases: &'a BTreeMap<ServiceId, PreviewBase<'a>>,
    snapshots: &'a BTreeMap<String, &'a RepositoryPullRequests>,
    existing: &BTreeMap<(ServiceId, u64), &Preview>,
    now: Timestamp,
) -> Vec<Candidate<'a>> {
    let mut candidates = Vec::new();
    for base in bases.values() {
        if base.service.meta.deletion_timestamp.is_some()
            || base.service.status.rollout == kernel_api::RolloutState::Frozen
        {
            continue;
        }
        let Some(snapshot) = snapshots.get(&base.repository) else {
            continue;
        };
        for pull_request in &snapshot.pull_requests {
            let key = (base.service.meta.id.clone(), pull_request.number);
            if existing.contains_key(&key) {
                continue;
            }
            if eligible(pull_request, &base.repository)
                && expires_at(pull_request, base.policy) > now
            {
                candidates.push(Candidate {
                    base: base.clone(),
                    pull_request,
                });
            }
        }
    }
    candidates
}

fn new_preview(candidate: &Candidate<'_>) -> Result<Preview, PreviewSourcePlanError> {
    let id_text = preview_identity(
        &candidate.base.service.meta.id,
        candidate.pull_request.number,
    );
    let preview_id = PreviewId::new(id_text.clone()).map_err(generated_error)?;
    let service_id = ServiceId::new(id_text).map_err(generated_error)?;
    let owner_kind = ResourceKind::new("Service").map_err(generated_error)?;
    let policy = candidate.base.policy;
    Ok(Object {
        meta: ObjectMeta {
            id: preview_id,
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: Default::default(),
            generation: Generation(1),
            owner_refs: vec![OwnerReference {
                resource: ResourceId::new(
                    owner_kind,
                    ResourceName::from(candidate.base.service.meta.id.clone()),
                ),
                ownership: Ownership::Controller,
            }],
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: PreviewSpec {
            base_service_id: candidate.base.service.meta.id.clone(),
            repository: candidate.base.repository.clone(),
            pull_request_number: candidate.pull_request.number,
            title: candidate.pull_request.title.clone(),
            head_reference: candidate.pull_request.head_reference.clone(),
            author: candidate.pull_request.author.clone(),
            head_revision: candidate.pull_request.head_revision.clone(),
            service_id,
            close_grace_period_secs: policy.close_grace_period_secs,
            expires_at: expires_at(candidate.pull_request, candidate.base.policy),
        },
        status: PreviewStatus {
            pull_request_state: PullRequestState::Open,
            phase: PreviewPhase::Pending,
            teardown_at: None,
            conditions: Vec::new(),
        },
    })
}

fn update_open_preview(
    current: &Preview,
    base: &PreviewBase<'_>,
    pull_request: &PullRequest,
) -> Preview {
    let policy = base.policy;
    let mut desired = current.clone();
    let revision_changed = current.spec.head_revision != pull_request.head_revision;
    desired.meta.deletion_timestamp = None;
    desired.status.pull_request_state = PullRequestState::Open;
    desired.spec.repository.clone_from(&base.repository);
    desired.spec.title.clone_from(&pull_request.title);
    desired
        .spec
        .head_reference
        .clone_from(&pull_request.head_reference);
    desired.spec.author.clone_from(&pull_request.author);
    desired
        .spec
        .head_revision
        .clone_from(&pull_request.head_revision);
    desired.spec.close_grace_period_secs = policy.close_grace_period_secs;
    desired.spec.expires_at = expires_at(pull_request, base.policy);
    if desired.spec != current.spec {
        desired.meta.generation = Generation(current.meta.generation.0.saturating_add(1));
    }
    if current.meta.deletion_timestamp.is_some() || revision_changed {
        desired.status.phase = PreviewPhase::Pending;
        desired.status.teardown_at = None;
    }
    desired
}

fn close_preview(current: &Preview, now: Timestamp) -> Preview {
    let mut desired = current.clone();
    desired.status.pull_request_state = PullRequestState::Closed;
    desired.meta.deletion_timestamp.get_or_insert(now);
    desired
}

fn expire_preview(current: &Preview, now: Timestamp) -> Preview {
    if current.meta.deletion_timestamp.is_some() {
        return current.clone();
    }
    let mut desired = current.clone();
    desired.spec.expires_at = now;
    if desired.spec != current.spec {
        desired.meta.generation = Generation(current.meta.generation.0.saturating_add(1));
    }
    desired.meta.deletion_timestamp.get_or_insert(now);
    desired
}

fn eligible(pull_request: &PullRequest, repository: &str) -> bool {
    pull_request.readiness == PullRequestReadiness::Ready
        && pull_request
            .head_repository
            .as_deref()
            .is_some_and(|head| head.eq_ignore_ascii_case(repository))
        && !pull_request.head_revision.trim().is_empty()
}

fn expires_at(pull_request: &PullRequest, policy: &PreviewPolicy) -> Timestamp {
    let lifetime_secs = policy.lifetime_secs;
    let lifetime_millis = i64::try_from(lifetime_secs.saturating_mul(1_000)).unwrap_or(i64::MAX);
    Timestamp(pull_request.created_at.0.saturating_add(lifetime_millis))
}

fn preview_identity(base_service_id: &ServiceId, pull_request_number: u64) -> String {
    let friendly = format!("{}-pr-{pull_request_number}", base_service_id.as_str());
    if friendly.len() <= 63
        && friendly
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
    {
        return friendly;
    }
    let mut digest = Sha256::new();
    digest.update(base_service_id.as_str());
    digest.update([0]);
    digest.update(pull_request_number.to_be_bytes());
    let suffix = digest
        .finalize()
        .iter()
        .take(12)
        .map(|byte| format!("{byte:02x}"))
        .collect::<String>();
    format!("preview-{suffix}")
}

fn feedback(preview: &Preview, kind: PreviewFeedbackKind) -> PreviewFeedback {
    PreviewFeedback {
        base_service_id: preview.spec.base_service_id.clone(),
        repository: preview.spec.repository.clone(),
        pull_request_number: preview.spec.pull_request_number,
        head_revision: preview.spec.head_revision.clone(),
        service_id: preview.spec.service_id.clone(),
        kind,
    }
}

fn generated_error(error: impl std::fmt::Display) -> PreviewSourcePlanError {
    PreviewSourcePlanError::InvalidGeneratedResource {
        message: error.to_string(),
    }
}
