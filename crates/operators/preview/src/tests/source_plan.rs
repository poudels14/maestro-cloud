use kernel_api::{
    ArtifactTemplate, BuildSource, Generation, PreviewPhase, PullRequestState, ServiceId, Timestamp,
};

use crate::{
    PreviewFeedbackKind, PreviewSourcePlanError, PullRequest, PullRequestReadiness,
    RepositoryPullRequests, plan_preview_sources,
};

use super::support::{base_service, metadata, preview};

#[test]
fn admits_oldest_candidates_globally_and_reports_exclusions() {
    let api = service("api", "https://github.com/acme/api.git");
    let worker = service("worker", "git@github.com:acme/worker.git");
    let repositories = vec![
        repository(
            "acme/api",
            vec![
                pull_request(3, 3_000, PullRequestReadiness::Ready, "acme/api"),
                pull_request(4, 1_000, PullRequestReadiness::Draft, "acme/api"),
            ],
        ),
        repository(
            "acme/worker",
            vec![
                pull_request(8, 2_000, PullRequestReadiness::Ready, "acme/worker"),
                pull_request(9, 4_000, PullRequestReadiness::Ready, "other/fork"),
            ],
        ),
    ];

    let plan =
        plan_preview_sources(&[api, worker], &[], &repositories, Timestamp(5_000), 1).unwrap();

    assert_eq!(plan.creates.len(), 1);
    let created = plan.creates.first().unwrap();
    assert_eq!(created.meta.id.as_str(), "worker-pr-8");
    assert_eq!(created.spec.repository, "acme/worker");
    assert_eq!(created.spec.head_reference, "feature-8");
    assert_eq!(created.spec.author, "author-8");
    assert_eq!(created.spec.expires_at, Timestamp(3_602_000));
    assert_eq!(
        feedback_count(&plan.feedback, PreviewFeedbackKind::QuotaExceeded),
        1
    );
    assert_eq!(
        feedback_count(&plan.feedback, PreviewFeedbackKind::Ineligible),
        0
    );
    let queued = plan
        .feedback
        .iter()
        .find(|item| item.kind == PreviewFeedbackKind::QuotaExceeded)
        .unwrap();
    assert_eq!(queued.base_service_id.as_str(), "api");
    assert_eq!(queued.service_id.as_str(), "api-pr-3");
    assert_eq!(queued.pull_request_number, 3);
    assert_eq!(queued.head_revision, format!("{:040x}", 3));
}

#[test]
fn updates_pushes_and_reopens_without_changing_preview_identity() {
    let base = service("api", "https://github.com/acme/api");
    let mut current = preview();
    current.meta.generation = Generation(7);
    current.meta.deletion_timestamp = Some(Timestamp(10_000));
    current.status.pull_request_state = PullRequestState::Closed;
    current.status.phase = PreviewPhase::Closing;
    current.status.teardown_at = Some(Timestamp(20_000));
    let open = pull_request(42, 1_000, PullRequestReadiness::Ready, "ACME/API");
    let repositories = vec![repository("ACME/API", vec![open.clone()])];

    let plan =
        plan_preview_sources(&[base], &[current], &repositories, Timestamp(15_000), 3).unwrap();

    assert!(plan.creates.is_empty());
    assert_eq!(plan.updates.len(), 1);
    let desired = plan.updates.first().unwrap();
    assert_eq!(desired.meta.id.as_str(), "api-pr-42");
    assert_eq!(desired.spec.service_id.as_str(), "api-pr-42");
    assert_eq!(desired.spec.head_reference, open.head_reference);
    assert_eq!(desired.spec.author, open.author);
    assert_eq!(desired.spec.head_revision, open.head_revision);
    assert_eq!(desired.meta.generation, Generation(8));
    assert_eq!(desired.meta.deletion_timestamp, None);
    assert_eq!(desired.status.pull_request_state, PullRequestState::Open);
    assert_eq!(desired.status.phase, PreviewPhase::Pending);
    assert_eq!(desired.status.teardown_at, None);
    assert_eq!(
        plan.feedback.first().unwrap().kind,
        PreviewFeedbackKind::Reopened
    );
}

#[test]
fn reports_the_current_revision_as_ready_and_a_changed_revision_as_updating() {
    let base = service("api", "https://github.com/acme/api");
    let mut current = preview();
    current.status.phase = PreviewPhase::Active;
    let mut open = pull_request(42, 1_000, PullRequestReadiness::Ready, "acme/api");
    open.head_revision.clone_from(&current.spec.head_revision);

    let ready = plan_preview_sources(
        std::slice::from_ref(&base),
        std::slice::from_ref(&current),
        &[repository("acme/api", vec![open.clone()])],
        Timestamp(2_000),
        3,
    )
    .unwrap();
    let feedback = ready.feedback.first().unwrap();
    assert_eq!(feedback.kind, PreviewFeedbackKind::Ready);
    assert_eq!(feedback.head_revision, current.spec.head_revision);
    assert_eq!(feedback.service_id, current.spec.service_id);

    open.head_revision = "fedcba9876543210fedcba9876543210fedcba98".to_string();
    let updating = plan_preview_sources(
        &[base],
        &[current],
        &[repository("acme/api", vec![open.clone()])],
        Timestamp(2_000),
        3,
    )
    .unwrap();
    assert_eq!(
        updating.feedback.first().unwrap().kind,
        PreviewFeedbackKind::Updating
    );
    assert_eq!(
        updating.feedback.first().unwrap().head_revision,
        open.head_revision
    );
    assert_eq!(
        updating.updates.first().unwrap().status.phase,
        PreviewPhase::Pending
    );
}

#[test]
fn a_new_revision_requeues_a_failed_preview() {
    let base = service("api", "https://github.com/acme/api");
    let mut current = preview();
    current.status.phase = PreviewPhase::Failed;
    let open = pull_request(42, 1_000, PullRequestReadiness::Ready, "acme/api");

    let plan = plan_preview_sources(
        &[base],
        &[current],
        &[repository("acme/api", vec![open.clone()])],
        Timestamp(2_000),
        3,
    )
    .unwrap();

    let desired = plan.updates.first().unwrap();
    assert_eq!(desired.meta.generation, Generation(2));
    assert_eq!(desired.spec.head_revision, open.head_revision);
    assert_eq!(desired.status.phase, PreviewPhase::Pending);
    assert_eq!(
        plan.feedback.first().unwrap().kind,
        PreviewFeedbackKind::Updating
    );
}

#[test]
fn closes_only_after_a_successful_repository_snapshot() {
    let base = service("api", "https://github.com/acme/api");
    let current = preview();

    let unavailable = plan_preview_sources(
        std::slice::from_ref(&base),
        std::slice::from_ref(&current),
        &[],
        Timestamp(50_000),
        3,
    )
    .unwrap();
    assert!(unavailable.updates.is_empty());

    let fetched = plan_preview_sources(
        &[base],
        &[current],
        &[repository("acme/api", Vec::new())],
        Timestamp(50_000),
        3,
    )
    .unwrap();
    assert_eq!(fetched.updates.len(), 1);
    assert_eq!(
        fetched.updates.first().unwrap().status.pull_request_state,
        PullRequestState::Closed
    );
    assert_eq!(
        fetched.updates.first().unwrap().meta.deletion_timestamp,
        Some(Timestamp(50_000))
    );
    assert_eq!(
        fetched.feedback.first().unwrap().kind,
        PreviewFeedbackKind::Closing
    );
}

#[test]
fn invalid_or_disabled_bases_expire_existing_previews_without_blocking_others() {
    let mut invalid = service("api", "https://gitlab.com/acme/api");
    invalid.meta.id = ServiceId::new("invalid").unwrap();
    let valid = service("worker", "https://github.com/acme/worker");
    let mut orphan = preview();
    orphan.spec.base_service_id = ServiceId::new("invalid").unwrap();
    let repositories = vec![repository(
        "acme/worker",
        vec![pull_request(
            8,
            2_000,
            PullRequestReadiness::Ready,
            "acme/worker",
        )],
    )];

    let plan = plan_preview_sources(
        &[invalid, valid],
        &[orphan],
        &repositories,
        Timestamp(9_000),
        3,
    )
    .unwrap();

    assert_eq!(plan.diagnostics.len(), 1);
    assert_eq!(
        plan.diagnostics.first().unwrap().service_id.as_str(),
        "invalid"
    );
    assert_eq!(plan.updates.len(), 1);
    assert_eq!(
        plan.updates.first().unwrap().spec.expires_at,
        Timestamp(9_000)
    );
    assert_eq!(plan.creates.len(), 1);
    assert_eq!(
        plan.creates.first().unwrap().meta.id.as_str(),
        "worker-pr-8"
    );
}

#[test]
fn closing_previews_continue_to_consume_quota() {
    let base = service("api", "https://github.com/acme/api");
    let mut closing = preview();
    closing.meta.deletion_timestamp = Some(Timestamp(4_000));
    let repositories = vec![repository(
        "acme/api",
        vec![pull_request(
            43,
            2_000,
            PullRequestReadiness::Ready,
            "acme/api",
        )],
    )];

    let plan =
        plan_preview_sources(&[base], &[closing], &repositories, Timestamp(5_000), 1).unwrap();

    assert!(plan.creates.is_empty());
    assert_eq!(
        feedback_count(&plan.feedback, PreviewFeedbackKind::QuotaExceeded),
        1
    );
}

#[test]
fn skips_pull_requests_that_exhausted_their_lifetime_before_discovery() {
    let base = service("api", "https://github.com/acme/api");
    let repositories = vec![repository(
        "acme/api",
        vec![pull_request(
            42,
            1_000,
            PullRequestReadiness::Ready,
            "acme/api",
        )],
    )];

    let plan = plan_preview_sources(&[base], &[], &repositories, Timestamp(3_602_000), 1).unwrap();

    assert!(plan.creates.is_empty());
    assert!(plan.feedback.is_empty());
}

#[test]
fn rejects_ambiguous_snapshot_inputs() {
    let base = service("api", "https://github.com/acme/api");
    let snapshots = vec![
        repository("acme/api", Vec::new()),
        repository("ACME/API", Vec::new()),
    ];

    let error = plan_preview_sources(&[base], &[], &snapshots, Timestamp(0), 1).unwrap_err();

    assert_eq!(
        error,
        PreviewSourcePlanError::DuplicateRepository {
            repository: "acme/api".to_string()
        }
    );
}

fn service(id: &str, repository: &str) -> kernel_api::Service {
    let mut service = base_service();
    service.meta = metadata(ServiceId::new(id).unwrap());
    let ArtifactTemplate::Build { template } = &mut service.spec.artifact else {
        unreachable!();
    };
    let BuildSource::Git {
        repository: source, ..
    } = &mut template.source
    else {
        unreachable!();
    };
    source.clone_from(&repository.to_string());
    service.status.rollout = kernel_api::RolloutState::Active;
    service
}

fn repository(name: &str, pull_requests: Vec<PullRequest>) -> RepositoryPullRequests {
    RepositoryPullRequests {
        repository: name.to_string(),
        pull_requests,
    }
}

fn pull_request(
    number: u64,
    created_at: i64,
    readiness: PullRequestReadiness,
    head_repository: &str,
) -> PullRequest {
    PullRequest {
        number,
        title: format!("Pull request {number}"),
        author: format!("author-{number}"),
        readiness,
        created_at: Timestamp(created_at),
        head_reference: format!("feature-{number}"),
        head_revision: format!("{number:040x}"),
        head_repository: Some(head_repository.to_string()),
    }
}

fn feedback_count(feedback: &[crate::PreviewFeedback], kind: PreviewFeedbackKind) -> usize {
    feedback.iter().filter(|item| item.kind == kind).count()
}
