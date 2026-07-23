use kernel_api::{
    ArtifactTemplate, Build, BuildPhase, BuildSource, BuildStatus, DeploymentId, DeploymentPhase,
    Generation, RolloutState, Timestamp,
};

use super::plan_support::*;
use crate::plan;

#[test]
fn creates_one_stable_deployment_per_service_generation() {
    let input = input(service(Generation(7), RolloutState::Active), Vec::new());
    let first = plan(input.clone()).expect("first plan");
    let second = plan(input).expect("second plan");
    assert_eq!(first.create_deployments, second.create_deployments);
    let deployment = first.create_deployments.first().expect("deployment");
    assert_eq!(deployment.spec.service_generation, Generation(7));
    assert_eq!(deployment.status.phase, DeploymentPhase::Queued);
    assert!(deployment.spec.build_id.is_none());
}

#[test]
fn watched_commit_creates_a_pinned_deployment_in_the_same_service_generation() {
    let mut service = service(Generation(7), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let ArtifactTemplate::Build { template } = &mut service.spec.artifact else {
        return;
    };
    template.watch = true;
    let initial = plan(input(service.clone(), Vec::new()))
        .expect("initial deployment")
        .create_deployments
        .remove(0);
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_WATCH_REVISION_ANNOTATION.to_string()),
        "0123456789abcdef0123456789abcdef01234567".to_string(),
    );

    let watched = plan(input(service, vec![initial.clone()]))
        .expect("watched deployment")
        .create_deployments
        .remove(0);

    assert_ne!(watched.meta.id, initial.meta.id);
    assert_eq!(watched.spec.service_generation, Generation(7));
    let ArtifactTemplate::Build { template } = watched.spec.service.artifact else {
        return;
    };
    assert_eq!(
        template.source,
        BuildSource::Git {
            repository: "https://example.test/repo.git".to_string(),
            revision: "0123456789abcdef0123456789abcdef01234567".to_string(),
        }
    );
}

#[test]
fn watched_commit_reuses_an_equivalent_migrated_deployment() {
    let mut service = service(Generation(7), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let ArtifactTemplate::Build { template } = &mut service.spec.artifact else {
        return;
    };
    template.watch = true;
    let revision = "0123456789abcdef0123456789abcdef01234567";
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_WATCH_REVISION_ANNOTATION.to_string()),
        revision.to_string(),
    );
    let mut migrated = deployment(&service, DeploymentPhase::Ready);
    migrated.meta.id = DeploymentId::new("legacy-deployment").expect("legacy deployment id");
    let ArtifactTemplate::Build { template } = &mut migrated.spec.service.artifact else {
        return;
    };
    let BuildSource::Git {
        revision: captured, ..
    } = &mut template.source
    else {
        return;
    };
    *captured = revision.to_string();

    let result = plan(input(service, vec![migrated])).expect("reuse migrated deployment");

    assert!(result.create_deployments.is_empty());
}

#[test]
fn existing_watched_commit_advances_without_recreating_its_deployment() {
    let mut service = service(Generation(7), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let ArtifactTemplate::Build { template } = &mut service.spec.artifact else {
        return;
    };
    template.watch = true;
    let mut initial = plan(input(service.clone(), Vec::new()))
        .expect("initial deployment")
        .create_deployments
        .remove(0);
    initial.status.phase = DeploymentPhase::Ready;
    service.status.active_deployment_id = Some(initial.meta.id.clone());
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_WATCH_REVISION_ANNOTATION.to_string()),
        "0123456789abcdef0123456789abcdef01234567".to_string(),
    );
    let mut watched = plan(input(service.clone(), vec![initial.clone()]))
        .expect("watched deployment")
        .create_deployments
        .remove(0);

    let next = plan(input(
        service.clone(),
        vec![initial.clone(), watched.clone()],
    ))
    .expect("advance watched deployment");

    assert!(next.create_deployments.is_empty());
    assert_eq!(next.create_builds.len(), 1);
    assert_eq!(next.deployment_updates.len(), 1);
    assert_eq!(next.deployment_updates[0].id, watched.meta.id);
    assert_eq!(
        next.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );

    watched.status.phase = DeploymentPhase::Ready;
    let activated =
        plan(input(service, vec![initial, watched.clone()])).expect("activate watched deployment");
    assert_eq!(activated.service_updates.len(), 1);
    assert_eq!(
        activated.service_updates[0].status.active_deployment_id,
        Some(watched.meta.id)
    );
}

#[test]
fn frozen_build_stays_queued_then_unfreeze_creates_its_build() {
    let mut frozen = service(Generation(1), RolloutState::Frozen);
    frozen.spec.artifact = build_artifact();
    let created = plan(input(frozen.clone(), Vec::new())).expect("create deployment");
    let deployment = created.create_deployments[0].clone();
    assert!(deployment.spec.build_id.is_some());

    let frozen_plan = plan(input(frozen.clone(), vec![deployment.clone()])).expect("frozen plan");
    assert!(frozen_plan.deployment_updates.is_empty());
    assert!(frozen_plan.create_builds.is_empty());

    frozen.status.rollout = RolloutState::Active;
    let active = plan(input(frozen, vec![deployment])).expect("active plan");
    assert_eq!(
        active.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );
    assert_eq!(active.create_builds.len(), 1);
}

#[test]
fn forced_frozen_rollout_consumes_only_its_captured_generation() {
    let mut frozen = service(Generation(1), RolloutState::Frozen);
    frozen.status.rollout_bypass_generation = Some(Generation(1));
    let created = plan(input(frozen.clone(), Vec::new())).expect("create forced deployment");
    let mut forced = created.create_deployments[0].clone();
    assert!(forced.spec.bypass_rollout_freeze);

    let advancing =
        plan(input(frozen.clone(), vec![forced.clone()])).expect("advance forced deployment");
    assert_eq!(
        advancing.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );
    assert_eq!(advancing.service_updates.len(), 1);
    assert_eq!(
        advancing.service_updates[0]
            .status
            .rollout_bypass_generation,
        None
    );
    assert_eq!(
        advancing.service_updates[0].status.rollout,
        RolloutState::Frozen
    );

    forced.status = advancing.deployment_updates[0].status.clone();
    frozen.status = advancing.service_updates[0].status.clone();
    frozen.meta.generation = Generation(2);
    frozen.spec.version = "2.0.0".to_string();
    let next =
        plan(input(frozen.clone(), vec![forced.clone()])).expect("create later frozen deployment");
    let queued = next.create_deployments[0].clone();
    assert!(!queued.spec.bypass_rollout_freeze);

    let held =
        plan(input(frozen, vec![forced, queued.clone()])).expect("hold later frozen deployment");
    assert!(
        held.deployment_updates
            .iter()
            .all(|update| update.id != queued.meta.id)
    );
}

#[test]
fn cancel_goal_transitions_queued_deployment_without_mutating_status_directly() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Queued);
    deployment.spec.goal = kernel_api::DeploymentGoal::Cancel;

    let canceled = plan(input(service, vec![deployment])).expect("cancel plan");
    assert_eq!(canceled.deployment_updates.len(), 1);
    assert_eq!(
        canceled.deployment_updates[0].status.phase,
        DeploymentPhase::Canceled
    );
}

#[test]
fn remove_goal_clears_active_service_then_drains_to_removed() {
    let mut service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Ready);
    deployment.spec.goal = kernel_api::DeploymentGoal::Remove;
    service.status.active_deployment_id = Some(deployment.meta.id.clone());

    let draining = plan(input(service.clone(), vec![deployment.clone()])).expect("drain plan");
    assert_eq!(
        draining.deployment_updates[0].status.phase,
        DeploymentPhase::Draining
    );
    assert_eq!(
        draining.service_updates[0].status.active_deployment_id,
        None
    );

    deployment.status = draining.deployment_updates[0].status.clone();
    service.status = draining.service_updates[0].status.clone();
    let mut after_grace = input(service, vec![deployment]);
    after_grace.now = Timestamp(70_000);
    assert_eq!(
        plan(after_grace).expect("remove plan").deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Removed
    );
}

#[test]
fn successful_build_publishes_digest_without_skipping_assignment_readiness() {
    let mut svc = service(Generation(1), RolloutState::Active);
    svc.spec.artifact = build_artifact();
    let mut deployment = deployment(&svc, DeploymentPhase::Building);
    let mut build = Build {
        meta: metadata(
            deployment.spec.build_id.clone().expect("build id"),
            Generation(1),
        ),
        spec: kernel_api::BuildSpec {
            service_id: svc.meta.id.clone(),
            deployment_id: deployment.meta.id.clone(),
            template: build_template(),
        },
        status: BuildStatus {
            phase: BuildPhase::Succeeded,
            image_digest: Some("registry.test/api@sha256:abc".to_string()),
            source_revision: Some("abc".to_string()),
            conditions: Vec::new(),
        },
    };
    let mut snapshot = input(svc, vec![deployment.clone()]);
    snapshot.builds = vec![build.clone()];
    let waiting = plan(snapshot).expect("build output plan");
    assert_eq!(
        waiting.deployment_updates[0].status.image_digest.as_deref(),
        Some("registry.test/api@sha256:abc")
    );
    assert_eq!(
        waiting.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );

    deployment.status = waiting.deployment_updates[0].status.clone();
    build.status.phase = BuildPhase::Succeeded;
    let assigned = assignment(&deployment, "assignment-1", 1);
    let mut snapshot = input(
        service(Generation(1), RolloutState::Active),
        vec![deployment.clone()],
    );
    snapshot.services[0].spec.artifact = build_artifact();
    snapshot.assignments = vec![assigned.clone()];
    snapshot.replicas = vec![replica(&deployment, &assigned, DeploymentPhase::Ready, 0)];
    snapshot.builds = vec![build];
    assert_eq!(
        plan(snapshot).expect("ready build plan").deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Ready
    );
}

#[test]
fn canceled_deployment_remains_in_history_until_service_deletion() {
    let service = service(Generation(1), RolloutState::Active);
    let canceled = deployment(&service, DeploymentPhase::Canceled);
    assert!(
        plan(input(service, vec![canceled]))
            .expect("canceled history")
            .deployment_updates
            .is_empty()
    );
}

#[test]
fn readiness_requires_the_exact_current_assignment() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Building);
    let current = assignment(&deployment, "assignment-current", 2);
    let stale = assignment(&deployment, "assignment-stale", 1);
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![stale.clone(), current.clone()];
    snapshot.replicas = vec![replica(&deployment, &stale, DeploymentPhase::Ready, 0)];
    let pending = plan(snapshot).expect("pending plan");
    assert_eq!(
        pending.deployment_updates[0].status.phase,
        DeploymentPhase::PendingReady
    );

    deployment.status.phase = DeploymentPhase::PendingReady;
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![stale, current.clone()];
    snapshot.replicas = vec![replica(&deployment, &current, DeploymentPhase::Ready, 0)];
    let ready = plan(snapshot).expect("ready plan");
    assert_eq!(
        ready.deployment_updates[0].status.phase,
        DeploymentPhase::Ready
    );
    assert_eq!(
        ready.deployment_updates[0].status.ready_at,
        Some(Timestamp(40_000))
    );
}

#[test]
fn every_current_slot_must_exhaust_its_configured_restart_budget() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.replicas = 2;
    service.spec.max_restarts = Some(3);
    let deployment = deployment(&service, DeploymentPhase::PendingReady);
    let first = assignment_slot(&deployment, "assignment-0", 0, 1);
    let second = assignment_slot(&deployment, "assignment-1", 1, 1);
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![first.clone(), second.clone()];
    snapshot.replicas = vec![
        replica(&deployment, &first, DeploymentPhase::Crashed, 3),
        replica(&deployment, &second, DeploymentPhase::Crashed, 2),
    ];
    assert!(
        plan(snapshot)
            .expect("not exhausted")
            .deployment_updates
            .is_empty()
    );

    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![first.clone(), second.clone()];
    snapshot.replicas = vec![
        replica(&deployment, &first, DeploymentPhase::Crashed, 3),
        replica(&deployment, &second, DeploymentPhase::Crashed, 3),
    ];
    assert_eq!(
        plan(snapshot).expect("exhausted").deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Crashed
    );
}

#[test]
fn active_traffic_acknowledgement_drains_only_superseded_deployments() {
    let mut service = service(Generation(2), RolloutState::Active);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::Ready,
    );
    let incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::Ready,
    );
    service.status.active_deployment_id = Some(old.meta.id.clone());
    let mut first = input(service.clone(), vec![old.clone(), incoming.clone()]);
    first.traffic_generations = Vec::new();
    let activated = plan(first).expect("activate plan");
    assert_eq!(
        activated.service_updates[0].status.active_deployment_id,
        Some(incoming.meta.id.clone())
    );
    assert!(activated.deployment_updates.is_empty());

    service.status.active_deployment_id = Some(incoming.meta.id.clone());
    let mut acknowledged = input(service, vec![old.clone(), incoming.clone()]);
    acknowledged.traffic_generations = vec![traffic(&incoming)];
    let drained = plan(acknowledged).expect("drain plan");
    assert_eq!(drained.deployment_updates.len(), 1);
    assert_eq!(drained.deployment_updates[0].id, old.meta.id);
    assert_eq!(
        drained.deployment_updates[0].status.phase,
        DeploymentPhase::Draining
    );
    assert_eq!(
        drained.deployment_updates[0].status.draining_at,
        Some(Timestamp(40_000))
    );
}

#[test]
fn newer_watched_deployment_activates_and_drains_within_one_service_generation() {
    let mut service = service(Generation(2), RolloutState::Active);
    let mut old = deployment_generation(
        &service,
        "deployment-old",
        Generation(2),
        DeploymentPhase::Ready,
    );
    old.status.created_at = Timestamp(10_000);
    let mut incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::Ready,
    );
    incoming.status.created_at = Timestamp(20_000);
    service.status.active_deployment_id = Some(old.meta.id.clone());

    let activated = plan(input(service.clone(), vec![old.clone(), incoming.clone()]))
        .expect("activate watched deployment");
    assert_eq!(
        activated
            .service_updates
            .first()
            .map(|update| update.status.active_deployment_id.clone()),
        Some(Some(incoming.meta.id.clone()))
    );

    service.status.active_deployment_id = Some(incoming.meta.id.clone());
    let mut acknowledged = input(service, vec![old.clone(), incoming.clone()]);
    acknowledged.traffic_generations = vec![traffic(&incoming)];
    let drained = plan(acknowledged).expect("drain watched predecessor");
    assert_eq!(drained.deployment_updates.len(), 1);
    let update = drained
        .deployment_updates
        .first()
        .expect("watched drain update");
    assert_eq!(update.id, old.meta.id);
    assert_eq!(update.status.phase, DeploymentPhase::Draining);
}

#[test]
fn active_old_traffic_does_not_drain_a_newer_queued_candidate() {
    let mut service = service(Generation(2), RolloutState::Active);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::Ready,
    );
    let incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::Queued,
    );
    service.status.active_deployment_id = Some(old.meta.id.clone());
    let mut snapshot = input(service, vec![old.clone(), incoming.clone()]);
    snapshot.traffic_generations = vec![traffic(&old)];

    let advancing = plan(snapshot).expect("advance candidate");

    assert_eq!(advancing.deployment_updates.len(), 1);
    assert_eq!(advancing.deployment_updates[0].id, incoming.meta.id);
    assert_eq!(
        advancing.deployment_updates[0].status.phase,
        DeploymentPhase::Building
    );
}

#[test]
fn draining_waits_for_grace_and_assignment_removal() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Draining);
    deployment.status.draining_at = Some(Timestamp(10_000));
    let assignment = assignment(&deployment, "assignment-1", 1);
    let mut held = input(service.clone(), vec![deployment.clone()]);
    held.assignments = vec![assignment];
    assert!(plan(held).expect("held").deployment_updates.is_empty());

    let removed = plan(input(service, vec![deployment])).expect("removed");
    assert_eq!(
        removed.deployment_updates[0].status.phase,
        DeploymentPhase::Removed
    );
}

#[test]
fn deleting_service_cancels_queued_and_drains_serving_deployments() {
    let mut service = service(Generation(2), RolloutState::Active);
    service.meta.deletion_timestamp = Some(Timestamp(39_000));
    let queued = deployment_generation(
        &service,
        "deployment-queued",
        Generation(2),
        DeploymentPhase::Queued,
    );
    let ready = deployment_generation(
        &service,
        "deployment-ready",
        Generation(1),
        DeploymentPhase::Ready,
    );
    let result = plan(input(service, vec![queued.clone(), ready.clone()])).expect("delete plan");
    assert_eq!(result.create_deployments.len(), 0);
    assert_eq!(result.deployment_updates.len(), 2);
    assert_eq!(
        result
            .deployment_updates
            .iter()
            .find(|update| update.id == queued.meta.id)
            .expect("queued update")
            .status
            .phase,
        DeploymentPhase::Canceled
    );
    assert_eq!(
        result
            .deployment_updates
            .iter()
            .find(|update| update.id == ready.meta.id)
            .expect("ready update")
            .status
            .phase,
        DeploymentPhase::Draining
    );
}

#[test]
fn removed_children_are_collected_before_service_finalization() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let created = plan(input(service.clone(), Vec::new())).expect("deployment creation");
    let mut deployment = created.create_deployments[0].clone();
    let building = plan(input(service.clone(), vec![deployment.clone()])).expect("build creation");
    let build = building.create_builds[0].clone();
    deployment.status.phase = DeploymentPhase::Removed;
    service.meta.deletion_timestamp = Some(Timestamp(40_000));
    let assignment = assignment(&deployment, "assignment-old", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready, 0);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.builds = vec![build.clone()];
    snapshot.replicas = vec![replica.clone()];

    let collected = plan(snapshot).expect("child collection");
    assert_eq!(collected.delete_deployments, vec![deployment.meta.id]);
    assert_eq!(collected.delete_builds, vec![build.meta.id]);
    assert_eq!(collected.delete_replicas, vec![replica.meta.id]);
    assert!(collected.deployment_updates.is_empty());
}
