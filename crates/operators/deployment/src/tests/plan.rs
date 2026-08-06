use std::collections::BTreeMap;

use kernel_api::{
    ArtifactTemplate, Build, BuildPhase, BuildSource, BuildStatus, Condition, ConditionReason,
    ConditionState, ConditionType, DeploymentId, DeploymentPhase, Generation, IngressRouteId,
    IngressRouteSpec, IngressRouteStatus, Object, OwnerReference, Ownership, ResourceId,
    ResourceKind, ResourceName, RolloutState, SecretValue, ServiceId, TAILSCALE_GATEWAY_SERVICE_ID,
    Timestamp, desired_service_replicas,
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
fn terminal_system_deployment_starts_a_new_rollout() {
    let mut system = service(Generation(7), RolloutState::Active);
    system.meta.id = ServiceId::new(TAILSCALE_GATEWAY_SERVICE_ID).expect("system service id");
    system.spec.replicas = 0;
    let crashed = deployment(&system, DeploymentPhase::Crashed);

    let recovered = plan(input(system.clone(), vec![crashed.clone()]))
        .expect("recover terminal system deployment");
    assert_eq!(recovered.create_deployments.len(), 1);
    let replacement = &recovered.create_deployments[0];
    assert_eq!(replacement.spec.service_generation, Generation(8));
    assert_eq!(replacement.status.phase, DeploymentPhase::Queued);
    assert_ne!(replacement.meta.id, crashed.meta.id);
    assert_eq!(recovered.service_updates.len(), 1);
    assert_eq!(recovered.service_updates[0].generation, Generation(8));
    assert_eq!(desired_service_replicas(&system), 1);

    system.meta.generation = recovered.service_updates[0].generation;
    system.status = recovered.service_updates[0].status.clone();
    let stable = plan(input(system, vec![crashed, replacement.clone()]))
        .expect("do not create another recovery rollout");
    assert!(stable.create_deployments.is_empty());
    assert!(stable.service_updates.is_empty());
}

#[test]
fn terminal_user_deployment_remains_terminal() {
    let service = service(Generation(7), RolloutState::Active);
    let crashed = deployment(&service, DeploymentPhase::Crashed);

    let unchanged = plan(input(service, vec![crashed])).expect("retain user terminal state");

    assert!(unchanged.create_deployments.is_empty());
    assert!(unchanged.service_updates.is_empty());
}

#[test]
fn system_readiness_uses_one_replica_when_configured_for_zero() {
    let mut system = service(Generation(1), RolloutState::Active);
    system.meta.id = ServiceId::new(TAILSCALE_GATEWAY_SERVICE_ID).expect("system service id");
    system.spec.replicas = 0;
    let deployment = deployment(&system, DeploymentPhase::Building);
    let assignment = assignment(&deployment, "assignment-1", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready);
    let mut snapshot = input(system, vec![deployment]);
    snapshot.assignments = vec![assignment];
    snapshot.replicas = vec![replica];

    let ready = plan(snapshot).expect("apply system replica floor");

    assert_eq!(
        ready.deployment_updates[0].status.phase,
        DeploymentPhase::Ready
    );
}

#[test]
fn base_service_captures_ingress_context_for_external_templates() {
    let mut service = service(Generation(7), RolloutState::Active);
    service
        .spec
        .environment_sources
        .push("aws-secret://runtime-environment".to_owned());
    let service_id = service.meta.id.clone();
    let mut plan_input = input(service, Vec::new());
    plan_input.ingress_routes = vec![Object {
        meta: metadata(IngressRouteId::new("app-route").unwrap(), Generation(1)),
        spec: IngressRouteSpec {
            service_id,
            hosts: vec!["app.example.test".to_owned()],
            path_prefix: None,
            target_port: 3000,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    }];
    let deployment = plan(plan_input)
        .expect("capture base ingress endpoint")
        .create_deployments
        .remove(0);

    assert_eq!(
        deployment.spec.environment_template.ingress_host.as_deref(),
        Some("app.example.test")
    );
    assert_eq!(
        deployment.spec.environment_template.ingress_port,
        Some(3000)
    );
}

#[test]
fn ingress_templates_are_captured_and_route_changes_create_a_new_deployment() {
    let mut service = service(Generation(7), RolloutState::Active);
    service.meta.owner_refs = vec![OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new("Preview").unwrap(),
            ResourceName::new("api-pr-42").unwrap(),
        ),
        ownership: Ownership::Controller,
    }];
    service.spec.environment.insert(
        "INGRESS_URL".to_owned(),
        "https://${{ MAESTRO_INGRESS_HOST }}:${{ MAESTRO_INGRESS_PORT }}".to_owned(),
    );
    service
        .spec
        .environment_sources
        .push("aws-secret://preview-environment".to_owned());
    let mut first_input = input(service.clone(), Vec::new());
    first_input.ingress_routes = vec![Object {
        meta: metadata(IngressRouteId::new("api-route").unwrap(), Generation(1)),
        spec: IngressRouteSpec {
            service_id: service.meta.id.clone(),
            hosts: vec!["api-pr-42.preview.example.test".to_owned()],
            path_prefix: None,
            target_port: 8080,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    }];

    let first = plan(first_input)
        .expect("resolve initial ingress host")
        .create_deployments
        .remove(0);

    assert_eq!(
        first.spec.service.environment.get("INGRESS_URL"),
        Some(&"https://api-pr-42.preview.example.test:8080".to_owned())
    );
    assert_eq!(
        first.spec.environment_template.ingress_host.as_deref(),
        Some("api-pr-42.preview.example.test")
    );
    assert_eq!(first.spec.environment_template.ingress_port, Some(8080));
    assert_eq!(
        service.spec.environment.get("INGRESS_URL"),
        Some(&"https://${{ MAESTRO_INGRESS_HOST }}:${{ MAESTRO_INGRESS_PORT }}".to_owned())
    );

    let mut changed_input = input(service, vec![first.clone()]);
    changed_input.ingress_routes = vec![Object {
        meta: metadata(IngressRouteId::new("api-route").unwrap(), Generation(2)),
        spec: IngressRouteSpec {
            service_id: first.spec.service_id.clone(),
            hosts: vec!["api-pr-42-new.preview.example.test".to_owned()],
            path_prefix: None,
            target_port: 9090,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    }];
    let changed = plan(changed_input)
        .expect("resolve changed ingress host")
        .create_deployments
        .remove(0);

    assert_ne!(changed.meta.id, first.meta.id);
    assert_eq!(
        changed.spec.service.environment.get("INGRESS_URL"),
        Some(&"https://api-pr-42-new.preview.example.test:9090".to_owned())
    );
    assert_eq!(
        changed.spec.environment_template.ingress_host.as_deref(),
        Some("api-pr-42-new.preview.example.test")
    );
    assert_eq!(changed.spec.environment_template.ingress_port, Some(9090));
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
            source_title: Some("Ship the API".to_string()),
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
        waiting.deployment_updates[0].status.git_commit,
        Some(kernel_api::GitCommit {
            revision: "abc".to_string(),
            title: "Ship the API".to_string(),
        })
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
    snapshot.replicas = vec![replica(&deployment, &assigned, DeploymentPhase::Ready)];
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
    snapshot.replicas = vec![replica(&deployment, &stale, DeploymentPhase::Ready)];
    let publishing = plan(snapshot).expect("publishing plan");
    assert_eq!(
        publishing.deployment_updates[0].status.phase,
        DeploymentPhase::Publishing
    );

    deployment.status.phase = DeploymentPhase::Publishing;
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![stale.clone(), current.clone()];
    snapshot.replicas = vec![replica(
        &deployment,
        &current,
        DeploymentPhase::PendingReady,
    )];
    let pending = plan(snapshot).expect("pending-ready plan");
    assert_eq!(
        pending.deployment_updates[0].status.phase,
        DeploymentPhase::PendingReady
    );

    deployment.status.phase = DeploymentPhase::PendingReady;
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![stale, current.clone()];
    snapshot.replicas = vec![replica(&deployment, &current, DeploymentPhase::Ready)];
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
fn failed_assignment_crashes_a_pending_deployment_with_its_error() {
    let service = service(Generation(1), RolloutState::Active);
    let deployment = deployment(&service, DeploymentPhase::PendingReady);
    let mut failed = assignment(&deployment, "assignment-1", 1);
    failed.status.phase = kernel_api::AssignmentPhase::Failed;
    failed.status.conditions = vec![Condition {
        condition_type: ConditionType::RuntimeReady,
        state: ConditionState::False,
        reason: ConditionReason("ExternalValueSourceRejected".to_owned()),
        message: "failed to fetch AWS secret `maestro/api`: access denied".to_owned(),
        observed_generation: failed.meta.generation,
        last_transition_time: Timestamp(39_000),
    }];
    let mut snapshot = input(service, vec![deployment]);
    snapshot.assignments = vec![failed];

    let result = plan(snapshot).expect("project terminal assignment failure");
    let status = &result.deployment_updates[0].status;
    assert_eq!(status.phase, DeploymentPhase::Crashed);
    let failure = status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready)
        .expect("deployment failure condition");
    assert_eq!(failure.state, ConditionState::False);
    assert_eq!(failure.reason.0, "ExternalValueSourceRejected");
    assert!(
        failure
            .message
            .contains("failed to fetch AWS secret `maestro/api`")
    );
}

#[test]
fn deployment_collects_only_masked_secret_observations() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.replicas = 2;
    let deployment = deployment(&service, DeploymentPhase::Ready);
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let first = assignment_slot(&deployment, "assignment-0", 0, 1);
    let second = assignment_slot(&deployment, "assignment-1", 1, 1);
    let mut first_replica = replica(&deployment, &first, DeploymentPhase::Ready);
    first_replica.status.resolved_secrets = Some(BTreeMap::from([
        (
            "DATABASE_URL".to_owned(),
            SecretValue::new("production").masked(),
        ),
        ("TOKEN".to_owned(), SecretValue::new("first-token").masked()),
    ]));
    let mut second_replica = replica(&deployment, &second, DeploymentPhase::Ready);
    second_replica.status.resolved_secrets = Some(BTreeMap::from([
        (
            "DATABASE_URL".to_owned(),
            SecretValue::new("production").masked(),
        ),
        (
            "TOKEN".to_owned(),
            SecretValue::new("second-value").masked(),
        ),
    ]));
    let mut snapshot = input(service, vec![deployment]);
    snapshot.assignments = vec![first, second];
    snapshot.replicas = vec![first_replica, second_replica];

    let result = plan(snapshot).expect("collect masked secret observations");
    let status = &result.deployment_updates[0].status;
    assert_eq!(
        status
            .resolved_secrets
            .as_ref()
            .and_then(|secrets| secrets.get("DATABASE_URL"))
            .map(kernel_api::MaskedSecret::as_str),
        Some("••••tion")
    );
    assert_eq!(
        status
            .resolved_secrets
            .as_ref()
            .and_then(|secrets| secrets.get("TOKEN"))
            .map(kernel_api::MaskedSecret::as_str),
        Some("••••")
    );
}

#[test]
fn replica_observation_is_collected_after_its_assignment_disappears() {
    let service = service(Generation(1), RolloutState::Active);
    let deployment = deployment(&service, DeploymentPhase::Removed);
    let assignment = assignment(&deployment, "assignment-removed", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Removed);
    let mut snapshot = input(service, vec![deployment]);
    snapshot.replicas = vec![replica.clone()];

    let collected = plan(snapshot).expect("orphan replica collection");

    assert_eq!(collected.delete_replicas, vec![replica.meta.id]);
    assert!(collected.delete_deployments.is_empty());
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
fn superseded_pending_deployment_drains_before_any_candidate_is_ready() {
    let service = service(Generation(2), RolloutState::Active);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::PendingReady,
    );
    let incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::PendingReady,
    );

    let result = plan(input(service, vec![old.clone(), incoming.clone()]))
        .expect("retire superseded pending deployment");

    assert_eq!(result.create_deployments.len(), 0);
    assert_eq!(result.deployment_updates.len(), 1);
    assert_eq!(result.deployment_updates[0].id, old.meta.id);
    assert_eq!(
        result.deployment_updates[0].status.phase,
        DeploymentPhase::Draining
    );
    assert_eq!(
        result.deployment_updates[0].status.draining_at,
        Some(Timestamp(40_000))
    );
}

#[test]
fn superseded_frozen_queued_deployment_is_canceled() {
    let service = service(Generation(2), RolloutState::Frozen);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::Queued,
    );
    let incoming = deployment_generation(
        &service,
        "deployment-new",
        Generation(2),
        DeploymentPhase::Queued,
    );

    let result = plan(input(service, vec![old.clone(), incoming]))
        .expect("cancel superseded frozen deployment");

    assert_eq!(result.deployment_updates.len(), 1);
    assert_eq!(result.deployment_updates[0].id, old.meta.id);
    assert_eq!(
        result.deployment_updates[0].status.phase,
        DeploymentPhase::Canceled
    );
}

#[test]
fn superseded_active_queued_deployment_is_canceled_before_lifecycle_advancement() {
    let service = service(Generation(2), RolloutState::Active);
    let old = deployment_generation(
        &service,
        "deployment-old",
        Generation(1),
        DeploymentPhase::Queued,
    );

    let result = plan(input(service, vec![old.clone()])).expect("replace stale queued deployment");

    assert_eq!(result.create_deployments.len(), 1);
    assert_eq!(
        result.create_deployments[0].spec.service_generation,
        Generation(2)
    );
    assert_eq!(result.deployment_updates.len(), 1);
    assert_eq!(result.deployment_updates[0].id, old.meta.id);
    assert_eq!(
        result.deployment_updates[0].status.phase,
        DeploymentPhase::Canceled
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
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    service.meta.deletion_timestamp = Some(Timestamp(40_000));
    let assignment = assignment(&deployment, "assignment-old", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.builds = vec![build.clone()];
    snapshot.replicas = vec![replica.clone()];

    let collected = plan(snapshot).expect("child collection");
    assert_eq!(collected.delete_deployments, vec![deployment.meta.id]);
    assert_eq!(collected.delete_builds, vec![build.meta.id]);
    assert_eq!(collected.delete_replicas, vec![replica.meta.id]);
    assert!(collected.deployment_updates.is_empty());
    assert_eq!(collected.service_updates.len(), 1);
    assert_eq!(
        collected.service_updates[0].status.active_deployment_id,
        None
    );
}
