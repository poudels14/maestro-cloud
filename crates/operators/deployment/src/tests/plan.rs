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

fn live_assignment_nodes(
    assignments: &[kernel_api::Assignment],
) -> std::collections::BTreeSet<kernel_api::NodeId> {
    assignments
        .iter()
        .map(|assignment| assignment.spec.node_id.clone())
        .collect()
}

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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
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
fn explicit_redeploy_commit_pins_a_non_watched_git_service() {
    let mut service = service(Generation(8), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let revision = "0123456789abcdef0123456789abcdef01234567";
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_RESOLVED_REVISION_ANNOTATION.to_string()),
        revision.to_string(),
    );
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_RESOLVED_GENERATION_ANNOTATION.to_string()),
        "8".to_string(),
    );

    let deployment = plan(input(service, Vec::new()))
        .expect("explicit redeploy deployment")
        .create_deployments
        .remove(0);

    assert_eq!(deployment.spec.service_generation, Generation(8));
    let ArtifactTemplate::Build { template } = deployment.spec.service.artifact else {
        return;
    };
    assert!(!template.watch);
    assert_eq!(
        template.source,
        BuildSource::Git {
            repository: "https://example.test/repo.git".to_string(),
            revision: revision.to_string(),
        }
    );
}

#[test]
fn resolved_commit_from_an_older_generation_is_not_reused() {
    let mut service = service(Generation(9), RolloutState::Active);
    service.spec.artifact = build_artifact();
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_RESOLVED_REVISION_ANNOTATION.to_string()),
        "0123456789abcdef0123456789abcdef01234567".to_string(),
    );
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_RESOLVED_GENERATION_ANNOTATION.to_string()),
        "8".to_string(),
    );

    let deployment = plan(input(service, Vec::new()))
        .expect("new configuration deployment")
        .create_deployments
        .remove(0);

    let ArtifactTemplate::Build { template } = deployment.spec.service.artifact else {
        return;
    };
    assert_eq!(
        template.source,
        BuildSource::Git {
            repository: "https://example.test/repo.git".to_string(),
            revision: "main".to_string(),
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
    initial.status.ready_at = Some(Timestamp(1_000));
    service.status.active_deployment_id = Some(initial.meta.id.clone());
    service.meta.annotations.insert(
        kernel_api::AnnotationKey(kernel_api::BUILD_WATCH_REVISION_ANNOTATION.to_string()),
        "0123456789abcdef0123456789abcdef01234567".to_string(),
    );
    let mut snapshot = input(service.clone(), vec![initial.clone()]);
    observe_replica(&mut snapshot, &initial, DeploymentPhase::Ready);
    let mut watched = plan(snapshot)
        .expect("watched deployment")
        .create_deployments
        .remove(0);

    let mut snapshot = input(service.clone(), vec![initial.clone(), watched.clone()]);
    observe_replica(&mut snapshot, &initial, DeploymentPhase::Ready);
    let next = plan(snapshot).expect("advance watched deployment");

    assert!(next.create_deployments.is_empty());
    assert_eq!(next.create_builds.len(), 1);
    assert_eq!(next.deployment_updates.len(), 1);
    assert_eq!(next.deployment_updates[0].id, watched.meta.id);
    assert_eq!(
        next.deployment_updates[0].status.phase,
        DeploymentPhase::Preparing
    );

    watched.status.phase = DeploymentPhase::Ready;
    watched.status.ready_at = Some(Timestamp(2_000));
    let mut snapshot = input(service, vec![initial.clone(), watched.clone()]);
    observe_replica(&mut snapshot, &initial, DeploymentPhase::Ready);
    observe_replica(&mut snapshot, &watched, DeploymentPhase::Ready);
    let activated = plan(snapshot).expect("activate watched deployment");
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
        DeploymentPhase::Preparing
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
fn build_source_preparation_is_not_reported_as_building() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let mut deployment = deployment(&service, DeploymentPhase::Queued);

    let preparing =
        plan(input(service.clone(), vec![deployment.clone()])).expect("prepare build source");
    assert_eq!(preparing.create_builds.len(), 1);
    assert_eq!(
        preparing.deployment_updates[0].status.phase,
        DeploymentPhase::Preparing
    );

    deployment.status = preparing.deployment_updates[0].status.clone();
    let mut build = preparing.create_builds[0].clone();
    build.status.phase = BuildPhase::Building;
    let mut snapshot = input(service, vec![deployment]);
    snapshot.builds = vec![build];
    assert_eq!(
        plan(snapshot)
            .expect("start artifact build")
            .deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Building
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
        DeploymentPhase::Publishing
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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
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
fn ready_deployment_downgrades_while_its_replica_retries() {
    let mut service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Ready);
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let assignment = assignment(&deployment, "assignment-1", 1);
    let mut observed = replica(&deployment, &assignment, DeploymentPhase::Crashed);
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![assignment.clone()];
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
    snapshot.replicas = vec![observed.clone()];

    let retrying = plan(snapshot).expect("retry unhealthy workload");
    assert_eq!(
        retrying.deployment_updates[0].status.phase,
        DeploymentPhase::Retrying
    );
    assert_eq!(
        retrying.service_updates[0].status.active_deployment_id,
        None
    );

    deployment.status = retrying.deployment_updates[0].status.clone();
    service.status = retrying.service_updates[0].status.clone();
    observed.status.phase = DeploymentPhase::PendingReady;
    let mut snapshot = input(service, vec![deployment]);
    snapshot.assignments = vec![assignment];
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
    snapshot.replicas = vec![observed];
    assert_eq!(
        plan(snapshot)
            .expect("wait for recovered health")
            .deployment_updates[0]
            .status
            .phase,
        DeploymentPhase::Recovering
    );
}

#[test]
fn hard_node_loss_marks_the_active_deployment_and_replica_recovering() {
    let mut service = service(Generation(1), RolloutState::Active);
    let deployment = deployment(&service, DeploymentPhase::Ready);
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let assignment = assignment(&deployment, "assignment-1", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.assignments = vec![assignment];
    snapshot.replicas = vec![replica.clone()];

    let recovery = plan(snapshot).expect("project hard node loss");

    assert!(recovery.service_updates.is_empty());
    let deployment_status = &recovery
        .deployment_updates
        .iter()
        .find(|update| update.id == deployment.meta.id)
        .expect("recovering deployment update")
        .status;
    assert_eq!(deployment_status.phase, DeploymentPhase::Recovering);
    let deployment_ready = deployment_status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready)
        .expect("deployment readiness condition");
    assert_eq!(deployment_ready.state, ConditionState::Unknown);
    assert_eq!(deployment_ready.reason.0, "NodeUnreachable");

    let replica_status = &recovery
        .replica_updates
        .iter()
        .find(|update| update.id == replica.meta.id)
        .expect("recovering replica update")
        .status;
    assert_eq!(replica_status.phase, DeploymentPhase::Recovering);
    let runtime_ready = replica_status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::RuntimeReady)
        .expect("replica runtime condition");
    assert_eq!(runtime_ready.state, ConditionState::Unknown);
    assert_eq!(runtime_ready.reason.0, "NodeUnreachable");
}

#[test]
fn planned_shutdown_stops_then_recovers_the_same_active_deployment() {
    let mut service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Ready);
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let mut assignment = assignment(&deployment, "assignment-1", 1);
    assignment.status.phase = kernel_api::AssignmentPhase::Stopped;
    let mut replica = replica(&deployment, &assignment, DeploymentPhase::Stopped);
    replica.status.conditions = vec![Condition {
        condition_type: ConditionType::RuntimeReady,
        state: ConditionState::False,
        reason: ConditionReason("DaemonShutdown".to_owned()),
        message: "workload stopped for daemon shutdown; recovery is required".to_owned(),
        observed_generation: replica.meta.generation,
        last_transition_time: Timestamp(39_000),
    }];
    let mut stopped_input = input(service.clone(), vec![deployment.clone()]);
    stopped_input.assignments = vec![assignment.clone()];
    stopped_input.replicas = vec![replica.clone()];

    let stopped = plan(stopped_input).expect("project planned shutdown");

    assert!(stopped.service_updates.is_empty());
    let stopped_status = &stopped.deployment_updates[0].status;
    assert_eq!(stopped_status.phase, DeploymentPhase::Stopped);
    assert_eq!(
        stopped_status
            .conditions
            .iter()
            .find(|condition| condition.condition_type == ConditionType::Ready)
            .map(|condition| condition.reason.0.as_str()),
        Some("DaemonShutdown")
    );

    deployment.status = stopped_status.clone();
    assignment.status.phase = kernel_api::AssignmentPhase::Running;
    replica.status.phase = DeploymentPhase::PendingReady;
    let mut starting_input = input(service.clone(), vec![deployment.clone()]);
    starting_input
        .live_nodes
        .insert(assignment.spec.node_id.clone());
    starting_input.assignments = vec![assignment.clone()];
    starting_input.replicas = vec![replica.clone()];
    let recovering = plan(starting_input).expect("recover stopped deployment");
    assert_eq!(
        recovering.deployment_updates[0].status.phase,
        DeploymentPhase::Recovering
    );
    assert!(recovering.service_updates.is_empty());

    deployment.status = recovering.deployment_updates[0].status.clone();
    replica.status.phase = DeploymentPhase::Ready;
    let mut ready_input = input(service, vec![deployment]);
    ready_input
        .live_nodes
        .insert(assignment.spec.node_id.clone());
    ready_input.assignments = vec![assignment];
    ready_input.replicas = vec![replica];
    let ready = plan(ready_input).expect("finish recovery");
    assert_eq!(
        ready.deployment_updates[0].status.phase,
        DeploymentPhase::Ready
    );
    let ready_condition = ready.deployment_updates[0]
        .status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready)
        .expect("ready condition");
    assert_eq!(ready_condition.state, ConditionState::True);
    assert_eq!(ready_condition.reason.0, "ReplicasReady");
}

#[test]
fn planned_shutdown_can_recover_directly_to_ready() {
    let mut service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Stopped);
    deployment.status.ready_at = Some(Timestamp(1_000));
    deployment.status.conditions = vec![Condition {
        condition_type: ConditionType::Ready,
        state: ConditionState::False,
        reason: ConditionReason("DaemonShutdown".to_owned()),
        message: "deployment workloads stopped during daemon shutdown".to_owned(),
        observed_generation: deployment.meta.generation,
        last_transition_time: Timestamp(39_000),
    }];
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let assignment = assignment(&deployment, "assignment-1", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.live_nodes.insert(assignment.spec.node_id.clone());
    snapshot.assignments = vec![assignment];
    snapshot.replicas = vec![replica];

    let recovery = plan(snapshot).expect("recover a stopped deployment in one pass");

    let status = &recovery
        .deployment_updates
        .iter()
        .find(|update| update.id == deployment.meta.id)
        .expect("ready deployment update")
        .status;
    assert_eq!(status.phase, DeploymentPhase::Ready);
    let ready = status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready)
        .expect("ready condition");
    assert_eq!(ready.state, ConditionState::True);
    assert_eq!(ready.reason.0, "ReplicasReady");
}

#[test]
fn interrupted_shutdown_can_recover_directly_to_ready() {
    let mut service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Stopping);
    deployment.status.ready_at = Some(Timestamp(1_000));
    deployment.status.conditions = vec![Condition {
        condition_type: ConditionType::Ready,
        state: ConditionState::False,
        reason: ConditionReason("WorkloadsStopping".to_owned()),
        message: "deployment workloads are stopping".to_owned(),
        observed_generation: deployment.meta.generation,
        last_transition_time: Timestamp(39_000),
    }];
    service.status.active_deployment_id = Some(deployment.meta.id.clone());
    let assignment = assignment(&deployment, "assignment-1", 1);
    let replica = replica(&deployment, &assignment, DeploymentPhase::Ready);
    let mut snapshot = input(service, vec![deployment.clone()]);
    snapshot.live_nodes.insert(assignment.spec.node_id.clone());
    snapshot.assignments = vec![assignment];
    snapshot.replicas = vec![replica];

    let recovery = plan(snapshot).expect("recover an interrupted shutdown in one pass");

    let status = &recovery
        .deployment_updates
        .iter()
        .find(|update| update.id == deployment.meta.id)
        .expect("ready deployment update")
        .status;
    assert_eq!(status.phase, DeploymentPhase::Ready);
    let ready = status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::Ready)
        .expect("ready condition");
    assert_eq!(ready.state, ConditionState::True);
    assert_eq!(ready.reason.0, "ReplicasReady");
}

#[test]
fn interrupted_shutdown_accepts_every_observed_runtime_phase() {
    use kernel_api::AssignmentPhase;

    let observations = [
        (DeploymentPhase::Publishing, None, None),
        (
            DeploymentPhase::Starting,
            Some(AssignmentPhase::Running),
            None,
        ),
        (
            DeploymentPhase::PendingReady,
            Some(AssignmentPhase::Running),
            Some(DeploymentPhase::PendingReady),
        ),
        (
            DeploymentPhase::Retrying,
            Some(AssignmentPhase::Running),
            Some(DeploymentPhase::Crashed),
        ),
        (
            DeploymentPhase::Ready,
            Some(AssignmentPhase::Running),
            Some(DeploymentPhase::Ready),
        ),
        (
            DeploymentPhase::Stopping,
            Some(AssignmentPhase::Stopping),
            Some(DeploymentPhase::Stopping),
        ),
        (
            DeploymentPhase::Stopped,
            Some(AssignmentPhase::Stopped),
            Some(DeploymentPhase::Stopped),
        ),
        (
            DeploymentPhase::Crashed,
            Some(AssignmentPhase::Failed),
            None,
        ),
    ];

    for current in [DeploymentPhase::Stopping, DeploymentPhase::Stopped] {
        for (expected, assignment_phase, replica_phase) in observations {
            let mut service = service(Generation(1), RolloutState::Active);
            let deployment = deployment(&service, current);
            service.status.active_deployment_id = Some(deployment.meta.id.clone());
            let mut snapshot = input(service, vec![deployment.clone()]);
            if let Some(assignment_phase) = assignment_phase {
                let mut observed_assignment = assignment(&deployment, "assignment-1", 1);
                observed_assignment.status.phase = assignment_phase;
                snapshot
                    .live_nodes
                    .insert(observed_assignment.spec.node_id.clone());
                if let Some(replica_phase) = replica_phase {
                    snapshot.replicas.push(replica(
                        &deployment,
                        &observed_assignment,
                        replica_phase,
                    ));
                }
                snapshot.assignments.push(observed_assignment);
            }

            let recovery = plan(snapshot).expect("observed shutdown recovery must be valid");
            let status = &recovery
                .deployment_updates
                .iter()
                .find(|update| update.id == deployment.meta.id)
                .expect("shutdown recovery must update the deployment")
                .status;
            assert_eq!(
                status.phase, expected,
                "unexpected recovery phase from {current:?}"
            );
        }
    }
}

#[test]
fn crashed_deployment_keeps_its_failure_phase_while_cleanup_completes() {
    let service = service(Generation(1), RolloutState::Active);
    let mut deployment = deployment(&service, DeploymentPhase::Crashed);
    let assignment = assignment(&deployment, "assignment-1", 1);
    let mut pending_input = input(service.clone(), vec![deployment.clone()]);
    pending_input.assignments = vec![assignment];
    pending_input.live_nodes = live_assignment_nodes(&pending_input.assignments);

    let pending = plan(pending_input).expect("project pending cleanup");
    let pending_status = &pending.deployment_updates[0].status;
    assert_eq!(pending_status.phase, DeploymentPhase::Crashed);
    let pending_cleanup = pending_status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::CleanupComplete)
        .expect("pending cleanup condition");
    assert_eq!(pending_cleanup.state, ConditionState::False);
    assert_eq!(pending_cleanup.reason.0, "RuntimeCleanupPending");

    deployment.status = pending_status.clone();
    let complete = plan(input(service, vec![deployment])).expect("complete cleanup");
    let complete_status = &complete.deployment_updates[0].status;
    assert_eq!(complete_status.phase, DeploymentPhase::Crashed);
    let complete_cleanup = complete_status
        .conditions
        .iter()
        .find(|condition| condition.condition_type == ConditionType::CleanupComplete)
        .expect("complete cleanup condition");
    assert_eq!(complete_cleanup.state, ConditionState::True);
    assert_eq!(complete_cleanup.reason.0, "RuntimeResourcesRemoved");
}

#[test]
fn failed_build_crashes_its_deployment() {
    let mut service = service(Generation(1), RolloutState::Active);
    service.spec.artifact = build_artifact();
    let deployment = deployment(&service, DeploymentPhase::Building);
    let build = Build {
        meta: metadata(
            deployment.spec.build_id.clone().expect("build id"),
            Generation(1),
        ),
        spec: kernel_api::BuildSpec {
            service_id: service.meta.id.clone(),
            deployment_id: deployment.meta.id.clone(),
            template: build_template(),
        },
        status: BuildStatus {
            phase: BuildPhase::Failed,
            image_digest: None,
            source_revision: Some("abc".to_string()),
            source_title: Some("Broken build".to_string()),
            conditions: Vec::new(),
        },
    };
    let mut snapshot = input(service, vec![deployment]);
    snapshot.builds = vec![build];

    let result = plan(snapshot).expect("propagate failed build");

    assert_eq!(
        result.deployment_updates[0].status.phase,
        DeploymentPhase::Crashed
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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
    snapshot.replicas = vec![replica(&deployment, &stale, DeploymentPhase::Ready)];
    let publishing = plan(snapshot).expect("publishing plan");
    assert_eq!(
        publishing.deployment_updates[0].status.phase,
        DeploymentPhase::Starting
    );

    deployment.status.phase = DeploymentPhase::Publishing;
    let mut snapshot = input(service.clone(), vec![deployment.clone()]);
    snapshot.assignments = vec![stale.clone(), current.clone()];
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
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
fn pending_ready_returns_to_starting_while_its_workload_restarts() {
    let service = service(Generation(1), RolloutState::Active);
    let deployment = deployment(&service, DeploymentPhase::PendingReady);
    let assignment = assignment(&deployment, "assignment-1", 1);
    let mut snapshot = input(service, vec![deployment]);
    snapshot.live_nodes = live_assignment_nodes(std::slice::from_ref(&assignment));
    snapshot.assignments = vec![assignment];

    let result = plan(snapshot).expect("return pending deployment to starting");
    assert_eq!(
        result.deployment_updates[0].status.phase,
        DeploymentPhase::Starting
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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);

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
    snapshot.live_nodes = live_assignment_nodes(&snapshot.assignments);
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
    observe_replica(&mut first, &old, DeploymentPhase::Ready);
    observe_replica(&mut first, &incoming, DeploymentPhase::Ready);
    first.traffic_generations = Vec::new();
    let activated = plan(first).expect("activate plan");
    assert_eq!(
        activated.service_updates[0].status.active_deployment_id,
        Some(incoming.meta.id.clone())
    );
    assert!(activated.deployment_updates.is_empty());

    service.status.active_deployment_id = Some(incoming.meta.id.clone());
    let mut acknowledged = input(service, vec![old.clone(), incoming.clone()]);
    observe_replica(&mut acknowledged, &old, DeploymentPhase::Ready);
    observe_replica(&mut acknowledged, &incoming, DeploymentPhase::Ready);
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
fn preview_update_retires_active_deployment_before_replacement_is_ready() {
    let mut service = service(Generation(2), RolloutState::Active);
    service.meta.owner_refs = vec![OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new("Preview").unwrap(),
            ResourceName::new("preview-api-42").unwrap(),
        ),
        ownership: Ownership::Controller,
    }];
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
        DeploymentPhase::PendingReady,
    );
    service.status.active_deployment_id = Some(old.meta.id.clone());

    let replaced = plan(input(service, vec![old.clone(), incoming]))
        .expect("replace stale preview deployment immediately");

    assert_eq!(replaced.service_updates.len(), 1);
    assert_eq!(
        replaced.service_updates[0].status.active_deployment_id,
        None
    );
    let retired = replaced
        .deployment_updates
        .iter()
        .find(|update| update.id == old.meta.id)
        .expect("old preview deployment is retired");
    assert_eq!(retired.status.phase, DeploymentPhase::Draining);
    assert_eq!(retired.status.draining_at, Some(Timestamp(40_000)));
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

    let mut snapshot = input(service, vec![old.clone(), incoming.clone()]);
    observe_replica(&mut snapshot, &old, DeploymentPhase::PendingReady);
    observe_replica(&mut snapshot, &incoming, DeploymentPhase::PendingReady);
    let result = plan(snapshot).expect("retire superseded pending deployment");

    assert_eq!(result.create_deployments.len(), 0);
    assert_eq!(result.deployment_updates.len(), 2);
    let retired = result
        .deployment_updates
        .iter()
        .find(|update| update.id == old.meta.id)
        .expect("old deployment is retired");
    assert_eq!(retired.status.phase, DeploymentPhase::Draining);
    assert_eq!(retired.status.draining_at, Some(Timestamp(40_000)));
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

    let mut snapshot = input(service.clone(), vec![old.clone(), incoming.clone()]);
    observe_replica(&mut snapshot, &old, DeploymentPhase::Ready);
    observe_replica(&mut snapshot, &incoming, DeploymentPhase::Ready);
    let activated = plan(snapshot).expect("activate watched deployment");
    assert_eq!(
        activated
            .service_updates
            .first()
            .map(|update| update.status.active_deployment_id.clone()),
        Some(Some(incoming.meta.id.clone()))
    );

    service.status.active_deployment_id = Some(incoming.meta.id.clone());
    let mut acknowledged = input(service, vec![old.clone(), incoming.clone()]);
    observe_replica(&mut acknowledged, &old, DeploymentPhase::Ready);
    observe_replica(&mut acknowledged, &incoming, DeploymentPhase::Ready);
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
    observe_replica(&mut snapshot, &old, DeploymentPhase::Ready);
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
    held.live_nodes = live_assignment_nodes(&held.assignments);
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
