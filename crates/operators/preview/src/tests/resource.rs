use kernel_api::{ArtifactTemplate, BuildSource, DeploymentId, Generation, RolloutState};

use crate::resource::{desired_route, desired_service, owned_by_preview};

use super::support::{base_route, base_service, preview};

#[test]
fn derived_service_is_stable_pinned_isolated_and_freeze_aware() {
    let preview = preview();
    let base = base_service();

    let first = desired_service(&preview, &base, None).expect("derive service");

    assert_eq!(first.meta.id, preview.spec.service_id);
    assert!(owned_by_preview(&preview, &first.meta));
    assert_eq!(first.meta.generation, Generation(1));
    assert_eq!(first.spec.replicas, 1);
    assert_eq!(
        first.spec.environment.get("BASE").map(String::as_str),
        Some("true")
    );
    assert_eq!(
        first.spec.environment.get("PREVIEW").map(String::as_str),
        Some("true")
    );
    assert!(first.spec.volumes.is_empty());
    assert!(first.spec.preview.is_none());
    assert_eq!(first.status.rollout, RolloutState::Frozen);
    assert_eq!(first.status.replica_override, None);
    assert!(matches!(
        first.spec.artifact,
        ArtifactTemplate::Build { .. }
    ));
    let ArtifactTemplate::Build { template } = &first.spec.artifact else {
        return;
    };
    assert!(!template.watch);
    assert!(matches!(template.source, BuildSource::Git { .. }));
    let BuildSource::Git { revision, .. } = &template.source else {
        return;
    };
    assert_eq!(revision, &preview.spec.head_revision);

    let mut active = first.clone();
    active.status.active_deployment_id = Some(DeploymentId::new("deployment-old").unwrap());
    let unchanged = desired_service(&preview, &base, Some(&active)).expect("rederive service");
    assert_eq!(unchanged.meta.generation, Generation(1));
    assert_eq!(
        unchanged.status.active_deployment_id,
        active.status.active_deployment_id
    );

    let mut pushed = preview.clone();
    pushed.spec.head_revision = "89abcdef0123456789abcdef0123456789abcdef".to_string();
    let updated = desired_service(&pushed, &base, Some(&active)).expect("derive pushed service");
    assert_eq!(updated.meta.id, first.meta.id);
    assert_eq!(updated.meta.generation, Generation(2));
    assert_eq!(updated.status.active_deployment_id, None);
}

#[test]
fn derived_service_preserves_dynamic_environment_templates_for_preview_deployment() {
    let preview = preview();
    let mut base = base_service();
    base.spec
        .preview
        .as_mut()
        .expect("preview policy")
        .environment
        .insert(
            "PREVIEW_URL".to_owned(),
            "https://${{ MAESTRO_INGRESS_HOST }}".to_owned(),
        );

    let derived = desired_service(&preview, &base, None).expect("derive service");

    assert_eq!(
        derived
            .spec
            .environment
            .get("PREVIEW_URL")
            .map(String::as_str),
        Some("https://${{ MAESTRO_INGRESS_HOST }}")
    );
}

#[test]
fn derived_route_keeps_shape_under_one_stable_ingress_host() {
    let preview = preview();
    let base = base_route();

    let first = desired_route(&preview, &base, None, "preview.example.test").expect("derive route");
    let repeated = desired_route(&preview, &base, Some(&first), "preview.example.test")
        .expect("rederive route");

    assert_eq!(first.meta.id, repeated.meta.id);
    assert_eq!(repeated.meta.generation, Generation(1));
    assert!(owned_by_preview(&preview, &first.meta));
    assert_eq!(first.spec.service_id, preview.spec.service_id);
    assert_eq!(first.spec.hosts, ["api-pr-42.preview.example.test"]);
    assert_eq!(first.spec.path_prefix.as_deref(), Some("/v1"));
    assert_eq!(first.spec.target_port, 8080);
}

#[test]
fn image_only_base_is_rejected_before_derivation() {
    let preview = preview();
    let mut base = base_service();
    base.spec.artifact = ArtifactTemplate::Image {
        reference: "registry.test/api:latest".to_string(),
    };

    let error = desired_service(&preview, &base, None).expect_err("reject image base");

    assert!(error.to_string().contains("must use a build artifact"));
}

#[test]
fn derivation_rejects_an_existing_unowned_service_identity() {
    let preview = preview();
    let base = base_service();
    let mut collision = base_service();
    collision.meta.id = preview.spec.service_id.clone();

    let error = desired_service(&preview, &base, Some(&collision))
        .expect_err("reject unowned service collision");

    assert!(error.to_string().contains("owned by another resource"));
}
