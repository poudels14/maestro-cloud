use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    Generation, IngressRoute, IngressRouteId, IngressRouteSpec, IngressRouteStatus, Object,
    ObjectMeta, OwnerReference, Ownership, ResourceId, ResourceKind, ResourceName,
    ResourceRevision,
};

use crate::environment::resolve;
use crate::tests::plan_support::service;

#[test]
fn resolves_preview_host_templates_inside_runtime_environment_values() {
    let service = preview_service();
    let routes = vec![route(&service, "api-pr-42.preview.example.test")];
    let mut environment = BTreeMap::from([
        (
            "PREVIEW_URL".to_owned(),
            "https://${{ MAESTRO_PREVIEW_HOST }}/v1".to_owned(),
        ),
        (
            "CALLBACK".to_owned(),
            "${{MAESTRO_PREVIEW_HOST}}/callback/${{ MAESTRO_PREVIEW_HOST }}".to_owned(),
        ),
        ("LITERAL".to_owned(), "unchanged".to_owned()),
    ]);

    let fingerprint = resolve(&service, &routes, &mut environment).expect("resolve environment");

    assert!(fingerprint.is_some());
    assert_eq!(
        environment.get("PREVIEW_URL").map(String::as_str),
        Some("https://api-pr-42.preview.example.test/v1")
    );
    assert_eq!(
        environment.get("CALLBACK").map(String::as_str),
        Some("api-pr-42.preview.example.test/callback/api-pr-42.preview.example.test")
    );
    assert_eq!(
        environment.get("LITERAL").map(String::as_str),
        Some("unchanged")
    );
}

#[test]
fn preview_host_is_available_only_to_preview_owned_services() {
    let preview_service = preview_service();
    let routes = vec![route(&preview_service, "api-pr-42.preview.example.test")];
    let mut environment = BTreeMap::from([(
        "PREVIEW_URL".to_owned(),
        "https://${{ MAESTRO_PREVIEW_HOST }}".to_owned(),
    )]);

    resolve(&preview_service, &routes, &mut environment).expect("resolve preview host");

    assert_eq!(
        environment.get("PREVIEW_URL").map(String::as_str),
        Some("https://api-pr-42.preview.example.test")
    );

    let base_service = service(Generation(1), kernel_api::RolloutState::Active);
    let mut invalid = BTreeMap::from([(
        "PREVIEW_URL".to_owned(),
        "${{ MAESTRO_PREVIEW_HOST }}".to_owned(),
    )]);
    let error = resolve(
        &base_service,
        &[route(&base_service, "api.example.test")],
        &mut invalid,
    )
    .expect_err("base service must not resolve preview host");
    assert!(
        error
            .to_string()
            .contains("the service is not owned by a Preview")
    );
}

#[test]
fn rejects_missing_ambiguous_wildcard_unknown_and_malformed_variables() {
    let service = preview_service();
    let cases = [
        (
            Vec::new(),
            "${{ MAESTRO_PREVIEW_HOST }}",
            "no active ingress host",
        ),
        (
            vec![
                route(&service, "api.example.test"),
                route_with_id(&service, "route-2", "api-alt.example.test"),
            ],
            "${{ MAESTRO_PREVIEW_HOST }}",
            "multiple ingress hosts",
        ),
        (
            vec![route(&service, "*.example.test")],
            "${{ MAESTRO_PREVIEW_HOST }}",
            "is a wildcard",
        ),
        (
            vec![route(&service, "api.example.test")],
            "${{ MAESTRO_UNKNOWN }}",
            "variable is not supported",
        ),
        (
            vec![route(&service, "api.example.test")],
            "${{ MAESTRO_PREVIEW_HOST",
            "missing its closing",
        ),
    ];

    for (routes, value, expected) in cases {
        let mut environment = BTreeMap::from([("URL".to_owned(), value.to_owned())]);
        let error = resolve(&service, &routes, &mut environment)
            .expect_err("invalid environment template must fail");
        assert!(
            error.to_string().contains(expected),
            "`{}` did not contain `{expected}`",
            error
        );
    }
}

fn preview_service() -> kernel_api::Service {
    let mut service = service(Generation(1), kernel_api::RolloutState::Active);
    service.meta.owner_refs = vec![OwnerReference {
        resource: ResourceId::new(
            ResourceKind::new("Preview").unwrap(),
            ResourceName::new("api-pr-42").unwrap(),
        ),
        ownership: Ownership::Controller,
    }];
    service
}

fn route(service: &kernel_api::Service, host: &str) -> IngressRoute {
    route_with_id(service, "route-1", host)
}

fn route_with_id(service: &kernel_api::Service, id: &str, host: &str) -> IngressRoute {
    Object {
        meta: ObjectMeta {
            id: IngressRouteId::new(id).unwrap(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision(1),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: IngressRouteSpec {
            service_id: service.meta.id.clone(),
            hosts: vec![host.to_owned()],
            path_prefix: None,
            target_port: 8080,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation::default(),
            conditions: Vec::new(),
        },
    }
}
