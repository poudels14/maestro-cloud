use super::build_health_url_for_replica;
use crate::deployment::types::{
    DeploymentStatus, ServiceConfig, ServiceDeployConfig, ServiceDeployment, ServiceProvider,
};

fn deployment_with_ports(ingress_port: Option<u16>, expose_ports: Vec<u16>) -> ServiceDeployment {
    ServiceDeployment {
        id: "abc123xyz9".to_string(),
        created_at: 0,
        deployed_at: None,
        drained_at: None,
        status: DeploymentStatus::PendingReady,
        config: ServiceConfig {
            id: "svc".to_string(),
            name: "svc".to_string(),
            version: "v1".to_string(),
            provider: ServiceProvider::Docker,
            build: None,
            image: Some("svc:latest".to_string()),
            deploy: ServiceDeployConfig {
                flags: vec![],
                expose_ports,
                command: None,
                healthcheck_path: Some("/health".to_string()),
                replicas: 1,
                max_restarts: None,
                env: Default::default(),
                secrets: None,
            },
            ingress: ingress_port.map(|port| crate::deployment::types::IngressConfig {
                host: "svc.local".to_string(),
                port: Some(port),
            }),
        },
        git_commit: None,
        build: None,
    }
}

#[test]
fn health_url_prefers_ingress_port() {
    let deployment = deployment_with_ports(Some(8080), vec![3000]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None)
        .expect("ingress.port should produce health URL");
    assert_eq!(url, "http://svc-abc123:8080/health");
}

#[test]
fn health_url_requires_ingress_port_even_when_expose_ports_exist() {
    let deployment = deployment_with_ports(None, vec![3000, 5000]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None);
    assert!(url.is_none());
}

#[test]
fn health_url_requires_explicit_port() {
    let deployment = deployment_with_ports(None, vec![]);
    let url = build_health_url_for_replica(&deployment, 0, "/health", None);
    assert!(url.is_none());
}

#[test]
fn health_url_uses_fqdn_when_dns_domain_is_provided() {
    let deployment = deployment_with_ports(Some(8080), vec![]);
    let url = build_health_url_for_replica(
        &deployment,
        0,
        "/health",
        Some("cluster-1.maestro.internal"),
    )
    .expect("ingress.port should produce health URL");
    assert_eq!(
        url,
        "http://svc-abc123.cluster-1.maestro.internal:8080/health"
    );
}
