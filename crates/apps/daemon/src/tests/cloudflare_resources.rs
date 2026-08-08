use cluster::CloudflareTunnelConfig;
use kernel_api::{
    ArtifactTemplate, ExecPolicy, HealthProbe, NodeApiAccess, NodeRole, ReplicaSpread,
    SecretMountSpec, SecretValue, WorkloadUserSpec,
};

use crate::cloudflare_resources::{
    CLOUDFLARE_IMAGE, CLOUDFLARE_SERVICE_ID, CloudflareSystemResources,
};

use super::cluster_with_nodes;

#[test]
fn builds_a_pinned_secret_mounted_ready_connector_service() -> Result<(), Box<dyn std::error::Error>>
{
    let token = "test-cloudflare-tunnel-token";
    let mut cluster =
        cluster_with_nodes(&[("master", NodeRole::Master), ("worker", NodeRole::Worker)])?;
    cluster.cloudflare = Some(CloudflareTunnelConfig {
        token: SecretValue::new(token),
        replicas: 2,
    });

    let resources =
        CloudflareSystemResources::from_cluster(&cluster)?.ok_or("Cloudflare resources missing")?;
    let service = &resources.service;
    assert_eq!(service.meta.id.as_str(), CLOUDFLARE_SERVICE_ID);
    assert_eq!(service.spec.name, "Cloudflare Tunnel");
    assert_eq!(service.spec.replicas, 4);
    assert_eq!(service.spec.node_api, NodeApiAccess::Disabled);
    assert_eq!(service.spec.exec, ExecPolicy::Denied);
    assert_eq!(service.spec.user, Some(WorkloadUserSpec::UNPRIVILEGED));
    assert_eq!(
        service.spec.placement.replica_spread,
        ReplicaSpread::BestEffort
    );
    assert!(service.spec.environment.is_empty());
    assert!(matches!(
        &service.spec.artifact,
        ArtifactTemplate::Image { reference } if reference == CLOUDFLARE_IMAGE
    ));
    let command = service
        .spec
        .command
        .as_ref()
        .ok_or("cloudflared command missing")?;
    assert_eq!(command.executable, "/usr/local/bin/cloudflared");
    assert_eq!(
        command.arguments,
        [
            "tunnel",
            "--no-autoupdate",
            "--metrics",
            "0.0.0.0:20241",
            "run",
            "--token-file",
            "/run/secrets/cloudflare/token",
        ]
    );
    assert!(
        !command
            .arguments
            .iter()
            .any(|argument| argument.contains(token))
    );
    assert!(matches!(
        service.spec.health_check.as_ref().map(|check| &check.probe),
        Some(HealthProbe::Http { port: 20_241, path }) if path == "/ready"
    ));
    let Some(SecretMountSpec::Files { mount_path, files }) = &service.spec.secrets else {
        return Err("Cloudflare token is not a file secret".into());
    };
    assert_eq!(mount_path, "/run/secrets/cloudflare");
    assert_eq!(files.get("token").map(SecretValue::expose), Some(token));
    assert!(!format!("{service:?}").contains(token));
    Ok(())
}

#[test]
fn provisions_the_configured_replica_count_on_every_workload_node()
-> Result<(), Box<dyn std::error::Error>> {
    let mut cluster = cluster_with_nodes(&[
        ("master", NodeRole::Master),
        ("hybrid", NodeRole::Hybrid),
        ("worker", NodeRole::Worker),
        ("control-plane", NodeRole::ControlPlane),
    ])?;
    cluster.cloudflare = Some(CloudflareTunnelConfig {
        token: SecretValue::new("test-cloudflare-tunnel-token"),
        replicas: 3,
    });

    let resources =
        CloudflareSystemResources::from_cluster(&cluster)?.ok_or("Cloudflare resources missing")?;

    assert_eq!(resources.service.spec.replicas, 9);
    assert_eq!(
        resources.service.spec.placement.replica_spread,
        ReplicaSpread::BestEffort
    );
    Ok(())
}

#[test]
fn omits_connectors_when_the_cluster_has_no_tunnel() -> Result<(), Box<dyn std::error::Error>> {
    let cluster = cluster_with_nodes(&[("master", NodeRole::Master)])?;
    assert!(CloudflareSystemResources::from_cluster(&cluster)?.is_none());
    Ok(())
}
