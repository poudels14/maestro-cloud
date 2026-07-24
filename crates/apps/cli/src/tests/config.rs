use std::collections::BTreeMap;
use std::net::Ipv4Addr;

use kernel_api::ClusterId;

use crate::CliError;
use crate::config::{ConfigKind, init, load_cluster, validate};
use crate::config_source::{ConfigSourceReader, SystemConfigSourceReader};

struct MemoryReader {
    sources: BTreeMap<String, String>,
}

impl ConfigSourceReader for MemoryReader {
    async fn read(&self, source: &str) -> Result<String, CliError> {
        self.sources
            .get(source)
            .cloned()
            .ok_or_else(|| CliError::not_found(format!("missing fixture `{source}`")))
    }
}

#[tokio::test]
async fn init_creates_private_valid_templates_and_refuses_overwrite()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let cluster_path = directory.path().join("maestro.jsonc");
    let services_path = directory.path().join("maestro.services.jsonc");
    let mut output = Vec::new();
    init(ConfigKind::Cluster, Some(&cluster_path), &mut output)?;
    init(ConfigKind::Services, Some(&services_path), &mut output)?;

    let cluster = std::fs::read_to_string(&cluster_path)?;
    assert!(!cluster.contains("CHANGE_ME"));
    let cluster_value: serde_json::Value = json5::from_str(&cluster)?;
    assert!(
        cluster_value
            .pointer("/cluster/join-secret")
            .and_then(serde_json::Value::as_str)
            .is_some_and(|secret| secret.len() == 64)
    );
    let mut validation = Vec::new();
    validate(
        cluster_path.to_str().ok_or("non-UTF-8 cluster path")?,
        &mut validation,
        &SystemConfigSourceReader,
    )
    .await?;
    validate(
        services_path.to_str().ok_or("non-UTF-8 services path")?,
        &mut validation,
        &SystemConfigSourceReader,
    )
    .await?;
    let validation = String::from_utf8(validation)?;
    assert!(validation.contains("is a valid cluster config"));
    assert!(validation.contains("is a valid services config (1 services)"));
    let error = init(ConfigKind::Cluster, Some(&cluster_path), &mut output)
        .expect_err("existing config must not be overwritten");
    assert!(error.to_string().contains("refusing to overwrite"));

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        assert_eq!(
            std::fs::metadata(cluster_path)?.permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(services_path)?.permissions().mode() & 0o777,
            0o600
        );
    }
    Ok(())
}

#[tokio::test]
async fn validate_detects_inherited_cluster_config_and_reports_ignored_fields()
-> Result<(), Box<dyn std::error::Error>> {
    let child = "file:///config/maestro.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                child.to_string(),
                r#"{ $extends: "base.jsonc", futureRoot: true }"#.to_string(),
            ),
            (
                "file:///config/base.jsonc".to_string(),
                cluster_document("172.22.1.0/24"),
            ),
        ]),
    };
    let mut output = Vec::new();
    validate(child, &mut output, &reader).await?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("valid cluster config for `test-cluster` on node `node-1`"));
    assert!(output.contains("  - futureRoot"));
    Ok(())
}

#[tokio::test]
async fn validate_reports_exact_cluster_paths_and_rejects_ambiguous_documents()
-> Result<(), Box<dyn std::error::Error>> {
    let invalid_source = "file:///config/invalid.jsonc";
    let ambiguous_source = "file:///config/ambiguous.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                invalid_source.to_string(),
                cluster_document("172.22.1.7/24"),
            ),
            (
                ambiguous_source.to_string(),
                r#"{ cluster: {}, services: {} }"#.to_string(),
            ),
        ]),
    };
    let mut output = Vec::new();
    let error = validate(invalid_source, &mut output, &reader)
        .await
        .expect_err("non-canonical subnet must fail");
    assert!(error.to_string().contains("cluster.nodes.node-1.subnet:"));
    let error = validate(ambiguous_source, &mut output, &reader)
        .await
        .expect_err("ambiguous config must fail");
    assert!(error.to_string().contains("both `cluster` and `services`"));
    Ok(())
}

#[tokio::test]
async fn validate_reports_nested_paths_for_cluster_and_service_type_errors()
-> Result<(), Box<dyn std::error::Error>> {
    let cluster_source = "file:///config/type-cluster.jsonc";
    let services_source = "file:///config/type-services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (
                cluster_source.to_string(),
                cluster_document("172.22.1.0/24").replace("\"10.20.0.11\"", "7"),
            ),
            (
                services_source.to_string(),
                r#"{
                    services: {
                        api: {
                            name: "API",
                            image: "api:latest",
                            deploy: { replicas: "two" }
                        }
                    }
                }"#
                .to_string(),
            ),
        ]),
    };
    let mut output = Vec::new();
    let cluster_error = validate(cluster_source, &mut output, &reader)
        .await
        .expect_err("numeric endpoint must fail");
    assert!(
        cluster_error
            .to_string()
            .contains("cluster.nodes.node-1.endpoint:")
    );
    let services_error = validate(services_source, &mut output, &reader)
        .await
        .expect_err("text replica count must fail");
    assert!(
        services_error
            .to_string()
            .contains("services.api.deploy.replicas:")
    );
    Ok(())
}

#[tokio::test]
async fn validate_rejects_a_service_without_the_services_envelope()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.services.jsonc";
    let reader = MemoryReader {
        sources: BTreeMap::from([(
            source.to_string(),
            r#"{
                id: "my-service",
                name: "My Service",
                build: { dockerfile: "Dockerfile" },
                ingress: { host: "my-service.local", port: 80 },
                deploy: { healthcheckPath: "/health", replicas: 1 },
                futureRoot: true
            }"#
            .to_string(),
        )]),
    };
    let mut output = Vec::new();

    let error = validate(source, &mut output, &reader)
        .await
        .expect_err("single-service compatibility document must be rejected");
    assert!(
        error
            .to_string()
            .contains("expected a top-level `cluster` or `services` field")
    );
    assert!(output.is_empty());
    Ok(())
}

#[tokio::test]
async fn tailscale_config_resolves_auth_sources_and_defaults_to_the_cluster_route()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.jsonc";
    let document = cluster_document("172.22.1.0/24").replace(
        "\n            node: \"node-1\"",
        r#"
            tailscale: {
                "auth-key": "aws-secret://tailscale-auth",
                "advertise-routes": null,
                replicas: 1,
                "cross-cluster-dns": [{
                    "cluster-id": "remote",
                    nameservers: ["172.23.1.1", "172.23.2.1"]
                }]
            },
            node: "node-1""#,
    );
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (source.to_owned(), document),
            (
                "aws-secret://tailscale-auth".to_owned(),
                "tskey-auth-reusable-test-secret\n".to_owned(),
            ),
        ]),
    };

    let loaded = load_cluster(source, &reader).await?;
    let tailscale = loaded.cluster.tailscale.ok_or("tailscale config missing")?;
    assert_eq!(
        tailscale.auth_key.expose(),
        "tskey-auth-reusable-test-secret"
    );
    assert_eq!(
        tailscale.advertised_routes(loaded.cluster.cluster_cidr),
        ["172.22.0.0/16".parse()?]
    );
    assert_eq!(tailscale.tags, ["tag:maestro-gateway"]);
    let route = tailscale
        .cross_cluster_dns
        .first()
        .ok_or("cross-cluster DNS route missing")?;
    assert_eq!(route.cluster_id, ClusterId::new("remote")?);
    assert_eq!(
        route.nameservers,
        [Ipv4Addr::new(172, 23, 1, 1), Ipv4Addr::new(172, 23, 2, 1)]
    );
    Ok(())
}

#[tokio::test]
async fn cloudflare_config_resolves_the_tunnel_token_and_validates_replicas()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.jsonc";
    let document = cluster_document("172.22.1.0/24").replace(
        "\n            node: \"node-1\"",
        r#"
            cloudflare: {
                tunnel: {
                    token: "aws-secret://cloudflare-tunnel-token"
                }
            },
            node: "node-1""#,
    );
    let reader = MemoryReader {
        sources: BTreeMap::from([
            (source.to_owned(), document),
            (
                "aws-secret://cloudflare-tunnel-token".to_owned(),
                "test-cloudflare-tunnel-token\n".to_owned(),
            ),
        ]),
    };

    let loaded = load_cluster(source, &reader).await?;
    let cloudflare = loaded
        .cluster
        .cloudflare
        .ok_or("Cloudflare config missing")?;
    assert_eq!(cloudflare.token.expose(), "test-cloudflare-tunnel-token");
    assert_eq!(cloudflare.replicas, 2);

    let invalid_source = "file:///config/invalid-cloudflare.jsonc";
    let invalid = cluster_document("172.22.1.0/24").replace(
        "\n            node: \"node-1\"",
        r#"
            cloudflare: {
                tunnel: {
                    token: "test-token",
                    replicas: 0
                }
            },
            node: "node-1""#,
    );
    let reader = MemoryReader {
        sources: BTreeMap::from([(invalid_source.to_owned(), invalid)]),
    };
    let error = load_cluster(invalid_source, &reader)
        .await
        .expect_err("zero Cloudflare replicas must fail");
    assert!(error.to_string().contains("cloudflare.tunnel.replicas:"));
    Ok(())
}

#[tokio::test]
async fn cluster_config_rejects_removed_camel_case_aliases()
-> Result<(), Box<dyn std::error::Error>> {
    let source = "file:///config/maestro.jsonc";
    let document = cluster_document("172.22.1.0/24")
        .replace("\"cluster-cidr\"", "clusterCidr")
        .replace("\"join-secret\"", "joinSecret");
    let reader = MemoryReader {
        sources: BTreeMap::from([(source.to_owned(), document)]),
    };

    let error = load_cluster(source, &reader)
        .await
        .expect_err("camelCase cluster aliases must be rejected");
    assert!(error.to_string().contains("missing field `cluster-cidr`"));
    Ok(())
}

fn cluster_document(subnet: &str) -> String {
    format!(
        r#"{{
            cluster: {{
                name: "test-cluster",
                "cluster-cidr": "172.22.0.0/16",
                nodes: {{
                    "node-1": {{
                        endpoint: "10.20.0.11",
                        subnet: "{subnet}",
                        role: "master"
                    }}
                }},
                "control-allow-cidrs": ["10.20.0.0/24"],
                "join-secret": "a-test-join-secret-with-at-least-32-characters"
            }},
            node: "node-1"
        }}"#
    )
}
