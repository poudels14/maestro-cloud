use std::collections::{BTreeSet, HashMap};
use std::path::PathBuf;
use std::time::{SystemTime, UNIX_EPOCH};

use clap::Parser;
use serde_json::Value;

use super::*;

const START_SCHEMA: &str = include_str!("../../../maestro.start.schema.json");
const SERVICES_SCHEMA: &str = include_str!("../../../maestro.schema.json");

fn temp_path(label: &str, ext: &str) -> PathBuf {
    let unique = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("time")
        .as_nanos();
    std::env::temp_dir().join(format!(
        "maestro-{label}-{}-{unique}.{ext}",
        std::process::id()
    ))
}

#[test]
fn write_template_refuses_overwrite() {
    let path = temp_path("write-overwrite", "jsonc");
    write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect("first create should work");
    let err =
        write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect_err("second create should fail");
    assert!(err.to_string().contains("already exists"));
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn cluster_template_validates_as_cluster_config() {
    let path = temp_path("validate-cluster", "jsonc");
    write_template(&path, DEFAULT_START_TEMPLATE).expect("write");
    run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect("start template should validate as cluster config");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn inherited_cluster_config_validates_after_merging() {
    let base = temp_path("validate-extends-base", "jsonc");
    let node = temp_path("validate-extends-node", "jsonc");
    std::fs::write(
        &base,
        r#"{
            cluster: { name: "test" },
            ingress: { port: 8080 },
            subnet: "172.22.1.0/24",
            "encryption-key": "secret"
        }"#,
    )
    .expect("write base");
    std::fs::write(
        &node,
        format!(
            r#"{{
                "$extends": "file://{}",
                subnet: "172.22.2.0/24"
            }}"#,
            base.display()
        ),
    )
    .expect("write node");

    run_validate(node.to_str().expect("UTF-8 temp path"))
        .await
        .expect("inherited config should validate after merging");

    let _ = std::fs::remove_file(base);
    let _ = std::fs::remove_file(node);
}

#[tokio::test]
async fn validate_accepts_minimal_multi_node_config_with_default_roles() {
    let path = temp_path("validate-default-node-roles", "jsonc");
    std::fs::write(
        &path,
        r#"{
            node: "node2",
            cluster: {
                name: "test",
                nodes: {
                    node1: {
                        endpoint: "10.20.0.11",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    },
                    node2: {
                        endpoint: "10.20.0.12",
                        subnet: "172.22.2.0/24"
                    },
                    node3: {
                        endpoint: "10.20.0.13",
                        subnet: "172.22.3.0/24"
                    }
                },
                "join-secret": "0123456789abcdef0123456789abcdef"
            },
            ingress: { port: 8080 },
            "jwt-secret-key": "0123456789abcdef0123456789abcdef",
            "encryption-key": "secret"
        }"#,
    )
    .expect("write config");

    run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect("minimal multi-node config should validate");

    let _ = std::fs::remove_file(path);
}

#[test]
fn cluster_template_does_not_enable_optional_integrations() {
    let config: crate::config::StartConfig =
        json5::from_str(DEFAULT_START_TEMPLATE).expect("parse start template");
    assert_eq!(config.runtime, crate::config::RuntimeType::Docker);
    assert!(config.tailscale.is_none());
    assert!(config.datadog.is_none());
    assert!(config.depot.is_none());
    assert!(config.cloudflare.is_none());
    assert!(config.slack.is_none());
    assert!(config.log_backup.is_none());
}

#[test]
fn legacy_single_node_config_stays_on_the_legacy_path() {
    let config: crate::config::StartConfig = json5::from_str(
        r#"{
            cluster: { name: "legacy" },
            ingress: { port: 8080 },
            subnet: "172.22.0.0/16",
            "encryption-key": "existing-key",
            runtime: "docker"
        }"#,
    )
    .expect("parse legacy start config");

    assert!(config.cluster.nodes.is_empty());
    assert_eq!(config.node.role, crate::cluster::NodeRole::Hybrid);
    assert_eq!(config.subnet.as_deref(), Some("172.22.0.0/16"));
    crate::cluster::network::validate_cluster_config(
        &config.cluster,
        config.subnet.as_deref(),
        config.node.role,
    )
    .expect("legacy config remains valid");
}

#[test]
fn daemon_start_accepts_a_local_role_override() {
    let cli = crate::Cli::try_parse_from([
        "maestro",
        "daemon",
        "start",
        "--cluster-name",
        "test",
        "--ingress-port",
        "8080",
        "--encryption-key",
        "secret",
        "--data-dir",
        "/tmp/maestro-role-test",
        "--project-dir",
        ".",
        "--role=worker",
    ])
    .expect("parse role override");

    let Some(crate::CliCommand::Daemon {
        command: crate::DaemonCommand::Start(args),
    }) = cli.command
    else {
        panic!("expected daemon start");
    };
    assert_eq!(args.role, Some(crate::cluster::NodeRole::Worker));
}

#[test]
fn datadog_healthcheck_filter_is_opt_in() {
    let existing: crate::config::DatadogConfig =
        serde_json::from_value(serde_json::json!({ "api-key": "test" }))
            .expect("existing Datadog config should remain valid");
    assert!(existing.logs.include_healthcheck);

    let filtered: crate::config::DatadogConfig = serde_json::from_value(serde_json::json!({
        "api-key": "test",
        "logs": { "include-healthcheck": false }
    }))
    .expect("nested Datadog log config should parse");
    assert!(!filtered.logs.include_healthcheck);
}

#[tokio::test]
async fn services_template_validates_as_services_config() {
    let path = temp_path("validate-services", "jsonc");
    write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect("write");
    run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect("cluster template should validate as services config");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn services_config_validates_service_egress_allow() {
    let path = temp_path("validate-service-egress", "jsonc");
    std::fs::write(
        &path,
        r#"{
            services: {
                api: {
                    name: "API",
                    image: "example/api:latest",
                    deploy: {
                        egress: {
                            allow: [{ cidr: "10.0.10.0/24", ports: [5432] }]
                        }
                    }
                }
            }
        }"#,
    )
    .expect("write");

    run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect("service egress allow should validate");
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn services_config_reports_invalid_service_egress_path() {
    let path = temp_path("validate-invalid-service-egress", "jsonc");
    std::fs::write(
        &path,
        r#"{
            services: {
                api: {
                    name: "API",
                    image: "example/api:latest",
                    deploy: {
                        egress: {
                            allow: [{ cidr: "10.0.10.1/24", ports: [5432] }]
                        }
                    }
                }
            }
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("non-canonical CIDR should fail validation");
    assert!(
        error
            .to_string()
            .contains("services.api.deploy.egress.allow[0].cidr")
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn validate_rejects_unrecognized_config() {
    let path = temp_path("validate-unknown", "jsonc");
    std::fs::write(&path, r#"{"foo":1}"#).expect("write");
    let err = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("unknown config should fail");
    assert!(err.to_string().contains("unrecognized config"));
    let _ = std::fs::remove_file(path);
}

#[test]
fn config_validate_accepts_local_and_aws_sources() {
    for source in ["maestro.jsonc", "aws-secret://maestro/production/node1"] {
        let cli = crate::Cli::try_parse_from(["maestro", "config", "validate", source])
            .expect("config validate should parse");
        let Some(crate::CliCommand::Config {
            command:
                crate::ConfigCommand::Validate {
                    source: parsed_source,
                },
        }) = cli.command
        else {
            panic!("expected config validate");
        };
        assert_eq!(parsed_source, source);
    }
    assert!(
        crate::Cli::try_parse_from(["maestro", "config", "verify", "maestro.jsonc"]).is_err(),
        "the redundant config verify command must remain removed"
    );
}

#[tokio::test]
async fn validate_reports_the_full_path_of_a_missing_field() {
    let path = temp_path("validate-missing-field", "jsonc");
    std::fs::write(
        &path,
        r#"{
            cluster: {
                name: "test",
                nodes: {
                    node1: { subnet: "172.22.1.0/24", role: "master" }
                }
            },
            node: "node1",
            ingress: { port: 8080 },
            "encryption-key": "secret"
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("missing endpoint should fail validation");
    assert!(
        error
            .to_string()
            .contains("cluster.nodes.node1.endpoint: required field is missing"),
        "unexpected error: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn validate_reports_the_full_path_of_an_invalid_field() {
    let path = temp_path("validate-invalid-field", "jsonc");
    std::fs::write(
        &path,
        r#"{
            cluster: {
                name: "test",
                nodes: {
                    node1: {
                        endpoint: "not-an-ip",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    }
                }
            },
            node: "node1",
            ingress: { port: 8080 },
            "encryption-key": "secret"
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("invalid endpoint should fail validation");
    assert!(
        error.to_string().contains("cluster.nodes.node1.endpoint:"),
        "unexpected error: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn validate_reports_the_path_of_a_semantically_invalid_field() {
    let path = temp_path("validate-invalid-semantic-field", "jsonc");
    std::fs::write(
        &path,
        r#"{
            cluster: {
                name: "test",
                nodes: {
                    node1: {
                        endpoint: "8.8.8.8",
                        subnet: "172.22.1.0/24",
                        role: "master"
                    }
                },
                "control-allow-cidrs": ["10.0.0.0/8"],
                "join-secret": "12345678901234567890123456789012"
            },
            node: "node1",
            ingress: { port: 8080 },
            "encryption-key": "secret",
            "jwt-secret-key": "12345678901234567890123456789012"
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("public endpoint should fail validation");
    assert!(
        error
            .to_string()
            .contains("cluster.nodes.node1.endpoint: IP `8.8.8.8` must be private"),
        "unexpected error: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn validate_reports_unknown_strict_fields_as_invalid() {
    let path = temp_path("validate-unknown-strict-field", "jsonc");
    std::fs::write(
        &path,
        r#"{
            cluster: {
                name: "test",
                nodes: {
                    node1: {
                        endpoint: "10.0.0.10",
                        subnet: "172.22.1.0/24",
                        role: "master",
                        weight: 10
                    }
                }
            },
            node: "node1",
            ingress: { port: 8080 },
            "encryption-key": "secret"
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("unknown cluster node field should fail validation");
    assert!(
        error
            .to_string()
            .contains("cluster.nodes.node1.weight: unknown field"),
        "unexpected error: {error}"
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn validate_summarizes_type_errors_without_nested_context() {
    let path = temp_path("validate-type-error", "jsonc");
    std::fs::write(
        &path,
        r#"{
            cluster: { name: "test", nodes: [] },
            ingress: { port: 8080 },
            "encryption-key": "secret"
        }"#,
    )
    .expect("write");

    let error = run_validate(path.to_str().expect("UTF-8 temp path"))
        .await
        .expect_err("array-shaped nodes should fail validation");
    assert_eq!(
        error.to_string(),
        format!(
            "invalid config: {}\n  cluster.nodes: expected an object, got an array",
            path.display()
        )
    );
    let _ = std::fs::remove_file(path);
}

#[tokio::test]
async fn config_diagnostics_list_every_ignored_field_path() {
    let path = temp_path("validate-ignored-fields", "jsonc");
    std::fs::write(
        &path,
        r#"{
            "$schema": "https://example.test/maestro.start.schema.json",
            cluster: { name: "test" },
            ingress: { port: 8080, porrt: 8081 },
            subnet: "172.22.1.0/24",
            "encryption-key": "secret",
            builder: "depot",
            typo: true
        }"#,
    )
    .expect("write");

    let (_, ignored_fields) = crate::config::load_config_with_diagnostics(&path.to_string_lossy())
        .await
        .expect("config should otherwise be valid");
    assert_eq!(ignored_fields, ["builder", "ingress.porrt", "typo"]);
    let _ = std::fs::remove_file(path);
}

#[test]
fn services_config_diagnostics_list_nested_ignored_field_paths() {
    let (_, ignored_fields) = crate::cli::rollout::parse_cluster_config_with_diagnostics(
        r#"{
            services: {
                api: {
                    name: "API",
                    image: "example/api:latest",
                    deploy: {
                        replicas: 1,
                        replicaCount: 2,
                        egress: {
                            allow: [{
                                cidr: "10.0.10.0/24",
                                ports: [5432],
                                protocol: "tcp"
                            }]
                        }
                    }
                }
            }
        }"#,
    )
    .expect("config should otherwise be valid");
    assert_eq!(
        ignored_fields,
        [
            "services.api.deploy.egress.allow.0.protocol",
            "services.api.deploy.replicaCount"
        ]
    );
}

#[test]
fn quick_start_command_matches_the_cli() {
    let parsed = crate::Cli::try_parse_from([
        "maestro",
        "daemon",
        "start",
        "--config",
        "maestro.jsonc",
        "--admin-port",
        "3001",
        "--data-dir",
        "./data",
        "--project-dir",
        ".",
    ]);
    assert!(parsed.is_ok(), "quick-start command must remain parseable");
}

#[test]
fn dead_letter_admin_commands_match_the_cli() {
    for action in ["list", "export", "purge"] {
        let mut args = vec![
            "maestro",
            "daemon",
            "dead-letters",
            "--data-dir",
            "./data",
            "--cluster-name",
            "test",
            action,
        ];
        if action == "export" {
            args.extend(["--output", "dead-letters.jsonl"]);
        } else if action == "purge" {
            args.push("--all");
        }
        assert!(
            crate::Cli::try_parse_from(args).is_ok(),
            "dead-letter {action} command must remain parseable"
        );
    }
}

#[test]
fn start_schema_matches_serialized_config_fields() {
    let config = crate::config::StartConfig {
        cluster: crate::config::ClusterConfig {
            name: "test".to_string(),
            ..Default::default()
        },
        node: Default::default(),
        ingress: crate::config::IngressConfig {
            port: Some(80),
            ports: vec![443],
        },
        subnet: Some("172.22.0.0/16".to_string()),
        egress: crate::config::EgressConfig {
            deny: vec!["10.0.0.0/8".to_string()],
            allow: vec!["10.1.0.0/16".to_string()],
        },
        encryption_key: "secret".to_string(),
        tailscale: Some(crate::config::TailscaleConfig {
            auth_key: "auth-key".to_string(),
            advertise_routes: vec!["172.22.0.0/16".to_string()],
        }),
        jwt_secret_key: Some("jwt-secret".to_string()),
        tags: vec!["env:test".to_string()],
        datadog: Some(crate::config::DatadogConfig {
            api_key: "api-key".to_string(),
            site: Some("datadoghq.com".to_string()),
            include_ingress_logs: true,
            include_tailscale_logs: true,
            logs: crate::config::DatadogLogsConfig {
                include_healthcheck: false,
            },
            include_metrics: true,
        }),
        system: Some(crate::config::SystemType::Nixos),
        runtime: crate::config::RuntimeType::Docker,
        depot: Some(crate::config::DepotConfig {
            token: Some(crate::utils::crypto::SecretString::new(
                "depot-token".to_string(),
            )),
        }),
        cloudflare: Some(crate::config::CloudflareConfig {
            tunnel: crate::config::CloudflareTunnelConfig {
                token: crate::utils::crypto::SecretString::new("tunnel-token".to_string()),
                replicas: Some(2),
            },
        }),
        slack: Some(crate::config::SlackConfig {
            webhook_url: crate::utils::crypto::SecretString::new(
                "https://hooks.slack.test".to_string(),
            ),
        }),
        github: Some(crate::config::GithubConfig {
            token: crate::utils::crypto::SecretString::new("github-token".to_string()),
            preview_domain: "preview.example.test".to_string(),
            poll_interval_secs: 60,
            max_concurrent_previews: 10,
        }),
        homepage: Some("http://maestro.example.test".to_string()),
        log_backup: Some(crate::config::LogBackupConfig {
            bucket: "maestro-logs".to_string(),
            kms_key_id: "arn:aws:kms:us-west-2:123456789012:key/test".to_string(),
            region: Some("us-west-2".to_string()),
            prefix: Some("clusters/test".to_string()),
            retention_days: Some(30),
        }),
        disable_etcd_cert: true,
        allow_cli_deployment: true,
        allow_exec: true,
    };
    let model = serde_json::to_value(config).expect("serialize start config");
    let schema: Value = serde_json::from_str(START_SCHEMA).expect("parse start schema");

    assert_object_keys(
        &model,
        &schema,
        &[],
        &[
            "$schema",
            "$extends",
            "encryptionKey",
            "jwtSecretKey",
            "logBackup",
            "disableEtcdCert",
            "allowCliDeployment",
            "allowExec",
        ],
    );
    assert_object_keys(
        &model["cluster"],
        &schema["properties"]["cluster"],
        &[],
        &["controlAllowCidrs", "joinSecret"],
    );
    assert!(model["node"].is_null());
    assert_object_keys(
        &model["ingress"],
        &schema["properties"]["ingress"],
        &[],
        &[],
    );
    assert_object_keys(&model["egress"], &schema["properties"]["egress"], &[], &[]);
    assert_object_keys(
        &model["tailscale"],
        &schema["properties"]["tailscale"],
        &[],
        &["authKey", "advertiseRoutes"],
    );
    assert_object_keys(
        &model["datadog"],
        &schema["properties"]["datadog"],
        &[],
        &[
            "apiKey",
            "includeIngressLogs",
            "includeTailscaleLogs",
            "includeMetrics",
        ],
    );
    assert_object_keys(
        &model["datadog"]["logs"],
        &schema["properties"]["datadog"]["properties"]["logs"],
        &[],
        &["includeHealthcheck"],
    );
    assert_object_keys(&model["depot"], &schema["properties"]["depot"], &[], &[]);
    assert_object_keys(
        &model["cloudflare"],
        &schema["properties"]["cloudflare"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["cloudflare"]["tunnel"],
        &schema["properties"]["cloudflare"]["properties"]["tunnel"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["slack"],
        &schema["properties"]["slack"],
        &[],
        &["webhookUrl"],
    );
    assert_object_keys(
        &model["github"],
        &schema["properties"]["github"],
        &[],
        &["previewDomain", "pollIntervalSecs", "maxConcurrentPreviews"],
    );
    assert_object_keys(
        &model["log-backup"],
        &schema["properties"]["log-backup"],
        &[],
        &["kmsKeyId", "retentionDays"],
    );

    let node_schema =
        &schema["properties"]["cluster"]["properties"]["nodes"]["additionalProperties"];
    assert_eq!(
        node_schema["required"],
        serde_json::json!(["endpoint", "subnet"])
    );
    assert_eq!(node_schema["properties"]["role"]["default"], "hybrid");
}

#[test]
fn services_schema_matches_serialized_config_fields() {
    use crate::deployment::types::{
        Command, DepotConfig, EnvConfig, IngressConfig, PreviewConfig, PreviewEnvConfig,
        SecretKeyMeta, SecretsConfig, ServiceBuildConfig, ServiceConfig, ServiceDeployConfig,
        ServiceEgressConfig, ServiceEgressRule, VolumeMount, VolumeOwner,
    };

    let env = EnvConfig {
        source: Some("aws-secret://env".to_string()),
        items: HashMap::from([(
            "NAME".to_string(),
            crate::utils::crypto::SecretString::new("value".to_string()),
        )]),
    };
    let service = ServiceConfig {
        id: "api".to_string(),
        name: "API".to_string(),
        version: "version".to_string(),
        build: Some(ServiceBuildConfig {
            repo: Some("https://example.test/repo.git".to_string()),
            branch: Some("main".to_string()),
            dockerfile: "Dockerfile".to_string(),
            watch: true,
            registry: Some("registry.example.test".to_string()),
            depot: Some(DepotConfig {
                project: "project".to_string(),
            }),
            env: env.clone(),
            secrets: env.clone(),
        }),
        image: Some("registry.example.test/api:latest".to_string()),
        deploy: ServiceDeployConfig {
            flags: vec!["--verbose".to_string()],
            expose_ports: vec![8080],
            command: Some(Command {
                command: "server".to_string(),
                args: vec!["--port".to_string(), "8080".to_string()],
            }),
            healthcheck_path: Some("/health".to_string()),
            healthcheck_interval: 30,
            replicas: 2,
            exec: true,
            max_restarts: Some(5),
            env: env.clone(),
            secrets: Some(SecretsConfig {
                mount_path: "/run/secrets/app.env".to_string(),
                source: Some("aws-secret://app".to_string()),
                items: HashMap::from([("TOKEN".to_string(), "secret".to_string())]),
                keys: HashMap::from([(
                    "TOKEN".to_string(),
                    SecretKeyMeta {
                        hash: "hash".to_string(),
                        changed: true,
                    },
                )]),
            }),
            volumes: vec![VolumeMount {
                host_path: "/data/api".to_string(),
                mount_path: "/app/data".to_string(),
                read_only: true,
                owner: Some(VolumeOwner {
                    uid: 1000,
                    gid: Some(1000),
                }),
            }],
            node_affinity: Some(crate::cluster::NodeAffinity {
                node_id: Some("worker000001".to_string()),
                labels: std::collections::BTreeMap::from([(
                    "zone".to_string(),
                    "west".to_string(),
                )]),
            }),
            egress: ServiceEgressConfig {
                allow: vec![ServiceEgressRule {
                    cidr: "10.0.10.0/24".to_string(),
                    ports: vec![5432],
                }],
            },
        },
        ingress: Some(IngressConfig {
            host: Some("api.example.test".to_string()),
            hosts: vec!["api.internal.test".to_string()],
            port: Some(8080),
            session_affinity: Some(crate::deployment::types::SessionAffinityConfig {
                header: "X-Session-Node".to_string(),
            }),
        }),
        preview: Some(PreviewConfig {
            enabled: true,
            close_grace_period: "1d".to_string(),
            replicas: 1,
            env: PreviewEnvConfig {
                items: env.items.clone(),
            },
        }),
        preview_source: None,
    };
    let mut model = serde_json::to_value(service).expect("serialize service config");
    let model = model.as_object_mut().expect("service object");
    model.remove("id");
    model.remove("version");
    let model = Value::Object(model.clone());
    let schema: Value = serde_json::from_str(SERVICES_SCHEMA).expect("parse services schema");
    let definitions = &schema["$defs"];

    assert_object_keys(&model, &definitions["service"], &[], &[]);
    assert_object_keys(&model["build"], &definitions["build"], &[], &[]);
    assert!(model["ingress"].get("blockedIps").is_none());
    assert_object_keys(
        &model["build"]["depot"],
        &definitions["build"]["properties"]["depot"],
        &[],
        &[],
    );
    assert_object_keys(&model["build"]["env"], &definitions["envConfig"], &[], &[]);
    assert_object_keys(&model["deploy"], &definitions["deploy"], &[], &[]);
    assert_object_keys(
        &model["deploy"]["nodeAffinity"],
        &definitions["deploy"]["properties"]["nodeAffinity"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["egress"],
        &definitions["deploy"]["properties"]["egress"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["egress"]["allow"][0],
        &definitions["deploy"]["properties"]["egress"]["properties"]["allow"]["items"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["command"],
        &definitions["command"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["secrets"],
        &definitions["deploy"]["properties"]["secrets"],
        &["keys"],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["volumes"][0],
        &definitions["deploy"]["properties"]["volumes"]["items"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["deploy"]["volumes"][0]["owner"],
        &definitions["deploy"]["properties"]["volumes"]["items"]["properties"]["owner"],
        &[],
        &[],
    );
    assert_object_keys(&model["ingress"], &definitions["ingress"], &[], &[]);
    assert_object_keys(&model["preview"], &definitions["preview"], &[], &[]);
    assert_object_keys(
        &model["ingress"]["sessionAffinity"],
        &definitions["sessionAffinity"],
        &[],
        &[],
    );
}

fn assert_object_keys(
    model: &Value,
    schema: &Value,
    ignored_model_keys: &[&str],
    ignored_schema_keys: &[&str],
) {
    let model_keys = model
        .as_object()
        .expect("model value must be an object")
        .keys()
        .filter(|key| !ignored_model_keys.contains(&key.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    let schema_keys = schema["properties"]
        .as_object()
        .expect("schema must have object properties")
        .keys()
        .filter(|key| !ignored_schema_keys.contains(&key.as_str()))
        .cloned()
        .collect::<BTreeSet<_>>();
    assert_eq!(model_keys, schema_keys);
}
