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

#[test]
fn cluster_template_validates_as_cluster_config() {
    let path = temp_path("validate-cluster", "jsonc");
    write_template(&path, DEFAULT_START_TEMPLATE).expect("write");
    run_validate(&path).expect("start template should validate as cluster config");
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
    assert!(config.cluster.subnets.is_empty());
    assert_eq!(config.node.role, crate::cluster::NodeRole::Hybrid);
    assert!(config.cluster.ca_sha256.is_none());
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

#[test]
fn services_template_validates_as_services_config() {
    let path = temp_path("validate-services", "jsonc");
    write_template(&path, DEFAULT_CLUSTER_TEMPLATE).expect("write");
    run_validate(&path).expect("cluster template should validate as services config");
    let _ = std::fs::remove_file(path);
}

#[test]
fn validate_rejects_unrecognized_config() {
    let path = temp_path("validate-unknown", "jsonc");
    std::fs::write(&path, r#"{"foo":1}"#).expect("write");
    let err = run_validate(&path).expect_err("unknown config should fail");
    assert!(err.to_string().contains("unrecognized config"));
    let _ = std::fs::remove_file(path);
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
        log_backup: Some(crate::config::LogBackupConfig {
            bucket: "maestro-logs".to_string(),
            kms_key_id: "arn:aws:kms:us-west-2:123456789012:key/test".to_string(),
            region: Some("us-west-2".to_string()),
            prefix: Some("clusters/test".to_string()),
            retention_days: Some(30),
        }),
        disable_etcd_cert: true,
        allow_cli_deployment: true,
    };
    let model = serde_json::to_value(config).expect("serialize start config");
    let schema: Value = serde_json::from_str(START_SCHEMA).expect("parse start schema");

    assert_object_keys(&model, &schema, &[], &["$schema"]);
    assert_object_keys(
        &model["cluster"],
        &schema["properties"]["cluster"],
        &[],
        &[],
    );
    assert_object_keys(&model["node"], &schema["properties"]["node"], &[], &[]);
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
        &[],
    );
    assert_object_keys(
        &model["datadog"],
        &schema["properties"]["datadog"],
        &[],
        &[],
    );
    assert_object_keys(
        &model["datadog"]["logs"],
        &schema["properties"]["datadog"]["properties"]["logs"],
        &[],
        &[],
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
    assert_object_keys(&model["slack"], &schema["properties"]["slack"], &[], &[]);
    assert_object_keys(
        &model["log-backup"],
        &schema["properties"]["log-backup"],
        &[],
        &[],
    );
}

#[test]
fn services_schema_matches_serialized_config_fields() {
    use crate::deployment::types::{
        Command, DepotConfig, EnvConfig, IngressConfig, SecretKeyMeta, SecretsConfig,
        ServiceBuildConfig, ServiceConfig, ServiceDeployConfig, VolumeMount, VolumeOwner,
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
        },
        ingress: Some(IngressConfig {
            host: Some("api.example.test".to_string()),
            hosts: vec!["api.internal.test".to_string()],
            port: Some(8080),
            session_affinity: Some(crate::deployment::types::SessionAffinityConfig {
                header: "X-Session-Node".to_string(),
            }),
        }),
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
