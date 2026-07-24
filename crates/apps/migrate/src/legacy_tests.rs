use serde_json::json;

use kernel_api::{
    AnnotationKey, ArtifactTemplate, BUILD_WATCH_REVISION_ANNOTATION, Build, BuildSource,
    BuiltinKind, Deployment, DeploymentPhase, FirewallPolicy, FirewallVerdict, IngressRoute,
    RolloutState, Service, TransportProtocol,
};

use crate::legacy_fixtures::{cluster_state, encrypt_for_test};
use crate::legacy_node_tests::node_entries;
use crate::legacy_services::{LegacyServiceCatalog, LegacyServiceError};
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

pub(crate) const MASTER_SECRET: &str = "correct horse battery staple for maestro";

#[test]
fn service_catalog_authenticates_and_joins_legacy_records() -> TestResult {
    let snapshot = service_snapshot(vec![
        encrypted(
            "/maetro/services/api/deploy-1/deploy/env",
            json!({"PUBLIC_NAME": "api"}),
        )?,
        encrypted(
            "/maetro/services/api/deploy-1/deploy/secrets",
            json!({"TOKEN": "secret"}),
        )?,
        encrypted(
            "/maetro/services/api/deploy-1/build/env",
            json!({"TARGET": "release"}),
        )?,
        encrypted(
            "/maetro/services/api/deploy-1/build/secrets",
            json!({"CARGO_TOKEN": "build-secret"}),
        )?,
        encrypted(
            "/maetro/services/api/deploy-1/preview/env",
            json!({"PREVIEW": "true"}),
        )?,
        LegacyEntry::new("/maetro/cluster/meta", b"cluster".to_vec()),
    ])?;

    let catalog = LegacyServiceCatalog::decode(&snapshot, MASTER_SECRET)?;
    let service = catalog
        .services
        .get("api")
        .ok_or("decoded service should exist")?;
    let deployment = service
        .deployments
        .first()
        .ok_or("decoded deployment should exist")?;

    assert_eq!(service.next_history_index, 1);
    assert_eq!(deployment.history_index, 0);
    assert_eq!(
        deployment.data.deploy_environment.get("PUBLIC_NAME"),
        Some(&"api".to_owned())
    );
    assert_eq!(
        deployment.data.deploy_secrets.get("TOKEN"),
        Some(&"secret".to_owned())
    );
    assert_eq!(
        deployment.data.build_environment.get("TARGET"),
        Some(&"release".to_owned())
    );
    assert_eq!(
        deployment.data.build_secrets.get("CARGO_TOKEN"),
        Some(&"build-secret".to_owned())
    );
    assert_eq!(
        deployment.data.preview_environment.get("PREVIEW"),
        Some(&"true".to_owned())
    );
    assert_eq!(catalog.unclaimed.len(), 1);
    assert_eq!(
        catalog.unclaimed.first().map(LegacyEntry::key),
        Some("/maetro/cluster/meta")
    );
    Ok(())
}

#[test]
fn service_catalog_rejects_wrong_encryption_secret() -> TestResult {
    let snapshot = service_snapshot(vec![encrypted(
        "/maetro/services/api/deploy-1/deploy/env",
        json!({"PUBLIC_NAME": "api"}),
    )?])?;

    assert!(matches!(
        LegacyServiceCatalog::decode(&snapshot, "wrong secret"),
        Err(LegacyServiceError::Crypto(_))
    ));
    Ok(())
}

#[test]
fn service_catalog_rejects_counter_drift_and_orphan_sidecars() -> TestResult {
    let mut counter_drift = fixture_entries();
    counter_drift
        .retain(|entry| entry.key() != "/maetro/services/api/deployments/history-next-index");
    counter_drift.push(LegacyEntry::new(
        "/maetro/services/api/deployments/history-next-index",
        b"2".to_vec(),
    ));
    let snapshot = LegacySnapshot::new(counter_drift)?;
    assert!(matches!(
        LegacyServiceCatalog::decode(&snapshot, MASTER_SECRET),
        Err(LegacyServiceError::InvalidHistoryCounter { .. })
    ));

    let mut missing_counter = fixture_entries();
    missing_counter
        .retain(|entry| entry.key() != "/maetro/services/api/deployments/history-next-index");
    let snapshot = LegacySnapshot::new(missing_counter)?;
    assert!(matches!(
        LegacyServiceCatalog::decode(&snapshot, MASTER_SECRET),
        Err(LegacyServiceError::MissingHistoryCounter { .. })
    ));

    let snapshot = service_snapshot(vec![encrypted(
        "/maetro/services/api/missing/deploy/env",
        json!({"PUBLIC_NAME": "api"}),
    )?])?;
    assert!(matches!(
        LegacyServiceCatalog::decode(&snapshot, MASTER_SECRET),
        Err(LegacyServiceError::OrphanSidecar { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_converts_service_and_deployment_resources() -> TestResult {
    let snapshot = cutover_service_snapshot(vec![
        encrypted(
            "/maetro/services/api/deploy-1/deploy/env",
            json!({"PUBLIC_NAME": "api"}),
        )?,
        encrypted(
            "/maetro/services/api/deploy-1/deploy/secrets",
            json!({"TOKEN": "secret"}),
        )?,
    ])?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    assert_eq!(plan.source_digest(), snapshot.digest());
    assert_eq!(plan.writes().len(), 3);
    let service: Service = decode_write(&plan, BuiltinKind::Service)?;
    let deployment: Deployment = decode_write(&plan, BuiltinKind::Deployment)?;

    assert_eq!(service.meta.id.as_str(), "api");
    assert_eq!(service.meta.generation.0, 1);
    assert_eq!(service.status.rollout, RolloutState::Frozen);
    assert_eq!(service.status.replica_override, Some(3));
    assert_eq!(
        service
            .status
            .active_deployment_id
            .as_ref()
            .map(|id| id.as_str()),
        Some("deploy-1")
    );
    assert_eq!(
        service.spec.environment.get("PUBLIC_NAME"),
        Some(&"api".to_owned())
    );
    let secret_mount = service
        .spec
        .secrets
        .as_ref()
        .ok_or_else(|| std::io::Error::other("missing secret mount"))?;
    let kernel_api::SecretMountSpec::Dotenv { items, .. } = secret_mount else {
        return Err("migrated secret mount is not dotenv".into());
    };
    assert_eq!(
        items.get("TOKEN").map(|value| value.expose()),
        Some("secret")
    );
    assert_eq!(deployment.spec.service_generation.0, 1);
    assert_eq!(deployment.status.phase, DeploymentPhase::Ready);
    assert_eq!(
        deployment.status.image_digest.as_deref(),
        Some("registry.example/api@sha256:abc")
    );
    assert_eq!(deployment.status.ready_at.map(|time| time.0), Some(2_000));
    Ok(())
}

#[test]
fn cutover_plan_rejects_unclaimed_key_families() -> TestResult {
    let snapshot = cutover_service_snapshot(vec![LegacyEntry::new(
        "/maetro/cluster/meta",
        b"cluster".to_vec(),
    )])?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::UnsupportedLegacyKey { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_preserves_build_network_and_preview_policy() -> TestResult {
    let config = json!({
        "id": "web",
        "name": "Web",
        "version": "v2",
        "build": {
            "repo": "git@github.com:Example/Web.git",
            "branch": "main",
            "dockerfile": "Dockerfile",
            "watch": true,
            "env": {"source": "env://build"},
            "secrets": {"source": "secret://build"}
        },
        "deploy": {
            "exposePorts": [9090],
            "healthcheckPath": "/ready",
            "healthcheckInterval": 15,
            "replicas": 2,
            "env": {"source": "env://deploy"},
            "nodeAffinity": {"labels": {"region": "west"}},
            "egress": {"allow": [{"cidr": "10.0.0.0/8", "ports": [443]}]}
        },
        "ingress": {
            "host": "web.example.test",
            "hosts": ["alt.example.test"],
            "port": 8080,
            "sessionAffinity": {"header": "X-Session"}
        },
        "preview": {
            "enabled": true,
            "closeGracePeriod": "12h",
            "replicas": 1,
            "env": {"source": "env://preview"}
        }
    });
    let mut entries = vec![
        json_entry(
            "/maetro/services/web/info",
            json!({"config": config.clone()}),
        ),
        LegacyEntry::new(
            "/maetro/services/web/deployments/history-next-index",
            b"1".to_vec(),
        ),
        json_entry(
            "/maetro/services/web/deployments/history/0000000000",
            json!({
                "id": "deploy-web",
                "createdAt": 5_000,
                "deployedAt": 6_000,
                "status": "READY",
                "config": config,
                "gitCommit": {
                    "reference": "0123456789abcdef",
                    "message": "Ship it"
                },
                "build": {
                    "dockerImageId": "registry.example/web@sha256:def",
                    "sourceNodeId": "node-a"
                }
            }),
        ),
        encrypted(
            "/maetro/services/web/deploy-web/deploy/env",
            json!({"RUST_LOG": "info"}),
        )?,
        encrypted(
            "/maetro/services/web/deploy-web/build/env",
            json!({"TARGET": "release"}),
        )?,
        encrypted(
            "/maetro/services/web/deploy-web/build/secrets",
            json!({"TOKEN": "build-secret"}),
        )?,
        encrypted(
            "/maetro/services/web/deploy-web/preview/env",
            json!({"PREVIEW": "true"}),
        )?,
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    assert_eq!(plan.writes().len(), 6);
    let service: Service = decode_write(&plan, BuiltinKind::Service)?;
    let deployment: Deployment = decode_write(&plan, BuiltinKind::Deployment)?;
    let build: Build = decode_write(&plan, BuiltinKind::Build)?;
    let route: IngressRoute = decode_write(&plan, BuiltinKind::IngressRoute)?;
    let policy: FirewallPolicy = decode_write(&plan, BuiltinKind::FirewallPolicy)?;

    assert_eq!(service.spec.exposed_ports, [8080, 9090]);
    assert_eq!(
        service.spec.environment.get("RUST_LOG").map(String::as_str),
        Some("info")
    );
    assert_eq!(
        service
            .meta
            .annotations
            .get(&AnnotationKey(BUILD_WATCH_REVISION_ANNOTATION.to_owned()))
            .map(String::as_str),
        Some("0123456789abcdef")
    );
    let preview = service
        .spec
        .preview
        .as_ref()
        .ok_or_else(|| std::io::Error::other("missing preview policy"))?;
    assert_eq!(preview.close_grace_period_secs, 12 * 60 * 60);
    assert_eq!(
        preview.environment.get("PREVIEW").map(String::as_str),
        Some("true")
    );
    let ArtifactTemplate::Build { template } = &deployment.spec.service.artifact else {
        return Err(std::io::Error::other("missing deployment build template").into());
    };
    assert!(matches!(
        &template.source,
        BuildSource::Git { revision, .. } if revision == "0123456789abcdef"
    ));
    assert_eq!(build.status.image_digest, deployment.status.image_digest);
    assert_eq!(route.spec.hosts, ["alt.example.test", "web.example.test"]);
    assert_eq!(route.spec.target_port, 8080);
    assert_eq!(policy.spec.default_verdict, FirewallVerdict::Deny);
    assert_eq!(
        policy.spec.rules.first().map(|rule| rule.protocol),
        Some(TransportProtocol::Any)
    );
    Ok(())
}

fn service_snapshot(extra: Vec<LegacyEntry>) -> Result<LegacySnapshot, crate::SnapshotError> {
    let mut entries = fixture_entries();
    entries.extend(extra);
    LegacySnapshot::new(entries)
}

pub(crate) fn cutover_service_snapshot(
    extra: Vec<LegacyEntry>,
) -> Result<LegacySnapshot, crate::SnapshotError> {
    let mut entries = fixture_entries();
    entries.extend(extra);
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    LegacySnapshot::new(entries)
}

fn fixture_entries() -> Vec<LegacyEntry> {
    vec![
        json_entry(
            "/maetro/services/api/info",
            json!({
                "config": service_config(),
                "deployFrozen": true,
                "replicasOverride": 3
            }),
        ),
        LegacyEntry::new(
            "/maetro/services/api/deployments/history-next-index",
            b"1".to_vec(),
        ),
        json_entry(
            "/maetro/services/api/deployments/history/0000000000",
            json!({
                "id": "deploy-1",
                "createdAt": 1_000,
                "deployedAt": 2_000,
                "status": "READY",
                "config": service_config(),
                "build": {"dockerImageId": "registry.example/api@sha256:abc"}
            }),
        ),
    ]
}

fn service_config() -> serde_json::Value {
    json!({
        "id": "api",
        "name": "API",
        "version": "v1",
        "image": "registry.example/api:latest",
        "deploy": {
            "exposePorts": [8080],
            "healthcheckPath": "/health",
            "secrets": {
                "mountPath": "/run/secrets/env",
                "keys": {
                    "TOKEN": {"hash": "", "changed": false}
                }
            }
        }
    })
}

fn json_entry(key: &str, value: serde_json::Value) -> LegacyEntry {
    LegacyEntry::new(key, value.to_string().into_bytes())
}

pub(crate) fn encrypted(
    key: &str,
    value: serde_json::Value,
) -> Result<LegacyEntry, Box<dyn std::error::Error>> {
    let plaintext = serde_json::to_vec(&value)?;
    Ok(LegacyEntry::new(
        key,
        encrypt_for_test(MASTER_SECRET, &plaintext)?,
    ))
}

fn decode_write<Value: serde::de::DeserializeOwned>(
    plan: &crate::MigrationPlan,
    kind: BuiltinKind,
) -> Result<Value, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == kind)
        .ok_or_else(|| std::io::Error::other(format!("missing {kind} write")))?;
    Ok(serde_json::from_slice(write.value())?)
}
