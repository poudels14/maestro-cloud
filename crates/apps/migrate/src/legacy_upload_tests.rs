use serde_json::json;

use kernel_api::{AnnotationKey, ArtifactTemplate, BuiltinKind, Deployment, Service};

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::node_entries;
use crate::legacy_tests::MASTER_SECRET;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const LEGACY_ARCHIVE: &str = "api-deploy-upload.tar.gz";
const IMAGE_DIGEST: &str = "registry.example/api@sha256:abc";

#[test]
fn cutover_freezes_a_successful_upload_to_its_resolved_image() -> TestResult {
    let snapshot = upload_snapshot("READY", Some(IMAGE_DIGEST))?;
    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let service: Service = decode_write(&plan, BuiltinKind::Service)?;
    let deployment: Deployment = decode_write(&plan, BuiltinKind::Deployment)?;

    assert_eq!(image_reference(&service.spec.artifact), Some(IMAGE_DIGEST));
    assert_eq!(
        image_reference(&deployment.spec.service.artifact),
        Some(IMAGE_DIGEST)
    );
    assert!(deployment.spec.build_id.is_none());
    assert!(
        plan.writes()
            .iter()
            .all(|write| write.kind() != BuiltinKind::Build)
    );
    let annotation = AnnotationKey("migration.maestro.dev/legacy-upload-archive".to_owned());
    assert_eq!(
        service
            .meta
            .annotations
            .get(&annotation)
            .map(String::as_str),
        Some(LEGACY_ARCHIVE)
    );
    assert_eq!(
        deployment
            .meta
            .annotations
            .get(&annotation)
            .map(String::as_str),
        Some(LEGACY_ARCHIVE)
    );
    Ok(())
}

#[test]
fn cutover_rejects_an_upload_that_never_produced_an_image() -> TestResult {
    let snapshot = upload_snapshot("BUILDING", None)?;
    let error = match plan_legacy_snapshot(&snapshot, MASTER_SECRET) {
        Ok(_) => return Err("unresolved upload should block cutover".into()),
        Err(error) => error,
    };

    assert!(matches!(error, LegacyPlanError::InvalidDeployment { .. }));
    assert!(
        error
            .to_string()
            .contains("uploaded archive `api-deploy-upload.tar.gz` has no resolved image")
    );
    Ok(())
}

fn upload_snapshot(
    status: &str,
    image_digest: Option<&str>,
) -> Result<LegacySnapshot, crate::SnapshotError> {
    let config = json!({
        "id": "api",
        "name": "Uploaded API",
        "version": "v1-up",
        "build": {"dockerfile": "Dockerfile"},
        "deploy": {"exposePorts": [8080]}
    });
    let deployment = json!({
        "id": "deploy-upload",
        "createdAt": 1_000,
        "deployedAt": 2_000,
        "status": status,
        "config": config.clone(),
        "uploadArchive": LEGACY_ARCHIVE,
        "gitCommit": {
            "reference": "upload",
            "message": "maestro up: api-deploy-upload.tar.gz"
        },
        "build": image_digest.map(|image| json!({"dockerImageId": image}))
    });
    let mut entries = vec![
        entry(
            "/maetro/services/api/info",
            json!({"config": config, "deployFrozen": false}),
        ),
        LegacyEntry::new(
            "/maetro/services/api/deployments/history-next-index",
            b"1".to_vec(),
        ),
        entry(
            "/maetro/services/api/deployments/history/0000000000",
            deployment,
        ),
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    LegacySnapshot::new(entries)
}

fn image_reference(artifact: &ArtifactTemplate) -> Option<&str> {
    match artifact {
        ArtifactTemplate::Image { reference } => Some(reference),
        ArtifactTemplate::Build { .. } => None,
    }
}

fn entry(key: &str, value: serde_json::Value) -> LegacyEntry {
    LegacyEntry::new(key, value.to_string().into_bytes())
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
