use kernel_api::{AnnotationKey, BuiltinKind, Node};
use serde_json::json;
use sha2::{Digest, Sha256};

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::node_entries;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";
const SUMMARY_ANNOTATION: &str = "migration.maestro.dev/legacy-derived-state";

#[test]
fn cutover_validates_and_summarizes_regenerable_cluster_state() -> TestResult {
    let image = "registry.example/api@sha256:abc";
    let digest = hex::encode(Sha256::digest(image.as_bytes()));
    let holder = json!({
        "image": image,
        "nodeId": "node-a",
        "availableAtMs": 2_500
    });
    let snapshot = cutover_snapshot(vec![
        json_entry(
            "/maetro/cluster/stats/node-a",
            json!({
                "reportedAtMs": 3_000,
                "version": "0.9.0",
                "uptimeMs": 2_000,
                "spool": {
                    "rowCount": 4,
                    "highWatermark": 8,
                    "oldestEntryAtMs": 1_000,
                    "databaseBytes": 4096
                },
                "sinks": [{
                    "id": "archive",
                    "cursor": 7,
                    "pendingEntries": 1,
                    "oldestPendingAtMs": 2_000,
                    "lastSuccessAtMs": 2_500,
                    "lastErrorAtMs": null,
                    "lastError": null,
                    "consecutiveFailures": 0,
                    "lastCursorAdvanceAtMs": 2_500,
                    "filteredEntries": 3
                }],
                "deadLetters": {
                    "count": 0,
                    "capacity": 100,
                    "payloadBytes": 0,
                    "latestAtMs": null,
                    "latestStatus": null,
                    "latestError": null
                }
            }),
        ),
        json_entry(
            "/maetro/cluster/disks/node-a",
            json!([{
                "name": "root",
                "mountPoint": "/",
                "totalBytes": 1000,
                "availableBytes": 400,
                "fileSystem": "ext4"
            }]),
        ),
        json_entry(
            "/maetro/cluster/dns/api",
            json!({
                "serviceId": "api",
                "stableFqdn": "api.test.maestro.internal",
                "viaIngress": true,
                "addresses": ["10.42.1.2"],
                "replicaRecords": [["api-0.test.maestro.internal", "10.42.1.2"]]
            }),
        ),
        json_entry(
            &format!("/maetro/cluster/image-holders/{digest}/node-a"),
            holder.clone(),
        ),
        json_entry(
            &format!("/maetro/cluster/node-image-holders/node-a/{digest}"),
            holder,
        ),
        json_entry(
            "/maetro/cluster/traefik-service-map/api-g-generation",
            json!({
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "nodeId": "node-a"
            }),
        ),
        json_entry(
            "/maetro/cluster/unschedulable",
            json!([{
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "replicaIndex": 2,
                "reason": "no schedulable node"
            }]),
        ),
        json_entry(
            "/maetro/system/rbac-ready",
            json!({"version": 3, "initializedBy": "node-a"}),
        ),
    ])?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let master: Node = decode_write(&plan, BuiltinKind::Node)?;
    let summary = master
        .meta
        .annotations
        .get(&AnnotationKey(SUMMARY_ANNOTATION.to_owned()))
        .ok_or("derived-state summary annotation is missing")?;
    assert_eq!(
        serde_json::from_str::<serde_json::Value>(summary)?,
        json!({
            "controllerStats": 1,
            "diskSnapshots": 1,
            "dnsRecordSets": 1,
            "imageHolders": 1,
            "traefikServiceMappings": 1,
            "unschedulableReplicas": 1,
            "rbacVersion": 3
        })
    );
    Ok(())
}

#[test]
fn cutover_requires_the_legacy_leadership_lease_to_expire() -> TestResult {
    for key in ["/maetro/cluster/leader", "/maetro/cluster/leader/lease"] {
        let snapshot = cutover_snapshot(vec![LegacyEntry::new(key, b"node-a".to_vec())])?;
        let Err(error) = plan_legacy_snapshot(&snapshot, MASTER_SECRET) else {
            return Err("active leader should block cutover".into());
        };
        assert!(matches!(error, LegacyPlanError::DecodeLegacyState { .. }));
        assert!(error.to_string().contains("still active"));
    }
    Ok(())
}

#[test]
fn cutover_rejects_inconsistent_image_holder_indexes() -> TestResult {
    let image = "registry.example/api@sha256:abc";
    let digest = hex::encode(Sha256::digest(image.as_bytes()));
    let snapshot = cutover_snapshot(vec![json_entry(
        &format!("/maetro/cluster/image-holders/{digest}/node-a"),
        json!({
            "image": image,
            "nodeId": "node-a",
            "availableAtMs": 2_500
        }),
    )])?;

    let Err(error) = plan_legacy_snapshot(&snapshot, MASTER_SECRET) else {
        return Err("inconsistent holder index should block cutover".into());
    };
    assert!(
        error
            .to_string()
            .contains("forward and reverse indexes disagree")
    );
    Ok(())
}

#[test]
fn cutover_rejects_derived_state_with_missing_owners() -> TestResult {
    for entry in [
        json_entry("/maetro/cluster/disks/missing-node", json!([])),
        json_entry(
            "/maetro/cluster/dns/missing-service",
            json!({
                "serviceId": "missing-service",
                "stableFqdn": "missing.test.maestro.internal",
                "addresses": [],
                "replicaRecords": []
            }),
        ),
        json_entry(
            "/maetro/cluster/traefik-service-map/stale",
            json!({
                "serviceId": "api",
                "deploymentId": "missing-deployment"
            }),
        ),
    ] {
        let snapshot = cutover_snapshot(vec![entry])?;
        assert!(matches!(
            plan_legacy_snapshot(&snapshot, MASTER_SECRET),
            Err(LegacyPlanError::DecodeLegacyState { .. })
        ));
    }
    Ok(())
}

fn cutover_snapshot(extra: Vec<LegacyEntry>) -> Result<LegacySnapshot, crate::SnapshotError> {
    let mut entries = service_entries();
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    entries.extend(extra);
    LegacySnapshot::new(entries)
}

fn service_entries() -> Vec<LegacyEntry> {
    let config = json!({
        "id": "api",
        "name": "API",
        "version": "v1",
        "image": "registry.example/api:latest",
        "deploy": {"exposePorts": [8080]}
    });
    vec![
        json_entry(
            "/maetro/services/api/info",
            json!({
                "config": config.clone(),
                "deployFrozen": false,
                "replicasOverride": null
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
                "config": config,
                "build": {"dockerImageId": "registry.example/api@sha256:abc"}
            }),
        ),
    ]
}

fn json_entry(key: &str, value: serde_json::Value) -> LegacyEntry {
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
