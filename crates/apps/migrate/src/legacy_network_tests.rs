use kernel_api::{
    AnnotationKey, BuiltinKind, Generation, IngressBlocklist, TrafficGeneration,
    TrafficGenerationPhase,
};
use serde_json::json;

use crate::legacy_fixtures::cluster_state;
use crate::legacy_node_tests::node_entries;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";

#[test]
fn cutover_plan_converts_active_traffic_and_global_blocklist() -> TestResult {
    let mut entries = workload_entries();
    entries.extend([
        json_entry(
            "/maetro/cluster/traffic/api",
            json!({
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "trafficEpoch": 6,
                "activeAssignmentIds": ["assignment-1"],
                "activeNodeIds": ["node-a"],
                "generation": "0123456789abcdef",
                "routingFingerprint": "legacy-route-fingerprint",
                "switchedAtMs": 4_000,
                "drainOldAfterMs": 34_000
            }),
        ),
        LegacyEntry::new(
            "/maetro/cluster/ingress-blocklist/203.0.113.9",
            b"203.0.113.9".to_vec(),
        ),
        LegacyEntry::new(
            "/maetro/cluster/ingress-blocklist/2001:db8::9",
            b"2001:db8::9".to_vec(),
        ),
        LegacyEntry::new(
            "/maetro/cluster/ingress-blocklist-applied",
            b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa".to_vec(),
        ),
    ]);
    let snapshot = LegacySnapshot::new(entries)?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let traffic: TrafficGeneration = decode_write(&plan, BuiltinKind::TrafficGeneration)?;
    let blocklist: IngressBlocklist = decode_write(&plan, BuiltinKind::IngressBlocklist)?;

    assert_eq!(traffic.meta.id.as_str(), "traffic-0123456789abcdef");
    assert_eq!(traffic.spec.epoch, 6);
    assert_eq!(traffic.spec.routes.len(), 1);
    assert_eq!(traffic.spec.targets.len(), 1);
    assert_eq!(
        traffic
            .spec
            .targets
            .first()
            .map(|target| target.endpoint.to_string()),
        Some("10.42.1.5:8080".to_owned())
    );
    assert_eq!(traffic.status.phase, TrafficGenerationPhase::Active);
    assert_eq!(
        traffic.status.activated_at.map(|value| value.0),
        Some(4_000)
    );
    assert_eq!(
        traffic
            .meta
            .annotations
            .get(&AnnotationKey(
                "migration.maestro.dev/legacy-drain-old-after-ms".to_owned()
            ))
            .map(String::as_str),
        Some("34000")
    );
    assert_eq!(
        blocklist
            .spec
            .addresses
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>(),
        ["203.0.113.9", "2001:db8::9"]
    );
    assert_eq!(blocklist.status.applied_generation, Generation(0));
    assert!(blocklist.status.configuration_digest.is_none());
    Ok(())
}

#[test]
fn cutover_plan_rejects_traffic_without_ready_owned_assignments() -> TestResult {
    let mut entries = workload_entries();
    entries.retain(|entry| entry.key() != "/maetro/cluster/replica-states/node-a/assignment-1");
    entries.push(json_entry(
        "/maetro/cluster/traffic/api",
        json!({
            "serviceId": "api",
            "deploymentId": "deploy-1",
            "trafficEpoch": 1,
            "activeAssignmentIds": ["assignment-1"],
            "activeNodeIds": ["node-a"],
            "generation": "generation-1",
            "switchedAtMs": 1,
            "drainOldAfterMs": 2
        }),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::InvalidClusterState { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_rejects_noncanonical_blocklist_entries() -> TestResult {
    let mut entries = workload_entries();
    entries.push(LegacyEntry::new(
        "/maetro/cluster/ingress-blocklist/2001:0db8::9",
        b"2001:0db8::9".to_vec(),
    ));
    let snapshot = LegacySnapshot::new(entries)?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

fn workload_entries() -> Vec<LegacyEntry> {
    let mut entries = vec![
        json_entry(
            "/maetro/services/api/info",
            json!({"config": service_config()}),
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
        json_entry(
            "/maetro/cluster/assignments/node-a",
            json!({
                "nodeId": "node-a",
                "generation": 3,
                "assignments": [{
                    "assignmentId": "assignment-1",
                    "placementEpoch": 1,
                    "serviceId": "api",
                    "deploymentId": "deploy-1",
                    "replicaIndex": 0,
                    "nodeId": "node-a",
                    "containerIp": "10.42.1.5",
                    "createdAtMs": 3_000
                }]
            }),
        ),
        json_entry(
            "/maetro/cluster/replica-states/node-a/assignment-1",
            json!({
                "serviceId": "api",
                "deploymentId": "deploy-1",
                "replicaIndex": 0,
                "status": "READY",
                "nodeId": "node-a",
                "assignmentId": "assignment-1",
                "endpoint": {
                    "containerIp": "10.42.1.5",
                    "containerHostname": "api-deploy",
                    "ingressContainerPort": 8080,
                    "gateway": {"hostIp": "10.0.0.5", "port": 443}
                }
            }),
        ),
    ];
    entries.extend(node_entries("node-a", "master", 10, 1));
    entries.extend(cluster_state());
    entries
}

fn service_config() -> serde_json::Value {
    json!({
        "id": "api",
        "name": "API",
        "version": "v1",
        "image": "registry.example/api:latest",
        "deploy": {"exposePorts": [8080]},
        "ingress": {"host": "api.example.test", "port": 8080}
    })
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
