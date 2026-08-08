use kernel_api::{Deployment, ServiceId};
use serde_json::json;

use crate::deployments::write_history;

#[test]
fn deployment_history_is_newest_first_complete_and_secret_safe()
-> Result<(), Box<dyn std::error::Error>> {
    let phases = [
        ("queued", "QUEUED", 1),
        ("building", "BUILDING", 2),
        ("publishing", "PUBLISHING", 3),
        ("starting", "STARTING", 4),
        ("pending", "PENDING_READY", 5),
        ("retrying", "RETRYING", 6),
        ("ready", "READY", 7),
        ("recovering", "RECOVERING", 8),
        ("stopping", "STOPPING", 9),
        ("stopped", "STOPPED", 10),
        ("crashed", "CRASHED", 11),
        ("removed", "REMOVED", 12),
        ("draining", "DRAINING", 13),
        ("canceled", "CANCELED", 14),
    ];
    let deployments = phases
        .iter()
        .map(|(id, phase, created_at)| deployment(id, phase, *created_at))
        .collect::<Result<Vec<_>, _>>()?;
    let mut output = Vec::new();
    write_history(&ServiceId::new("api")?, deployments, &mut output)?;
    let output = String::from_utf8(output)?;
    for expected in [
        "queued",
        "building",
        "publishing",
        "starting",
        "pending-ready",
        "retrying",
        "ready",
        "recovering",
        "stopping",
        "stopped",
        "crashed",
        "removed",
        "draining",
        "canceled",
    ] {
        assert!(output.contains(expected));
    }
    assert!(
        output
            .find("canceled")
            .is_some_and(|newest| output.find("queued").is_some_and(|oldest| newest < oldest))
    );
    assert!(output.contains("registry.test/api@sha256:public"));
    assert!(!output.contains("private-deployment-secret"));
    Ok(())
}

#[test]
fn empty_deployment_history_is_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = Vec::new();
    write_history(&ServiceId::new("api")?, Vec::new(), &mut output)?;
    assert_eq!(
        String::from_utf8(output)?,
        "[maestro]: no deployments found for service `api`\n"
    );
    Ok(())
}

fn deployment(id: &str, phase: &str, created_at: u64) -> Result<Deployment, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {
            "id": format!("deployment-{id}"),
            "revision": created_at,
            "generation": 1
        },
        "spec": {
            "serviceId": "api",
            "serviceGeneration": created_at,
            "restartGeneration": 1,
            "service": {
                "name": "API",
                "version": format!("1.0.{created_at}"),
                "artifact": {"type": "image", "reference": "registry.test/api:latest"},
                "replicas": 1,
                "environment": {},
                "secrets": {
                    "format": "dotenv",
                    "mountPath": "/run/secrets/service.env",
                    "items": {"TOKEN": "private-deployment-secret"}
                },
                "exec": "denied"
            },
            "goal": "run"
        },
        "status": {
            "phase": phase,
            "createdAt": created_at,
            "readyAt": if phase == "READY" { Some(created_at) } else { None },
            "imageDigest": "registry.test/api@sha256:public"
        }
    }))
}
