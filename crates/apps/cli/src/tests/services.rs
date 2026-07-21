use kernel_api::Service;
use serde_json::json;

use crate::services::write_services;

#[test]
fn service_listing_is_stable_and_never_prints_secrets() -> Result<(), Box<dyn std::error::Error>> {
    let services = vec![
        service("worker", "Worker", "2", 3, None)?,
        service("api", "API", "1", 2, Some(1))?,
    ];
    let mut output = Vec::new();
    write_services(services, &mut output)?;
    let output = String::from_utf8(output)?;
    assert!(output.contains("ID"));
    assert!(output.contains("api"));
    assert!(output.contains("1*"));
    assert!(output.contains("registry.example.test/api:1"));
    assert!(
        output
            .find("api")
            .is_some_and(|api| { output.find("worker").is_some_and(|worker| api < worker) })
    );
    assert!(!output.contains("database-password"));
    Ok(())
}

#[test]
fn empty_service_listing_is_explicit() -> Result<(), Box<dyn std::error::Error>> {
    let mut output = Vec::new();
    write_services(Vec::new(), &mut output)?;
    assert_eq!(String::from_utf8(output)?, "[maestro]: no services found\n");
    Ok(())
}

fn service(
    id: &str,
    name: &str,
    version: &str,
    replicas: u32,
    replica_override: Option<u32>,
) -> Result<Service, serde_json::Error> {
    serde_json::from_value(json!({
        "meta": {
            "id": id,
            "revision": 1,
            "generation": 1
        },
        "spec": {
            "name": name,
            "version": version,
            "artifact": {
                "type": "image",
                "reference": format!("registry.example.test/{id}:{version}")
            },
            "replicas": replicas,
            "environment": {},
            "secrets": {
                "mountPath": "/run/secrets/service.env",
                "items": {"DATABASE_PASSWORD": "database-password"}
            },
            "exec": "allowed"
        },
        "status": {
            "replicaOverride": replica_override,
            "rollout": "active"
        }
    }))
}
