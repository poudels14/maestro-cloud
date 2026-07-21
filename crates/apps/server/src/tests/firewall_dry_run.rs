use std::collections::BTreeSet;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::body::Body;
use axum::http::{Method, Request, StatusCode, header};
use firewall::FirewallSettings;
use kernel_api::{
    FirewallPolicy, Generation, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec,
    NodeNetworkStatus, Object,
};
use serde_json::{Value, json};
use tower::ServiceExt;

use crate::{ApiServer, ServerSettings};

use super::{decode, metadata, put, request, seeded_store};

#[tokio::test]
async fn firewall_dry_run_uses_the_real_planner_without_persisting_the_proposal()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    put(&store, &cluster_id, "NodeNetwork", "network-1", &network()?).await?;
    let unconfigured = ApiServer::new(
        store.clone(),
        cluster_id.clone(),
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;
    assert_eq!(
        dry_run(&unconfigured).await?.status(),
        StatusCode::SERVICE_UNAVAILABLE
    );

    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?
    .with_firewall_settings(settings());
    let response = dry_run(&server).await?;
    assert_eq!(response.status(), StatusCode::OK);
    let response = decode::<Value>(response).await?;
    assert!(
        response
            .get("bundleDigest")
            .and_then(Value::as_str)
            .is_some_and(|digest| !digest.is_empty())
    );
    let ruleset = response
        .get("rulesets")
        .and_then(Value::as_array)
        .and_then(|rulesets| rulesets.first())
        .ok_or("dry-run returned no node ruleset")?;
    assert_eq!(
        ruleset.get("nodeId").and_then(Value::as_str),
        Some("node-1")
    );
    assert!(
        ruleset
            .get("script")
            .and_then(Value::as_str)
            .is_some_and(|script| script.contains("10.0.0.0/8"))
    );
    let policies: Vec<FirewallPolicy> =
        decode(request(&server, "/api/firewall/policies", None).await?).await?;
    assert!(policies.is_empty());
    Ok(())
}

async fn dry_run(
    server: &ApiServer,
) -> Result<axum::response::Response, Box<dyn std::error::Error>> {
    Ok(server
        .router()
        .oneshot(
            Request::builder()
                .method(Method::POST)
                .uri("/api/firewall/policies/api-egress/dry-run")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(serde_json::to_vec(&json!({
                    "spec": {
                        "direction": "egress",
                        "subject": {"type": "global"},
                        "rules": [{
                            "cidr": "10.0.0.0/8",
                            "protocol": "tcp",
                            "ports": [{"start": 443, "end": 443}],
                            "verdict": "allow"
                        }],
                        "defaultVerdict": "deny"
                    }
                }))?))?,
        )
        .await?)
}

fn settings() -> FirewallSettings {
    FirewallSettings {
        table_name: "maestro_firewall".to_string(),
        workload_interface: "maestro0".to_string(),
        dns_port: 53,
        protected_host_ports: vec![443],
        control_allow_cidrs: vec!["10.20.0.0/24".to_string()],
        system_services: BTreeSet::new(),
    }
}

fn network() -> Result<NodeNetwork, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeNetworkId::new("network-1")?),
        spec: NodeNetworkSpec {
            node_id: NodeId::new("node-1")?,
            public_key: "node-public-key".to_string(),
            endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 20, 0, 1)), 51_820),
            workload_subnet: "10.42.1.0/24".to_string(),
            mtu_bytes: 1_420,
        },
        status: NodeNetworkStatus {
            applied_generation: Generation(1),
            conditions: Vec::new(),
        },
    })
}
