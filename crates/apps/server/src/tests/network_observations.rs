use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::http::StatusCode;
use kernel_api::{
    AssignmentId, DnsRecord, DnsRecordId, DnsRecordSpec, DnsRecordStatus, DnsRecordValue,
    FirewallDirection, FirewallPolicy, FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus,
    FirewallSubject, FirewallVerdict, Generation, IngressRoute, IngressRouteId, IngressRouteSpec,
    IngressRouteStatus, IngressRouting, NodeFirewall, NodeFirewallId, NodeFirewallSpec,
    NodeFirewallStatus, NodeId, NodeNetwork, NodeNetworkId, NodeNetworkSpec, NodeNetworkStatus,
    Object, ServiceId, TrafficGeneration, TrafficGenerationId, TrafficGenerationPhase,
    TrafficGenerationSpec, TrafficGenerationStatus, TrafficRoute, TrafficTarget,
};

use crate::{ApiServer, ServerSettings};

use super::{decode, metadata, put, request, seeded_store};

#[tokio::test]
async fn network_control_plane_observations_are_typed_revisioned_and_scoped()
-> Result<(), Box<dyn std::error::Error>> {
    let (store, cluster_id) = seeded_store().await?;
    put(&store, &cluster_id, "NodeNetwork", "network-1", &network()?).await?;
    put(
        &store,
        &cluster_id,
        "NodeFirewall",
        "firewall-1",
        &node_firewall()?,
    )
    .await?;
    put(&store, &cluster_id, "DnsRecord", "api-dns", &dns_record()?).await?;
    put(
        &store,
        &cluster_id,
        "FirewallPolicy",
        "api-egress",
        &firewall_policy()?,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "IngressRoute",
        "api-route",
        &ingress_route()?,
    )
    .await?;
    put(
        &store,
        &cluster_id,
        "TrafficGeneration",
        "api-traffic-1",
        &traffic_generation()?,
    )
    .await?;
    let server = ApiServer::new(
        store,
        cluster_id,
        ServerSettings::new("127.0.0.1:3000".parse()?, None),
    )?;

    assert_revisioned_list::<NodeNetwork>(&server, "/api/cluster/networks").await?;
    assert_revisioned_list::<NodeFirewall>(&server, "/api/cluster/node-firewalls").await?;
    assert_revisioned_list::<DnsRecord>(&server, "/api/cluster/dns-records").await?;
    assert_revisioned_list::<FirewallPolicy>(&server, "/api/firewall/policies").await?;
    assert_revisioned_list::<IngressRoute>(&server, "/api/services/api/routes").await?;
    assert_revisioned_list::<TrafficGeneration>(&server, "/api/services/api/traffic-generations")
        .await?;
    let routing: Vec<IngressRouting> =
        decode(request(&server, "/api/ingress/routes", None).await?).await?;
    assert_eq!(routing.len(), 1);
    let active_route = routing.first().ok_or("active ingress route missing")?;
    assert_eq!(active_route.service_id, ServiceId::new("api")?);
    assert_eq!(active_route.rule, "Host(`api.example.test`)");
    assert_eq!(active_route.entry_points, ["web"]);
    assert_eq!(active_route.servers, ["http://10.42.1.10:8080"]);

    assert_eq!(
        request(&server, "/api/services/missing/routes/api-route", None)
            .await?
            .status(),
        StatusCode::NOT_FOUND
    );
    let firewall: NodeFirewall =
        decode(request(&server, "/api/cluster/node-firewalls/firewall-1", None).await?).await?;
    assert_eq!(firewall.spec.digest, "rules-digest");
    Ok(())
}

async fn assert_revisioned_list<Resource>(
    server: &ApiServer,
    path: &str,
) -> Result<(), Box<dyn std::error::Error>>
where
    Resource: serde::de::DeserializeOwned + Revisioned,
{
    let resources: Vec<Resource> = decode(request(server, path, None).await?).await?;
    assert_eq!(resources.len(), 1);
    assert!(
        resources
            .first()
            .is_some_and(|resource| resource.revision() > 0)
    );
    Ok(())
}

trait Revisioned {
    fn revision(&self) -> u64;
}

macro_rules! revisioned {
    ($($resource:ty),+ $(,)?) => {
        $(
            impl Revisioned for $resource {
                fn revision(&self) -> u64 {
                    self.meta.revision.0
                }
            }
        )+
    };
}

revisioned!(
    NodeNetwork,
    NodeFirewall,
    DnsRecord,
    FirewallPolicy,
    IngressRoute,
    TrafficGeneration,
);

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

fn node_firewall() -> Result<NodeFirewall, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(NodeFirewallId::new("firewall-1")?),
        spec: NodeFirewallSpec {
            node_id: NodeId::new("node-1")?,
            table_name: "maestro".to_string(),
            script: "table inet maestro {}".to_string(),
            digest: "rules-digest".to_string(),
        },
        status: NodeFirewallStatus {
            applied_generation: Generation(1),
            applied_digest: Some("rules-digest".to_string()),
            conditions: Vec::new(),
        },
    })
}

fn dns_record() -> Result<DnsRecord, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DnsRecordId::new("api-dns")?),
        spec: DnsRecordSpec {
            name: "api.maestro.internal.".to_string(),
            values: vec![DnsRecordValue::A(Ipv4Addr::new(10, 42, 1, 4))],
            ttl_secs: 30,
        },
        status: DnsRecordStatus {
            applied_generation: Generation(1),
            published_nodes: vec![NodeId::new("node-1")?],
            conditions: Vec::new(),
        },
    })
}

fn firewall_policy() -> Result<FirewallPolicy, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(FirewallPolicyId::new("api-egress")?),
        spec: FirewallPolicySpec {
            direction: FirewallDirection::Egress,
            subject: FirewallSubject::Service(ServiceId::new("api")?),
            rules: Vec::new(),
            default_verdict: FirewallVerdict::Allow,
        },
        status: FirewallPolicyStatus {
            applied_generation: Generation(1),
            ruleset_digest: Some("rules-digest".to_string()),
            conditions: Vec::new(),
        },
    })
}

fn ingress_route() -> Result<IngressRoute, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(IngressRouteId::new("api-route")?),
        spec: IngressRouteSpec {
            service_id: ServiceId::new("api")?,
            hosts: vec!["api.example.test".to_string()],
            path_prefix: None,
            target_port: 8_080,
            session_affinity: None,
        },
        status: IngressRouteStatus {
            applied_generation: Generation(1),
            conditions: Vec::new(),
        },
    })
}

fn traffic_generation() -> Result<TrafficGeneration, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(TrafficGenerationId::new("api-traffic-1")?),
        spec: TrafficGenerationSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: kernel_api::DeploymentId::new("api-deployment-1")?,
            epoch: 1,
            routes: vec![TrafficRoute {
                route_id: IngressRouteId::new("api-route")?,
                route_generation: Generation(1),
                hosts: vec!["api.example.test".to_owned()],
                path_prefix: None,
                target_port: 8_080,
                session_affinity: None,
            }],
            targets: vec![TrafficTarget {
                assignment_id: AssignmentId::new("api-assignment-1")?,
                node_id: NodeId::new("node-1")?,
                endpoint: SocketAddr::new(IpAddr::V4(Ipv4Addr::new(10, 42, 1, 10)), 8_080),
            }],
        },
        status: TrafficGenerationStatus {
            phase: TrafficGenerationPhase::Active,
            staged_at: kernel_api::Timestamp(1_000),
            activated_at: Some(kernel_api::Timestamp(1_100)),
            retired_at: None,
            conditions: Vec::new(),
        },
    })
}
