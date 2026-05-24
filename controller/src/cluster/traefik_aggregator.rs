//! Builds a Traefik HTTP-routing config from cluster-wide assignments + service
//! ingress rules. Pure logic — the output is the canonical config that the
//! leader writes to etcd under `traefik/`, where every node's Traefik already
//! reads from (see `INGRESS_IMAGE_TAG` startup args in deployment module).
//!
//! Each service gets:
//!   * a Traefik router per ingress host
//!   * a load-balancer service with one server per replica's `{node-tailscale-ip}:{port}`

use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use super::scheduling::Assignment;
use super::types::NodeInfo;
use crate::deployment::types::{IngressConfig, ServiceConfig};

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikRouter {
    pub rule: String,
    pub entry_points: Vec<String>,
    pub service: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikServer {
    pub url: String,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikLoadBalancer {
    pub servers: Vec<TraefikServer>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikService {
    pub load_balancer: TraefikLoadBalancer,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikHttpConfig {
    pub routers: BTreeMap<String, TraefikRouter>,
    pub services: BTreeMap<String, TraefikService>,
}

#[derive(Debug, Clone, Default, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "camelCase")]
pub struct TraefikDynamicConfig {
    pub http: TraefikHttpConfig,
}

pub struct AggregatorInput<'a> {
    pub services: &'a [ServiceConfig],
    pub assignments: &'a [Assignment],
    pub nodes: &'a [NodeInfo],
    pub default_entry_point: &'a str,
}

pub fn build_traefik_config(input: AggregatorInput<'_>) -> TraefikDynamicConfig {
    let mut config = TraefikDynamicConfig::default();
    let node_address: BTreeMap<String, String> = input
        .nodes
        .iter()
        .map(|node| {
            (
                node.node_id.clone(),
                node.tailscale_ip
                    .clone()
                    .unwrap_or_else(|| node.hostname.clone()),
            )
        })
        .collect();

    let mut assignments_by_service: BTreeMap<String, Vec<&Assignment>> = BTreeMap::new();
    for assignment in input.assignments {
        assignments_by_service
            .entry(assignment.service_id.clone())
            .or_default()
            .push(assignment);
    }

    for service in input.services {
        let Some(ingress) = service.ingress.as_ref() else {
            continue;
        };
        let hosts = collect_hosts(ingress);
        if hosts.is_empty() {
            continue;
        }
        let assignments = match assignments_by_service.get(&service.id) {
            Some(list) => list,
            None => continue,
        };
        if assignments.is_empty() {
            continue;
        }
        let servers: Vec<TraefikServer> = assignments
            .iter()
            .filter_map(|assignment| {
                let address = node_address.get(&assignment.node_id)?;
                Some(TraefikServer {
                    url: format!("http://{address}:{}", assignment.port),
                })
            })
            .collect();
        if servers.is_empty() {
            continue;
        }
        let service_key = service.id.clone();
        config.http.services.insert(
            service_key.clone(),
            TraefikService {
                load_balancer: TraefikLoadBalancer { servers },
            },
        );
        for host in hosts {
            let router_key = format!("{}-{}", service.id, sanitize(&host));
            config.http.routers.insert(
                router_key,
                TraefikRouter {
                    rule: format!("Host(`{host}`)"),
                    entry_points: vec![input.default_entry_point.to_string()],
                    service: service_key.clone(),
                },
            );
        }
    }

    config
}

fn collect_hosts(ingress: &IngressConfig) -> Vec<String> {
    let mut hosts: Vec<String> = ingress
        .hosts()
        .into_iter()
        .map(|host| host.to_string())
        .collect();
    hosts.sort();
    hosts.dedup();
    hosts
}

fn sanitize(value: &str) -> String {
    value
        .chars()
        .map(|ch| if ch.is_ascii_alphanumeric() { ch } else { '-' })
        .collect()
}
