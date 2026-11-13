use std::collections::BTreeMap;
use std::net::{IpAddr, SocketAddr};

use maestro_core::{
    Assignment, Backend, Deployment, DeploymentId, DeploymentStatus, DiscoveredReplica,
    DiscoveryTable, PreviewSource, ReplicaId, ReplicaPhase, ReplicaStatus, RouteTable, ServiceId,
    ServiceRoute, ServiceSpec, discovery_key_hash, internal_name, subnet_gateway, version_label,
};
use maestro_store::{DiscoveryKeys, StateMachine, StoreError};

const ROUTED_STATUSES: [DeploymentStatus; 2] =
    [DeploymentStatus::Deploying, DeploymentStatus::Ready];

pub struct RoutingInput<'a> {
    pub services: &'a [(ServiceId, ServiceSpec)],
    pub deployments: &'a [Deployment],
    pub assignments: &'a [Assignment],
    pub observed: &'a dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
}

pub fn cluster_routes(
    state: &StateMachine,
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    previous: &RouteTable,
) -> Result<RouteTable, StoreError> {
    let services = state.services()?;
    let deployments = services
        .iter()
        .map(|(service_id, _)| state.deployments(service_id))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    let assignments = all_assignments(state)?;
    let routes = build_routes(
        &RoutingInput {
            services: &services,
            deployments: &deployments,
            assignments: &assignments,
            observed,
        },
        previous,
    );
    Ok(RouteTable {
        blocked_ips: state.blocked_ips()?,
        ..routes
    })
}

pub fn cluster_dns(
    state: &StateMachine,
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    cluster: &str,
    previous: &BTreeMap<String, Vec<IpAddr>>,
) -> Result<BTreeMap<String, Vec<IpAddr>>, StoreError> {
    let deployments = state
        .services()?
        .iter()
        .map(|(service_id, _)| state.deployments(service_id))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    Ok(build_dns(
        &deployments,
        &all_assignments(state)?,
        observed,
        cluster,
        previous,
    ))
}

pub fn cluster_discovery(
    state: &StateMachine,
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    cluster: &str,
    discovery_keys: &DiscoveryKeys,
    system_ips: &[IpAddr],
) -> Result<DiscoveryTable, StoreError> {
    let services = state.services()?;
    let deployments = services
        .iter()
        .map(|(service_id, _)| state.deployments(service_id))
        .collect::<Result<Vec<_>, _>>()?
        .concat();
    let assignments = all_assignments(state)?;
    let gateways = state
        .nodes()?
        .into_iter()
        .filter_map(|node| subnet_gateway(node.subnet.as_deref()?));
    Ok(DiscoveryTable {
        services: discovered_replicas(&deployments, &assignments, observed, cluster),
        access: network_access(&services, &state.preview_sources()?),
        members: running_members(&assignments, observed),
        always_allowed: gateways.chain(system_ips.iter().copied()).collect(),
        key_hashes: services
            .iter()
            .map(|(service_id, _)| {
                (
                    service_id.clone(),
                    discovery_key_hash(&discovery_keys.key(service_id)),
                )
            })
            .collect(),
    })
}

/// What the discovery API returns for one service, for the admin API to show the same answer.
pub fn service_discovery(
    state: &StateMachine,
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    cluster: &str,
    service_id: &ServiceId,
) -> Result<Vec<DiscoveredReplica>, StoreError> {
    let assignments = all_assignments(state)?
        .into_iter()
        .filter(|assignment| &assignment.service_id == service_id)
        .collect::<Vec<_>>();
    Ok(discovered_replicas(
        &state.deployments(service_id)?,
        &assignments,
        observed,
        cluster,
    )
    .remove(service_id)
    .unwrap_or_default())
}

pub fn build_dns(
    deployments: &[Deployment],
    assignments: &[Assignment],
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    cluster: &str,
    previous: &BTreeMap<String, Vec<IpAddr>>,
) -> BTreeMap<String, Vec<IpAddr>> {
    let serving = serving_deployments(deployments);
    let records = assignments
        .iter()
        .filter(|assignment| !assignment.is_retiring())
        .filter_map(|assignment| {
            let status = observed(&assignment.replica_id)?;
            let ip = status.ip?;
            Some((assignment.clone(), ip, status.phase))
        })
        .flat_map(|(assignment, ip, phase)| {
            let serves = serving.get(&assignment.service_id) == Some(&assignment.deployment_id);
            std::iter::once(internal_name(cluster, assignment.replica_id.as_str()))
                .chain(serves.then(|| internal_name(cluster, assignment.service_id.as_str())))
                .map(move |name| (name, ip, phase))
        })
        .collect::<Vec<_>>();
    let healthy = records
        .iter()
        .filter(|(_, _, phase)| *phase == ReplicaPhase::Healthy)
        .fold(
            BTreeMap::<String, Vec<IpAddr>>::new(),
            |mut names, (name, ip, _)| {
                names.entry(name.clone()).or_default().push(*ip);
                names
            },
        );
    let still_serving = records
        .iter()
        .filter(|(name, ip, phase)| {
            *phase == ReplicaPhase::Unhealthy
                && previous.get(name).is_some_and(|ips| ips.contains(ip))
        })
        .collect::<Vec<_>>();
    still_serving
        .into_iter()
        .fold(healthy, |mut names, (name, ip, _)| {
            names.entry(name.clone()).or_default().push(*ip);
            names
        })
}

pub fn build_routes(input: &RoutingInput, previous: &RouteTable) -> RouteTable {
    let serving = serving_deployments(input.deployments);
    let routes = input
        .services
        .iter()
        .filter_map(|(service_id, spec)| {
            let ingress = spec.ingress.as_ref()?;
            let hosts = ingress.all_hosts().map(str::to_string).collect::<Vec<_>>();
            let routed_replicas = input
                .assignments
                .iter()
                .filter(|assignment| &assignment.service_id == service_id)
                .filter(|assignment| !assignment.is_retiring())
                .filter(|assignment| serving.get(service_id) == Some(&assignment.deployment_id))
                .map(|assignment| (assignment, (input.observed)(&assignment.replica_id)))
                .collect::<Vec<_>>();
            let previous_backends = previous
                .routes
                .get(service_id)
                .map(|route| route.backends.clone())
                .unwrap_or_default();
            let healthy = routed_replicas.iter().filter_map(|(assignment, status)| {
                let status = status.as_ref()?;
                let ip = status
                    .ip
                    .filter(|_| status.phase == ReplicaPhase::Healthy)?;
                Some(Backend {
                    replica_id: assignment.replica_id.clone(),
                    address: SocketAddr::new(ip, ingress.port),
                })
            });
            let still_serving = previous_backends.iter().filter(|backend| {
                routed_replicas.iter().any(|(assignment, status)| {
                    assignment.replica_id == backend.replica_id
                        && status.as_ref().is_some_and(|status| {
                            status.phase == ReplicaPhase::Unhealthy
                                && status.ip == Some(backend.address.ip())
                        })
                })
            });
            let mut backends = healthy.chain(still_serving.cloned()).collect::<Vec<_>>();
            backends.sort();
            backends.dedup();
            let observation_missing = routed_replicas.iter().any(|(_, status)| status.is_none());
            let held = (backends.is_empty() && observation_missing).then_some(previous_backends);
            Some((
                service_id.clone(),
                ServiceRoute {
                    hosts,
                    backends: held.unwrap_or(backends),
                    affinity_header: ingress
                        .session_affinity
                        .as_ref()
                        .map(|affinity| affinity.header.clone()),
                },
            ))
        })
        .collect();
    RouteTable {
        routes,
        blocked_ips: Vec::new(),
    }
}

fn discovered_replicas(
    deployments: &[Deployment],
    assignments: &[Assignment],
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
    cluster: &str,
) -> BTreeMap<ServiceId, Vec<DiscoveredReplica>> {
    let serving = serving_deployments(deployments);
    let mut replicas = assignments
        .iter()
        .filter(|assignment| !assignment.is_retiring())
        .filter_map(|assignment| {
            let deployment = deployments.iter().find(|deployment| {
                deployment.id == assignment.deployment_id
                    && (serving.get(&deployment.service_id) == Some(&deployment.id)
                        || deployment.status == DeploymentStatus::Draining)
            })?;
            let status = observed(&assignment.replica_id)?;
            let replica = DiscoveredReplica {
                replica_id: assignment.replica_id.clone(),
                deployment_id: assignment.deployment_id.clone(),
                version: deployment.version.map(version_label),
                host: internal_name(cluster, assignment.replica_id.as_str()),
                ip: status.ip?,
                port: deployment
                    .spec
                    .ingress
                    .as_ref()
                    .map(|ingress| ingress.port)
                    .or_else(|| deployment.spec.deploy.expose_ports.first().copied()),
                healthy: status.phase == ReplicaPhase::Healthy,
                draining: deployment.status == DeploymentStatus::Draining,
                node_id: assignment.node_id,
            };
            Some((assignment.service_id.clone(), replica))
        })
        .fold(
            BTreeMap::<ServiceId, Vec<DiscoveredReplica>>::new(),
            |mut services, (service_id, replica)| {
                services.entry(service_id).or_default().push(replica);
                services
            },
        );
    replicas
        .values_mut()
        .for_each(|service| service.sort_by(|left, right| left.replica_id.cmp(&right.replica_id)));
    replicas
}

/// The deployment each service's traffic goes to: its newest ready one, or, while it has none
/// (its first deploy), its newest one still rolling out. Traffic only moves to a new deployment
/// once all of its replicas are healthy, so requests never mix versions.
fn serving_deployments(deployments: &[Deployment]) -> BTreeMap<ServiceId, DeploymentId> {
    let rank = |deployment: &Deployment| {
        (
            deployment.status == DeploymentStatus::Ready,
            deployment.created_at_ms,
            deployment.id.clone(),
        )
    };
    deployments
        .iter()
        .filter(|deployment| ROUTED_STATUSES.contains(&deployment.status))
        .fold(
            BTreeMap::<ServiceId, &Deployment>::new(),
            |mut serving, deployment| {
                let newer = serving
                    .get(&deployment.service_id)
                    .is_none_or(|current| rank(deployment) > rank(current));
                if newer {
                    serving.insert(deployment.service_id.clone(), deployment);
                }
                serving
            },
        )
        .into_iter()
        .map(|(service_id, deployment)| (service_id, deployment.id.clone()))
        .collect()
}

/// Who may reach each service: its `network.from`, plus the previews of every listed service.
fn network_access(
    services: &[(ServiceId, ServiceSpec)],
    previews: &[(ServiceId, PreviewSource)],
) -> BTreeMap<ServiceId, Vec<ServiceId>> {
    services
        .iter()
        .map(|(service_id, spec)| {
            let listed = spec
                .network
                .as_ref()
                .map(|network| network.from.clone())
                .unwrap_or_default();
            let listed_previews = previews
                .iter()
                .filter(|(_, source)| listed.contains(&source.base_service_id))
                .map(|(preview_id, _)| preview_id.clone())
                .collect::<Vec<_>>();
            (
                service_id.clone(),
                listed.into_iter().chain(listed_previews).collect(),
            )
        })
        .collect()
}

fn running_members(
    assignments: &[Assignment],
    observed: &dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
) -> BTreeMap<ServiceId, Vec<IpAddr>> {
    assignments
        .iter()
        .filter_map(|assignment| {
            let ip = observed(&assignment.replica_id)?.ip?;
            Some((assignment.service_id.clone(), ip))
        })
        .fold(
            BTreeMap::<ServiceId, Vec<IpAddr>>::new(),
            |mut members, (service_id, ip)| {
                members.entry(service_id).or_default().push(ip);
                members
            },
        )
}

fn all_assignments(state: &StateMachine) -> Result<Vec<Assignment>, StoreError> {
    Ok(state
        .nodes()?
        .iter()
        .map(|node| state.assignments(node.id))
        .collect::<Result<Vec<_>, _>>()?
        .concat())
}

#[cfg(test)]
mod tests {
    use std::net::IpAddr;

    use maestro_core::{
        ByteSize, DeploySpec, DeploymentId, EffectiveResources, IngressSpec, Millicores, NodeId,
        ResolvedRequest,
    };

    use super::*;

    fn spec() -> ServiceSpec {
        ServiceSpec {
            name: "Web".to_string(),
            image: Some("nginx".to_string()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: Some(IngressSpec {
                host: Some("web.example.com".to_string()),
                hosts: vec!["www.example.com".to_string()],
                port: 8080,
                session_affinity: None,
            }),
            preview: None,
            network: None,
        }
    }

    fn deployment(id: &str, status: DeploymentStatus) -> Deployment {
        Deployment {
            id: DeploymentId::from(id),
            service_id: ServiceId::from("web"),
            spec: spec(),
            status,
            image: None,
            failure: None,
            created_at_ms: 0,
            updated_at_ms: 0,
            git_commit: None,
            built_on: None,
            version: None,
            restart_of: None,
        }
    }

    fn assignment(deployment: &str, index: u32) -> Assignment {
        let deployment_id = DeploymentId::from(deployment);
        Assignment {
            replica_id: ReplicaId::for_deployment(&deployment_id, index),
            service_id: ServiceId::from("web"),
            deployment_id,
            node_id: NodeId(1),
            index,
            image: "nginx".to_string(),
            resources: EffectiveResources {
                cpu_limit: Millicores::new(1000),
                memory_limit: ByteSize::new(1 << 30),
                request: ResolvedRequest {
                    cpu: Millicores::new(250),
                    memory: ByteSize::new(512 << 20),
                },
            },
            retiring_since_ms: None,
        }
    }

    fn status(replica: &str, phase: ReplicaPhase, last_octet: u8) -> ReplicaStatus {
        ReplicaStatus {
            replica_id: ReplicaId::from(replica),
            service_id: ServiceId::from("web"),
            deployment_id: DeploymentId::from("unused"),
            phase,
            ip: Some(IpAddr::from([10, 200, 1, last_octet])),
            restarts: 0,
            consecutive_failures: 0,
            last_error: None,
        }
    }

    fn table(
        deployments: &[Deployment],
        assignments: &[Assignment],
        statuses: &[ReplicaStatus],
        previous: &RouteTable,
    ) -> RouteTable {
        let services = [(ServiceId::from("web"), spec())];
        let observed = |replica_id: &ReplicaId| {
            statuses
                .iter()
                .find(|status| &status.replica_id == replica_id)
                .cloned()
        };
        build_routes(
            &RoutingInput {
                services: &services,
                deployments,
                assignments,
                observed: &observed,
            },
            previous,
        )
    }

    fn addresses(table: &RouteTable) -> Vec<String> {
        table.routes[&ServiceId::from("web")]
            .backends
            .iter()
            .map(|backend| backend.address.to_string())
            .collect()
    }

    #[test]
    fn routes_healthy_replicas_of_active_deployments() {
        let routes = table(
            &[
                deployment("d1", DeploymentStatus::Draining),
                deployment("d2", DeploymentStatus::Ready),
            ],
            &[
                assignment("d1", 0),
                assignment("d2", 0),
                assignment("d2", 1),
            ],
            &[
                status("d1-0", ReplicaPhase::Healthy, 2),
                status("d2-0", ReplicaPhase::Healthy, 3),
                status("d2-1", ReplicaPhase::Unhealthy, 4),
            ],
            &RouteTable::default(),
        );
        let route = &routes.routes[&ServiceId::from("web")];
        assert_eq!(route.hosts, ["web.example.com", "www.example.com"]);
        assert_eq!(addresses(&routes), ["10.200.1.3:8080"]);
    }

    #[test]
    fn switches_traffic_to_a_new_deployment_only_once_it_is_ready() {
        let old = Deployment {
            created_at_ms: 1,
            ..deployment("d1", DeploymentStatus::Ready)
        };
        let rolling_out = Deployment {
            created_at_ms: 2,
            ..deployment("d2", DeploymentStatus::Deploying)
        };
        let assignments = [assignment("d1", 0), assignment("d2", 0)];
        let statuses = [
            status("d1-0", ReplicaPhase::Healthy, 2),
            status("d2-0", ReplicaPhase::Healthy, 3),
        ];
        let during = table(
            &[old.clone(), rolling_out.clone()],
            &assignments,
            &statuses,
            &RouteTable::default(),
        );
        assert_eq!(addresses(&during), ["10.200.1.2:8080"]);

        let ready = Deployment {
            status: DeploymentStatus::Ready,
            ..rolling_out.clone()
        };
        let switched = table(
            &[old.clone(), ready.clone()],
            &assignments,
            &statuses,
            &during,
        );
        assert_eq!(addresses(&switched), ["10.200.1.3:8080"]);

        let observed = |replica_id: &ReplicaId| {
            statuses
                .iter()
                .find(|status| &status.replica_id == replica_id)
                .cloned()
        };
        let names = build_dns(
            &[old.clone(), rolling_out],
            &assignments,
            &observed,
            "prod",
            &BTreeMap::new(),
        );
        assert_eq!(service_ips(&names), ["10.200.1.2"]);
        assert!(names.contains_key("d2-0.prod.maestro"));

        let draining = Deployment {
            status: DeploymentStatus::Draining,
            ..old
        };
        let replicas = discovered_replicas(&[draining, ready], &assignments, &observed, "prod");
        let web = &replicas[&ServiceId::from("web")];
        assert_eq!(
            web.iter()
                .map(|replica| (replica.replica_id.to_string(), replica.draining))
                .collect::<Vec<_>>(),
            [("d1-0".to_string(), true), ("d2-0".to_string(), false)]
        );
    }

    #[test]
    fn retiring_replicas_leave_the_routes_while_still_running() {
        let retiring = Assignment {
            retiring_since_ms: Some(1),
            ..assignment("d1", 1)
        };
        let routes = table(
            &[deployment("d1", DeploymentStatus::Ready)],
            &[assignment("d1", 0), retiring],
            &[
                status("d1-0", ReplicaPhase::Healthy, 2),
                status("d1-1", ReplicaPhase::Healthy, 3),
            ],
            &RouteTable::default(),
        );
        assert_eq!(addresses(&routes), ["10.200.1.2:8080"]);
    }

    #[test]
    fn holds_previous_backends_when_status_is_unknown() {
        let deployments = [deployment("d1", DeploymentStatus::Ready)];
        let assignments = [assignment("d1", 0)];
        let first = table(
            &deployments,
            &assignments,
            &[status("d1-0", ReplicaPhase::Healthy, 2)],
            &RouteTable::default(),
        );
        let after_restart = table(&deployments, &assignments, &[], &first);
        assert_eq!(addresses(&after_restart), ["10.200.1.2:8080"]);
    }

    #[test]
    fn keeps_serving_unhealthy_replicas_until_they_are_retired() {
        let deployments = [deployment("d1", DeploymentStatus::Ready)];
        let first = table(
            &deployments,
            &[assignment("d1", 0)],
            &[status("d1-0", ReplicaPhase::Healthy, 2)],
            &RouteTable::default(),
        );
        let unhealthy = table(
            &deployments,
            &[assignment("d1", 0), assignment("d1", 1)],
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Running, 3),
            ],
            &first,
        );
        assert_eq!(addresses(&unhealthy), ["10.200.1.2:8080"]);
        let still_unhealthy = table(
            &deployments,
            &[assignment("d1", 0), assignment("d1", 1)],
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Running, 3),
            ],
            &unhealthy,
        );
        assert_eq!(addresses(&still_unhealthy), ["10.200.1.2:8080"]);
        let replaced = table(
            &deployments,
            &[assignment("d1", 0), assignment("d1", 1)],
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Healthy, 3),
            ],
            &still_unhealthy,
        );
        assert_eq!(addresses(&replaced), ["10.200.1.2:8080", "10.200.1.3:8080"]);
        let retired = table(
            &deployments,
            &[
                Assignment {
                    retiring_since_ms: Some(1),
                    ..assignment("d1", 0)
                },
                assignment("d1", 1),
            ],
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Healthy, 3),
            ],
            &replaced,
        );
        assert_eq!(addresses(&retired), ["10.200.1.3:8080"]);
    }

    #[test]
    fn never_routes_replicas_that_were_never_healthy() {
        let routes = table(
            &[deployment("d1", DeploymentStatus::Ready)],
            &[assignment("d1", 0)],
            &[status("d1-0", ReplicaPhase::Unhealthy, 2)],
            &RouteTable::default(),
        );
        assert!(addresses(&routes).is_empty());
    }

    #[test]
    fn drops_previously_healthy_replicas_once_they_stop_running() {
        let deployments = [deployment("d1", DeploymentStatus::Ready)];
        let assignments = [assignment("d1", 0)];
        let first = table(
            &deployments,
            &assignments,
            &[status("d1-0", ReplicaPhase::Healthy, 2)],
            &RouteTable::default(),
        );
        let restarting = table(
            &deployments,
            &assignments,
            &[ReplicaStatus {
                ip: None,
                ..status("d1-0", ReplicaPhase::Restarting, 2)
            }],
            &first,
        );
        assert!(addresses(&restarting).is_empty());
    }

    #[test]
    fn lets_listed_services_and_their_previews_reach_a_service() {
        let services = [
            (
                ServiceId::from("api"),
                ServiceSpec {
                    network: Some(maestro_core::NetworkSpec {
                        from: vec![ServiceId::from("web")],
                    }),
                    ..spec()
                },
            ),
            (ServiceId::from("web"), spec()),
        ];
        let previews = [(
            ServiceId::from("web-pr-12"),
            serde_json::from_value::<PreviewSource>(serde_json::json!({
                "baseServiceId": "web",
                "prNumber": 12,
                "headRef": "feature",
                "headSha": "abc",
                "title": "Feature",
                "createdAtMs": 1
            }))
            .unwrap(),
        )];
        let access = network_access(&services, &previews);
        assert_eq!(
            access[&ServiceId::from("api")],
            [ServiceId::from("web"), ServiceId::from("web-pr-12")]
        );
        assert!(access[&ServiceId::from("web")].is_empty());

        let retiring = Assignment {
            retiring_since_ms: Some(1),
            ..assignment("d1", 1)
        };
        let statuses = [
            status("d1-0", ReplicaPhase::Starting, 2),
            status("d1-1", ReplicaPhase::Healthy, 3),
        ];
        let observed = |replica_id: &ReplicaId| {
            statuses
                .iter()
                .find(|status| &status.replica_id == replica_id)
                .cloned()
        };
        let members = running_members(&[assignment("d1", 0), retiring], &observed);
        assert_eq!(
            members[&ServiceId::from("web")],
            [
                "10.200.1.2".parse::<IpAddr>().unwrap(),
                "10.200.1.3".parse().unwrap()
            ]
        );
    }

    #[test]
    fn discovers_the_live_replicas_of_each_service() {
        let deployments = [
            Deployment {
                version: Some(3),
                ..deployment("d1", DeploymentStatus::Ready)
            },
            deployment("d0", DeploymentStatus::Stopped),
        ];
        let retiring = Assignment {
            retiring_since_ms: Some(1),
            ..assignment("d1", 1)
        };
        let assignments = [assignment("d1", 0), retiring, assignment("d0", 0)];
        let statuses = [
            status("d1-0", ReplicaPhase::Running, 2),
            status("d1-1", ReplicaPhase::Healthy, 3),
            status("d0-0", ReplicaPhase::Healthy, 4),
        ];
        let observed = |replica_id: &ReplicaId| {
            statuses
                .iter()
                .find(|status| &status.replica_id == replica_id)
                .cloned()
        };
        let replicas = discovered_replicas(&deployments, &assignments, &observed, "Prod");
        let web = &replicas[&ServiceId::from("web")];
        assert_eq!(web.len(), 1);
        assert_eq!(web[0].replica_id, ReplicaId::from("d1-0"));
        assert_eq!(web[0].version.as_deref(), Some("00000003"));
        assert_eq!(web[0].host, "d1-0.prod.maestro");
        assert_eq!(web[0].ip.to_string(), "10.200.1.2");
        assert_eq!(web[0].port, Some(8080));
        assert!(!web[0].healthy);
    }

    fn dns(
        assignments: &[Assignment],
        statuses: &[ReplicaStatus],
        previous: &BTreeMap<String, Vec<IpAddr>>,
    ) -> BTreeMap<String, Vec<IpAddr>> {
        let observed = |replica_id: &ReplicaId| {
            statuses
                .iter()
                .find(|status| &status.replica_id == replica_id)
                .cloned()
        };
        build_dns(
            &[deployment("d1", DeploymentStatus::Ready)],
            assignments,
            &observed,
            "prod",
            previous,
        )
    }

    fn service_ips(names: &BTreeMap<String, Vec<IpAddr>>) -> Vec<String> {
        names
            .get(&internal_name("prod", "web"))
            .map(|ips| ips.iter().map(IpAddr::to_string).collect())
            .unwrap_or_default()
    }

    #[test]
    fn dns_keeps_unhealthy_replicas_until_they_are_retired() {
        let assignments = [assignment("d1", 0), assignment("d1", 1)];
        let first = dns(
            &assignments[..1],
            &[status("d1-0", ReplicaPhase::Healthy, 2)],
            &BTreeMap::new(),
        );
        let unhealthy = dns(
            &assignments,
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Unhealthy, 3),
            ],
            &first,
        );
        assert_eq!(service_ips(&unhealthy), ["10.200.1.2"]);
        let replaced = dns(
            &assignments,
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Healthy, 3),
            ],
            &unhealthy,
        );
        assert_eq!(service_ips(&replaced), ["10.200.1.3", "10.200.1.2"]);
        let retired = dns(
            &[
                Assignment {
                    retiring_since_ms: Some(1),
                    ..assignment("d1", 0)
                },
                assignment("d1", 1),
            ],
            &[
                status("d1-0", ReplicaPhase::Unhealthy, 2),
                status("d1-1", ReplicaPhase::Healthy, 3),
            ],
            &replaced,
        );
        assert_eq!(service_ips(&retired), ["10.200.1.3"]);
    }

    #[test]
    fn dns_never_resolves_replicas_that_were_never_healthy() {
        let names = dns(
            &[assignment("d1", 0)],
            &[status("d1-0", ReplicaPhase::Unhealthy, 2)],
            &BTreeMap::new(),
        );
        assert!(names.is_empty());
    }
}
