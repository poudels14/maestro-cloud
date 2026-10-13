use std::net::SocketAddr;

use maestro_core::{
    Assignment, Backend, Deployment, DeploymentStatus, ReplicaId, ReplicaPhase, ReplicaStatus,
    RouteTable, ServiceId, ServiceRoute, ServiceSpec,
};

const ROUTED_STATUSES: [DeploymentStatus; 2] =
    [DeploymentStatus::Deploying, DeploymentStatus::Ready];

pub struct RoutingInput<'a> {
    pub services: &'a [(ServiceId, ServiceSpec)],
    pub deployments: &'a [Deployment],
    pub assignments: &'a [Assignment],
    pub observed: &'a dyn Fn(&ReplicaId) -> Option<ReplicaStatus>,
}

pub fn build_routes(input: &RoutingInput, previous: &RouteTable) -> RouteTable {
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
                .filter(|assignment| {
                    input.deployments.iter().any(|deployment| {
                        deployment.id == assignment.deployment_id
                            && ROUTED_STATUSES.contains(&deployment.status)
                    })
                })
                .map(|assignment| (assignment, (input.observed)(&assignment.replica_id)))
                .collect::<Vec<_>>();
            let mut backends = routed_replicas
                .iter()
                .filter_map(|(assignment, status)| {
                    let status = status.as_ref()?;
                    let ip = status
                        .ip
                        .filter(|_| status.phase == ReplicaPhase::Healthy)?;
                    Some(Backend {
                        replica_id: assignment.replica_id.clone(),
                        address: SocketAddr::new(ip, ingress.port),
                    })
                })
                .collect::<Vec<_>>();
            backends.sort();
            let observation_missing = routed_replicas.iter().any(|(_, status)| status.is_none());
            let held = previous
                .routes
                .get(service_id)
                .filter(|_| backends.is_empty() && observation_missing)
                .map(|route| route.backends.clone());
            Some((
                service_id.clone(),
                ServiceRoute {
                    hosts,
                    backends: held.unwrap_or(backends),
                },
            ))
        })
        .collect();
    RouteTable { routes }
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
            }),
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
    fn drops_backends_when_replicas_are_confirmed_unhealthy() {
        let deployments = [deployment("d1", DeploymentStatus::Ready)];
        let assignments = [assignment("d1", 0)];
        let first = table(
            &deployments,
            &assignments,
            &[status("d1-0", ReplicaPhase::Healthy, 2)],
            &RouteTable::default(),
        );
        let unhealthy = table(
            &deployments,
            &assignments,
            &[status("d1-0", ReplicaPhase::Unhealthy, 2)],
            &first,
        );
        assert!(addresses(&unhealthy).is_empty());
    }
}
