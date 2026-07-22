use kernel_api::{IngressRouting, TrafficGeneration, TrafficGenerationPhase};

/// Projects deterministic logical routes from the generations receiving traffic.
pub fn active_routing(generations: &[TrafficGeneration]) -> Vec<IngressRouting> {
    let mut routing = generations
        .iter()
        .filter(|generation| {
            generation.meta.deletion_timestamp.is_none()
                && generation.status.phase == TrafficGenerationPhase::Active
        })
        .flat_map(|generation| {
            generation.spec.routes.iter().map(|route| {
                let mut servers = generation
                    .spec
                    .targets
                    .iter()
                    .filter(|target| target.endpoint.port() == route.target_port)
                    .map(|target| format!("http://{}", target.endpoint))
                    .collect::<Vec<_>>();
                servers.sort();
                servers.dedup();
                IngressRouting {
                    service_id: generation.spec.service_id.clone(),
                    rule: crate::traefik::route_rule(route),
                    entry_points: vec!["web".to_owned()],
                    servers,
                }
            })
        })
        .collect::<Vec<_>>();
    routing.sort_by(|left, right| {
        left.service_id
            .cmp(&right.service_id)
            .then_with(|| left.rule.cmp(&right.rule))
            .then_with(|| left.servers.cmp(&right.servers))
    });
    routing
}
