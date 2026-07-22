use kernel_api::TrafficGenerationPhase;

use super::plan::World;
use crate::{active_routing, plan};

#[test]
fn active_routing_projects_exact_rules_and_port_scoped_servers() {
    let mut world = World::ready();
    world.routes[0].spec.hosts = vec!["api.example.test".to_owned()];
    world.routes[0].spec.path_prefix = Some("/v1".to_owned());
    let mut generation = plan(world.input())
        .expect("traffic plan")
        .create_generations
        .remove(0);
    generation.status.phase = TrafficGenerationPhase::Active;
    generation.spec.targets.push({
        let mut other_port = generation.spec.targets[0].clone();
        other_port.endpoint.set_port(9_090);
        other_port
    });

    let routing = active_routing(&[generation.clone()]);
    assert_eq!(routing.len(), 1);
    assert_eq!(routing[0].service_id, generation.spec.service_id);
    assert_eq!(
        routing[0].rule,
        "(Host(`api.example.test`)) && PathPrefix(`/v1`)"
    );
    assert_eq!(routing[0].entry_points, ["web"]);
    assert_eq!(routing[0].servers, ["http://10.42.1.10:8080"]);

    generation.status.phase = TrafficGenerationPhase::Retired;
    assert!(active_routing(&[generation]).is_empty());
}
