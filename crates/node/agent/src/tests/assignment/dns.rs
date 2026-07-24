use super::*;

use kernel_api::WorkloadId;

const RESOLVER_SERVICE_ID: &str = "maestro-system-dns";

#[tokio::test]
async fn delegated_dns_starts_the_resolver_before_injecting_its_address()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let resolver_service_id = ServiceId::new(RESOLVER_SERVICE_ID)?;
    let resolver_deployment = resolver_deployment(DeploymentPhase::Ready);
    let resolver_assignment = resolver_assignment();
    let application_deployment = deployment();
    let mut application_assignment = assignment();
    application_assignment.spec.workload_address = None;
    seed_pair(
        &world,
        [&resolver_deployment, &application_deployment],
        [&resolver_assignment, &application_assignment],
    )
    .await?;

    let report = delegated_dns_agent(&world, resolver_service_id)
        .reconcile_once()
        .await?;

    assert_eq!(report.desired, 2);
    assert_eq!(report.running, 2);
    assert_eq!(report.unresolved, 0);
    let resolver_id = WorkloadId::new(resolver_assignment.meta.id.as_str())?;
    let application_id = WorkloadId::new(application_assignment.meta.id.as_str())?;
    let resolver_spec = world
        .runtime
        .workload_spec(&resolver_id)?
        .ok_or("resolver workload missing")?;
    let application_spec = world
        .runtime
        .workload_spec(&application_id)?
        .ok_or("application workload missing")?;
    let resolver_handle = world
        .runtime
        .list(&cluster_id(), &node_id("node-1"))
        .await?
        .into_iter()
        .find(|workload| workload.handle.workload_id() == &resolver_id)
        .ok_or("resolver workload missing from runtime listing")?
        .handle;
    let resolver_address = world
        .network
        .inspect(&resolver_handle)
        .await?
        .attachments
        .first()
        .ok_or("resolver network attachment missing")?
        .address;
    assert_eq!(resolver_spec.configuration().dns_server, None);
    assert_eq!(
        application_spec.configuration().dns_server,
        Some(resolver_address)
    );
    let creates = world
        .runtime
        .calls()?
        .into_iter()
        .filter(|call| call.operation == FakeRuntimeOperation::Create)
        .filter_map(|call| call.workload_id)
        .collect::<Vec<_>>();
    assert_eq!(creates, [resolver_id, application_id]);
    Ok(())
}

#[tokio::test]
async fn delegated_dns_defers_applications_until_the_resolver_is_ready()
-> Result<(), Box<dyn std::error::Error>> {
    let world = World::new();
    let resolver_service_id = ServiceId::new(RESOLVER_SERVICE_ID)?;
    let resolver_deployment = resolver_deployment(DeploymentPhase::PendingReady);
    let resolver_assignment = resolver_assignment();
    let application_deployment = deployment();
    let mut application_assignment = assignment();
    application_assignment.spec.workload_address = None;
    seed_pair(
        &world,
        [&resolver_deployment, &application_deployment],
        [&resolver_assignment, &application_assignment],
    )
    .await?;

    let report = delegated_dns_agent(&world, resolver_service_id)
        .reconcile_once()
        .await?;

    assert_eq!(report.running, 1);
    assert_eq!(report.unresolved, 1);
    let application_id = WorkloadId::new(application_assignment.meta.id.as_str())?;
    assert_eq!(world.runtime.workload_spec(&application_id)?, None);
    let stored = load_assignment(&world, &application_assignment.meta.id).await?;
    assert_eq!(stored.status.phase, AssignmentPhase::Pending);
    assert_eq!(
        stored
            .status
            .conditions
            .first()
            .map(|condition| condition.reason.0.as_str()),
        Some("DnsResolverUnavailable")
    );
    Ok(())
}

fn delegated_dns_agent(world: &World, resolver_service_id: ServiceId) -> AssignmentAgent {
    AssignmentAgent::new(
        world.store.clone(),
        world.runtime.clone(),
        world.network.clone(),
        AssignmentAgentSettings {
            cluster_id: cluster_id(),
            node_id: node_id("node-1"),
            network: NetworkSpec {
                name: "maestro-dev".to_owned(),
                addressing: NetworkAddressing::Delegated,
                mtu_bytes: 1_500,
            },
            dns: WorkloadDns::DelegatedService(resolver_service_id),
            system_host_ports: Default::default(),
            stop_timeout: Duration::from_secs(5),
            resync_interval: Duration::from_secs(30),
            restart_backoff_base: Duration::from_secs(5),
            restart_backoff_max: Duration::from_secs(60),
            reconcile_timeout: Duration::from_secs(10),
            secrets_root: world.secrets.path().to_path_buf(),
            node_api_root: world.node_api.path().join("mounts"),
        },
        None,
        world.monotonic_clock.clone(),
        world.status_clock.clone(),
    )
    .unwrap()
}

fn resolver_deployment(phase: DeploymentPhase) -> Deployment {
    let mut deployment = deployment();
    deployment.meta.id = DeploymentId::new("maestro-system-dns-deployment").unwrap();
    deployment.spec.service_id = ServiceId::new(RESOLVER_SERVICE_ID).unwrap();
    deployment.spec.service.name = "Maestro DNS".to_owned();
    deployment.status.phase = phase;
    deployment
}

fn resolver_assignment() -> Assignment {
    let mut assignment = assignment();
    assignment.meta.id = AssignmentId::new("maestro-system-dns-assignment").unwrap();
    assignment.spec.service_id = ServiceId::new(RESOLVER_SERVICE_ID).unwrap();
    assignment.spec.deployment_id = DeploymentId::new("maestro-system-dns-deployment").unwrap();
    assignment.spec.workload_address = None;
    assignment
}

async fn seed_pair(
    world: &World,
    deployments: [&Deployment; 2],
    assignments: [&Assignment; 2],
) -> Result<(), Box<dyn std::error::Error>> {
    let keyspace = Keyspace::new(&cluster_id());
    for deployment in deployments {
        put_resource(
            world.store.as_ref(),
            keyspace.resource(
                &ResourceKind::new("Deployment")?,
                &ResourceName::new(deployment.meta.id.as_str())?,
            ),
            serde_json::to_vec(deployment)?,
        )
        .await?;
    }
    for assignment in assignments {
        put_resource(
            world.store.as_ref(),
            keyspace.resource(
                &ResourceKind::new("Assignment")?,
                &ResourceName::new(assignment.meta.id.as_str())?,
            ),
            serde_json::to_vec(assignment)?,
        )
        .await?;
    }
    Ok(())
}

async fn load_assignment(
    world: &World,
    assignment_id: &AssignmentId,
) -> Result<Assignment, Box<dyn std::error::Error>> {
    let key = Keyspace::new(&cluster_id()).resource(
        &ResourceKind::new("Assignment")?,
        &ResourceName::new(assignment_id.as_str())?,
    );
    let stored = world.store.get(&key).await?.ok_or("assignment missing")?;
    Ok(serde_json::from_slice(&stored.value)?)
}
