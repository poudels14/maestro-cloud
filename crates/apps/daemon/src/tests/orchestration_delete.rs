use kernel_api::{
    Assignment, Deployment, DnsRecord, ReplicaState, Service, Timestamp, TrafficGeneration,
};

use super::orchestration::RolloutWorld;

#[tokio::test]
async fn deleting_service_retires_traffic_then_collects_every_child()
-> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    for node_count in [1_u8, 3_u8] {
        let world = RolloutWorld::new(node_count).await?;
        world.converge().await?;
        world.mark_service_deleting(Timestamp(10_000)).await?;
        world.converge().await?;

        let services = world.list::<Service>("Service").await?;
        assert_eq!(services.len(), 1);
        assert!(
            services
                .first()
                .is_some_and(|service| service.meta.deletion_timestamp.is_some())
        );
        assert_eq!(
            world.list::<Assignment>("Assignment").await?.len(),
            usize::from(node_count)
        );

        world.set_time(41_000);
        world.converge().await?;

        let remaining_services = world.list::<Service>("Service").await?;
        let remaining_deployments = world.list::<Deployment>("Deployment").await?;
        let remaining_assignments = world.list::<Assignment>("Assignment").await?;
        let remaining_replicas = world.list::<ReplicaState>("ReplicaState").await?;
        let remaining_traffic = world.list::<TrafficGeneration>("TrafficGeneration").await?;
        assert!(
            remaining_services.is_empty(),
            "resources after deletion:\nservices={remaining_services:#?}\ndeployments={remaining_deployments:#?}\nassignments={remaining_assignments:#?}\nreplicas={remaining_replicas:#?}\ntraffic={remaining_traffic:#?}"
        );
        assert!(remaining_deployments.is_empty());
        assert!(remaining_assignments.is_empty());
        assert!(remaining_replicas.is_empty());
        assert!(remaining_traffic.is_empty());
        assert!(world.list::<DnsRecord>("DnsRecord").await?.is_empty());
    }
    Ok(())
}
