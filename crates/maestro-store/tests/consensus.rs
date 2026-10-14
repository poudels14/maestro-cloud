use std::path::Path;
use std::time::Duration;

use maestro_core::{
    DeploySpec, Deployment, DeploymentId, DeploymentStatus, NodeId, ServiceId, ServiceSpec,
};
use maestro_store::{Consensus, ConsensusError, ConsensusSettings, Mutation};

const LEADER_TIMEOUT: Duration = Duration::from_secs(10);

async fn start(data_dir: &Path) -> Consensus {
    let consensus = Consensus::open(
        ConsensusSettings::new(NodeId(1), "127.0.0.1:7400", data_dir),
        None,
    )
    .await
    .unwrap();
    consensus.bootstrap_single_voter().await.unwrap();
    consensus.wait_until_leader(LEADER_TIMEOUT).await.unwrap();
    consensus
}

fn put_service(id: &str) -> Mutation {
    Mutation::PutService {
        id: ServiceId::from(id),
        spec: ServiceSpec {
            name: id.to_string(),
            image: Some("nginx:1.27".to_string()),
            build: None,
            deploy: DeploySpec::default(),
            ingress: None,
            preview: None,
        },
    }
}

fn create_deployment(service: &str, id: &str) -> Mutation {
    let Mutation::PutService { spec, .. } = put_service(service) else {
        unreachable!()
    };
    Mutation::CreateDeployment {
        deployment: Deployment {
            id: DeploymentId::from(id),
            service_id: ServiceId::from(service),
            spec,
            status: DeploymentStatus::Queued,
            image: None,
            failure: None,
            created_at_ms: 1,
            updated_at_ms: 1,
        },
    }
}

#[tokio::test]
async fn single_voter_commits_writes() {
    let directory = tempfile::tempdir().unwrap();
    let consensus = start(directory.path()).await;

    let first = consensus.write(vec![put_service("api")]).await.unwrap();
    let second = consensus
        .write(vec![create_deployment("api", "d1")])
        .await
        .unwrap();
    assert_eq!((first, second), (1, 2));
    assert_eq!(
        consensus
            .state()
            .deployments_with_status(DeploymentStatus::Queued)
            .unwrap()
            .len(),
        1
    );
    consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn rejected_writes_do_not_stall_the_log() {
    let directory = tempfile::tempdir().unwrap();
    let consensus = start(directory.path()).await;

    let rejected = consensus
        .write(vec![create_deployment("missing", "d1")])
        .await;
    assert!(matches!(rejected, Err(ConsensusError::Rejected(_))));

    let version = consensus.write(vec![put_service("api")]).await.unwrap();
    assert_eq!(version, 1);
    consensus.shutdown().await.unwrap();
}

#[tokio::test]
async fn state_survives_restart() {
    let directory = tempfile::tempdir().unwrap();
    {
        let consensus = start(directory.path()).await;
        consensus.write(vec![put_service("api")]).await.unwrap();
        consensus.shutdown().await.unwrap();
    }

    let restarted = start(directory.path()).await;
    assert!(
        restarted
            .state()
            .service(&ServiceId::from("api"))
            .unwrap()
            .is_some()
    );
    let version = restarted.write(vec![put_service("web")]).await.unwrap();
    assert_eq!(version, 2);
    restarted.shutdown().await.unwrap();
}

#[tokio::test]
async fn restarts_from_snapshot() {
    let directory = tempfile::tempdir().unwrap();
    {
        let consensus = start(directory.path()).await;
        consensus
            .write(vec![put_service("api"), create_deployment("api", "d1")])
            .await
            .unwrap();
        consensus.trigger_snapshot().await.unwrap();
        tokio::time::sleep(Duration::from_millis(300)).await;
        consensus.shutdown().await.unwrap();
    }
    assert!(directory.path().join("snapshot.json").exists());

    let restarted = start(directory.path()).await;
    assert_eq!(
        restarted
            .state()
            .deployments(&ServiceId::from("api"))
            .unwrap()
            .len(),
        1
    );
    restarted.write(vec![put_service("web")]).await.unwrap();
    assert_eq!(restarted.state().services().unwrap().len(), 2);
    restarted.shutdown().await.unwrap();
}
