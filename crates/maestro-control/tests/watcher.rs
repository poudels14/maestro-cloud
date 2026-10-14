mod support;

use std::sync::{Arc, Mutex};

use maestro_control::{BuildWatcher, RemoteHeads, StoreWriter};
use maestro_core::{
    Deployment, DeploymentId, DeploymentStatus, GitCommit, ServiceControls, ServiceId,
};
use maestro_store::{Mutation, SecretKey};
use support::{LocalWriter, build_service};

struct FakeHeads {
    head: Mutex<Result<String, String>>,
    calls: Mutex<Vec<(String, String, Option<String>)>>,
}

impl RemoteHeads for FakeHeads {
    async fn head(
        &self,
        repo: String,
        branch: String,
        token: Option<String>,
    ) -> Result<String, String> {
        self.calls.lock().unwrap().push((repo, branch, token));
        self.head.lock().unwrap().clone()
    }
}

struct Harness {
    writer: Arc<LocalWriter>,
    heads: Arc<FakeHeads>,
    watcher: BuildWatcher<LocalWriter, FakeHeads>,
}

fn harness(head: Result<&str, &str>) -> Harness {
    let writer = Arc::new(LocalWriter::new());
    let key = Arc::new(SecretKey::from_bytes(&[5u8; 32]));
    let mut spec = build_service(1);
    let build = spec.build.as_mut().unwrap();
    build.watch = true;
    build
        .secrets
        .items
        .insert("GH_TOKEN".to_string(), "tok".to_string());
    writer
        .state()
        .apply(&[Mutation::PutService {
            id: ServiceId::from("api"),
            spec: key.seal_spec(&spec),
        }])
        .unwrap();
    let heads = Arc::new(FakeHeads {
        head: Mutex::new(head.map(str::to_string).map_err(str::to_string)),
        calls: Mutex::default(),
    });
    let watcher = BuildWatcher::new(writer.clone(), heads.clone(), key);
    Harness {
        writer,
        heads,
        watcher,
    }
}

fn record(harness: &Harness, id: &str, status: DeploymentStatus, commit: Option<&str>) {
    let spec = harness
        .writer
        .state()
        .service(&ServiceId::from("api"))
        .unwrap()
        .unwrap();
    harness
        .writer
        .state()
        .apply(&[Mutation::CreateDeployment {
            deployment: Deployment {
                id: DeploymentId::from(id),
                service_id: ServiceId::from("api"),
                spec,
                status,
                image: None,
                failure: None,
                created_at_ms: harness.writer.state().version().unwrap(),
                updated_at_ms: 0,
                git_commit: commit.map(|reference| GitCommit {
                    reference: reference.to_string(),
                    message: String::new(),
                }),
                built_on: None,
            },
        }])
        .unwrap();
}

fn deployment_count(harness: &Harness) -> usize {
    harness
        .writer
        .state()
        .deployments(&ServiceId::from("api"))
        .unwrap()
        .len()
}

#[tokio::test]
async fn queues_a_build_when_the_branch_moves() {
    let harness = harness(Ok("bbb"));
    record(&harness, "d1", DeploymentStatus::Ready, Some("aaa"));
    harness.watcher.poll().await.unwrap();
    assert_eq!(deployment_count(&harness), 2);
    assert_eq!(
        harness.heads.calls.lock().unwrap()[0],
        (
            "https://github.com/acme/api.git".to_string(),
            "main".to_string(),
            Some("tok".to_string())
        )
    );
}

#[tokio::test]
async fn leaves_up_to_date_and_in_flight_services_alone() {
    let harness = harness(Ok("aaa"));
    record(&harness, "d1", DeploymentStatus::Ready, Some("aaa"));
    harness.watcher.poll().await.unwrap();
    assert_eq!(deployment_count(&harness), 1);

    record(&harness, "d2", DeploymentStatus::Building, None);
    *harness.heads.head.lock().unwrap() = Ok("ccc".to_string());
    harness.watcher.poll().await.unwrap();
    assert_eq!(deployment_count(&harness), 2);
    assert_eq!(harness.heads.calls.lock().unwrap().len(), 1);
}

#[tokio::test]
async fn skips_frozen_services() {
    let harness = harness(Ok("bbb"));
    harness
        .writer
        .state()
        .apply(&[Mutation::SetServiceControls {
            id: ServiceId::from("api"),
            controls: ServiceControls {
                frozen: true,
                replicas_override: None,
            },
        }])
        .unwrap();
    harness.watcher.poll().await.unwrap();
    assert_eq!(deployment_count(&harness), 0);
}

#[tokio::test]
async fn backs_off_after_a_failed_check() {
    let harness = harness(Err("network down"));
    harness.watcher.poll().await.unwrap();
    harness.watcher.poll().await.unwrap();
    assert_eq!(harness.heads.calls.lock().unwrap().len(), 1);
    assert_eq!(deployment_count(&harness), 0);
}
