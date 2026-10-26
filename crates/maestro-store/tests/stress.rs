mod support;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

use maestro_core::{
    Assignment, ByteSize, Deployment, DeploymentId, DeploymentStatus, EffectiveResources,
    GitCommit, Millicores, NodeId, ReplicaId, ResolvedRequest, ServiceId,
};
use maestro_proto::ClusterCa;
use maestro_store::Mutation;
use tokio::sync::Mutex;

use support::{TestNode, put_service, shutdown, start_cluster};

const NODE_IDS: [u64; 3] = [1, 2, 3];
const WRITE_TIMEOUT: Duration = Duration::from_secs(30);
const CONVERGE_TIMEOUT: Duration = Duration::from_secs(60);

struct Load {
    services: usize,
    deploys: usize,
    replicas_per_node: usize,
    kill_leader: bool,
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
#[ignore = "stress test; run with `cargo test -p maestro-store --test stress -- --ignored --nocapture`"]
async fn survives_bursts_of_deploys() {
    let load = Load {
        services: env_number("STRESS_SERVICES", 50),
        deploys: env_number("STRESS_DEPLOYS", 40),
        replicas_per_node: env_number("STRESS_REPLICAS_PER_NODE", 30),
        kill_leader: std::env::var("STRESS_KILL_LEADER").map_or(true, |value| value != "0"),
    };
    let ca = ClusterCa::generate("stress").unwrap();
    let nodes = Arc::new(start_cluster(&ca, &NODE_IDS, None).await);
    let alive = Arc::new(NODE_IDS.map(|_| AtomicBool::new(true)));
    let latencies = Arc::new(Mutex::new(Vec::new()));
    let completed = Arc::new(AtomicUsize::new(0));
    let total_writes = load.services * (load.deploys * (5 + NODE_IDS.len()) - 1);

    let services = (0..load.services)
        .map(|index| put_service(&format!("svc-{index}")))
        .collect();
    write(&nodes, &alive, services).await;

    let started = Instant::now();
    let writers = (0..load.services)
        .map(|index| {
            let nodes = nodes.clone();
            let alive = alive.clone();
            let latencies = latencies.clone();
            let completed = completed.clone();
            let deploys = load.deploys;
            let replicas_per_node = load.replicas_per_node;
            tokio::spawn(async move {
                let service_id = ServiceId::new(format!("svc-{index}"));
                for deploy in 0..deploys {
                    let deployment_id = DeploymentId::new(format!("d-{index}-{deploy}"));
                    let previous = deploy
                        .checked_sub(1)
                        .map(|previous| DeploymentId::new(format!("d-{index}-{previous}")));
                    for mutations in
                        deploy_writes(&service_id, &deployment_id, previous, replicas_per_node)
                    {
                        let latency = write(&nodes, &alive, mutations).await;
                        latencies.lock().await.push(latency);
                        completed.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
        })
        .collect::<Vec<_>>();

    let killed = match load.kill_leader {
        true => {
            wait_until_count(&completed, total_writes / 2).await;
            let leader = leader_index(&nodes, &alive).expect("a leader before the kill");
            alive[leader].store(false, Ordering::SeqCst);
            shutdown(std::slice::from_ref(&nodes[leader])).await;
            println!(
                "killed the leader (node {}) halfway through",
                NODE_IDS[leader]
            );
            Some(leader)
        }
        false => None,
    };
    for writer in writers {
        writer.await.unwrap();
    }
    let elapsed = started.elapsed();

    let survivors = (0..nodes.len())
        .filter(|index| Some(*index) != killed)
        .collect::<Vec<_>>();
    let target = nodes[survivors[0]].consensus.metrics().last_log_index;
    tokio::time::timeout(CONVERGE_TIMEOUT, async {
        while survivors
            .iter()
            .any(|index| nodes[*index].consensus.metrics().last_applied_index < target)
        {
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
    })
    .await
    .expect("survivors did not converge");
    let snapshots = survivors
        .iter()
        .map(|index| state_summary(&nodes[*index], load.services))
        .collect::<Vec<_>>();
    assert!(
        snapshots.windows(2).all(|pair| pair[0] == pair[1]),
        "survivors disagree on the final state"
    );
    let expected_ready = (0..load.services)
        .map(|index| format!("svc-{index}: d-{index}-{} ready", load.deploys - 1))
        .collect::<Vec<_>>();
    assert_eq!(
        snapshots[0]
            .iter()
            .map(|(service, latest, _)| format!("{service}: {latest} ready"))
            .collect::<Vec<_>>(),
        expected_ready
    );

    let mut latencies = latencies.lock().await.clone();
    latencies.sort();
    let percentile = |fraction: f64| {
        latencies[((latencies.len() as f64 * fraction) as usize).min(latencies.len() - 1)]
    };
    let metrics = nodes[survivors[0]].consensus.metrics();
    println!(
        "{} writes from {} concurrent services in {:.1?} ({:.0} writes/s)",
        latencies.len(),
        load.services,
        elapsed,
        latencies.len() as f64 / elapsed.as_secs_f64()
    );
    println!(
        "write latency p50 {:?}  p95 {:?}  p99 {:?}  max {:?}",
        percentile(0.50),
        percentile(0.95),
        percentile(0.99),
        latencies[latencies.len() - 1]
    );
    println!(
        "raft term {}  last log {:?}  snapshot at {:?}",
        metrics.term, metrics.last_log_index, metrics.snapshot_index
    );
    survivors.iter().for_each(|index| {
        println!(
            "node {} data dir {:.1} MiB",
            NODE_IDS[*index],
            directory_bytes(nodes[*index].directory.path()) as f64 / (1 << 20) as f64
        );
    });
    let survivor_nodes = survivors
        .iter()
        .map(|index| &nodes[*index])
        .collect::<Vec<_>>();
    for node in survivor_nodes {
        shutdown(std::slice::from_ref(node)).await;
    }
}

fn deploy_writes(
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    previous: Option<DeploymentId>,
    replicas_per_node: usize,
) -> Vec<Vec<Mutation>> {
    let status = |status: DeploymentStatus, image: Option<&str>| Mutation::SetDeploymentStatus {
        service_id: service_id.clone(),
        deployment_id: deployment_id.clone(),
        status,
        image: image.map(str::to_string),
        failure: None,
        at_ms: now_ms(),
    };
    let image = format!("maestro.local/{service_id}:{deployment_id}");
    let assignments = NODE_IDS.iter().map(|node_id| {
        vec![Mutation::SetNodeAssignments {
            node_id: NodeId(*node_id),
            assignments: (0..replicas_per_node)
                .map(|index| assignment(service_id, deployment_id, *node_id, index as u32))
                .collect(),
        }]
    });
    [
        vec![Mutation::CreateDeployment {
            deployment: Deployment {
                id: deployment_id.clone(),
                service_id: service_id.clone(),
                spec: match put_service(service_id.as_str()) {
                    Mutation::PutService { spec, .. } => spec,
                    _ => unreachable!(),
                },
                status: DeploymentStatus::Queued,
                image: None,
                failure: None,
                created_at_ms: now_ms(),
                updated_at_ms: now_ms(),
                git_commit: None,
                built_on: None,
            },
        }],
        vec![status(DeploymentStatus::Building, None)],
        vec![Mutation::SetDeploymentBuild {
            service_id: service_id.clone(),
            deployment_id: deployment_id.clone(),
            commit: Some(GitCommit {
                reference: "0123456789abcdef0123456789abcdef01234567".to_string(),
                message: "Stress test commit".to_string(),
            }),
            built_on: Some(NodeId(NODE_IDS[0])),
        }],
        vec![status(DeploymentStatus::Deploying, Some(&image))],
    ]
    .into_iter()
    .chain(assignments)
    .chain([vec![status(DeploymentStatus::Ready, Some(&image))]])
    .chain([previous
        .map(|previous| Mutation::SetDeploymentStatus {
            service_id: service_id.clone(),
            deployment_id: previous,
            status: DeploymentStatus::Stopped,
            image: None,
            failure: None,
            at_ms: now_ms(),
        })
        .into_iter()
        .collect()])
    .filter(|mutations: &Vec<Mutation>| !mutations.is_empty())
    .collect()
}

async fn write(nodes: &[TestNode], alive: &[AtomicBool; 3], mutations: Vec<Mutation>) -> Duration {
    let started = Instant::now();
    let mut last_error;
    let mut attempted = false;
    loop {
        let result = match leader_index(nodes, alive) {
            Some(leader) => {
                let result = nodes[leader].consensus.write(mutations.clone()).await;
                let retry = std::mem::replace(&mut attempted, true);
                match result {
                    Err(maestro_store::ConsensusError::Rejected(reason))
                        if retry && reason.contains("already exists") =>
                    {
                        Ok(0)
                    }
                    other => other,
                }
            }
            None => Err(maestro_store::ConsensusError::Rejected(
                "no node reported itself as leader".to_string(),
            )),
        };
        match result {
            Ok(_) => break started.elapsed(),
            Err(error) => last_error = error.to_string(),
        }
        assert!(
            started.elapsed() < WRITE_TIMEOUT,
            "a write did not commit within {WRITE_TIMEOUT:?}; last error: {last_error}; views: {}",
            leader_views(nodes, alive)
        );
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn leader_views(nodes: &[TestNode], alive: &[AtomicBool; 3]) -> String {
    (0..nodes.len())
        .filter(|index| alive[*index].load(Ordering::SeqCst))
        .map(|index| {
            let metrics = nodes[index].consensus.metrics();
            format!(
                "node {} term {} leader {:?} applied {:?}/{:?}",
                NODE_IDS[index],
                metrics.term,
                metrics.leader,
                metrics.last_applied_index,
                metrics.last_log_index
            )
        })
        .collect::<Vec<_>>()
        .join(", ")
}

fn leader_index(nodes: &[TestNode], alive: &[AtomicBool; 3]) -> Option<usize> {
    (0..nodes.len())
        .find(|index| alive[*index].load(Ordering::SeqCst) && nodes[*index].consensus.is_leader())
}

fn state_summary(node: &TestNode, services: usize) -> Vec<(String, String, usize)> {
    let state = node.consensus.state();
    (0..services)
        .map(|index| {
            let service_id = ServiceId::new(format!("svc-{index}"));
            let deployments = state.deployments(&service_id).unwrap();
            let latest = deployments
                .iter()
                .filter(|deployment| deployment.status == DeploymentStatus::Ready)
                .max_by_key(|deployment| deployment.created_at_ms)
                .map(|deployment| deployment.id.to_string())
                .unwrap_or_default();
            (service_id.to_string(), latest, deployments.len())
        })
        .collect()
}

fn assignment(
    service_id: &ServiceId,
    deployment_id: &DeploymentId,
    node_id: u64,
    index: u32,
) -> Assignment {
    Assignment {
        replica_id: ReplicaId::new(format!("{deployment_id}-{node_id}-{index}")),
        service_id: service_id.clone(),
        deployment_id: deployment_id.clone(),
        node_id: NodeId(node_id),
        index,
        image: format!("maestro.local/{service_id}:{deployment_id}"),
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

async fn wait_until_count(counter: &AtomicUsize, target: usize) {
    while counter.load(Ordering::Relaxed) < target {
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

fn directory_bytes(path: &std::path::Path) -> u64 {
    std::fs::read_dir(path)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| match entry.file_type() {
            Ok(kind) if kind.is_dir() => directory_bytes(&entry.path()),
            _ => entry.metadata().map_or(0, |metadata| metadata.len()),
        })
        .sum()
}

fn env_number(name: &str, default: usize) -> usize {
    std::env::var(name)
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(default)
}

fn now_ms() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_millis() as u64)
}
