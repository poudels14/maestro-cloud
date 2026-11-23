#![cfg(target_os = "linux")]

use std::collections::{BTreeMap, BTreeSet};
use std::path::{Path, PathBuf};
use std::time::Duration;

use maestro_agent::{IngressWriter, traefik_container};
use maestro_core::{Backend, ReplicaId, RouteTable, ServiceId, ServiceRoute, affinity_token};
use maestro_runtime::{CniNetwork, ContainerdRuntime, ContainerdSettings, Runtime};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};

const INGRESS_PORT: u16 = 18080;
const METRICS_ADDRESS: &str = "127.0.0.1:19100";
const HOST: &str = "web.maestro.test";
const AFFINITY_HEADER: &str = "X-Session-Affinity";
const REPLICAS: [&str; 3] = ["d1-0", "d1-1", "d1-2"];
const READY_TIMEOUT: Duration = Duration::from_secs(60);

#[derive(Clone)]
struct Response {
    headers: BTreeMap<String, String>,
    body: String,
}

fn enabled() -> bool {
    std::env::var("MAESTRO_CONTAINERD_TESTS").is_ok_and(|value| value == "1")
}

#[tokio::test(flavor = "multi_thread")]
async fn traefik_tags_responses_with_the_replica_that_served_them() {
    if enabled() {
        replica_header().await;
    }
}

async fn replica_header() {
    let state = tempfile::tempdir().unwrap();
    let config_dir = state.path().join("traefik");
    let runtime = runtime(state.path()).await;
    let backends = start_backends().await;
    let mut writer = IngressWriter::new(config_dir.clone(), &[INGRESS_PORT]);
    writer.write(&route_table(&backends)).await.unwrap();
    let spec = traefik_spec(&config_dir);
    let _ = runtime.remove(&spec.id).await;
    runtime.pull(&spec.image).await.unwrap();
    runtime.start(&spec).await.unwrap();

    let first = wait_until_routed().await;
    let responses = [vec![first], requests(&[], 29).await].concat();
    responses.iter().for_each(|response| {
        assert_eq!(
            response.headers.get("x-maestro-replica"),
            Some(&response.body)
        );
    });
    let served = responses
        .iter()
        .map(|response| response.body.clone())
        .collect::<BTreeSet<_>>();
    assert_eq!(served, REPLICAS.map(String::from).into());

    let token = affinity_token(&ReplicaId::from("d1-1"));
    let pinned = requests(&[(AFFINITY_HEADER, token.as_str())], 10).await;
    assert!(pinned.iter().all(|response| {
        response.body == "d1-1"
            && response
                .headers
                .get("x-maestro-replica")
                .map(String::as_str)
                == Some("d1-1")
    }));

    let cookie = responses[0]
        .headers
        .get("set-cookie")
        .and_then(|cookie| cookie.split(';').next())
        .expect("the sticky cookie is set")
        .to_string();
    assert!(cookie.starts_with("maestro-affinity="));
    let sticky = requests(&[("Cookie", cookie.as_str())], 10).await;
    assert!(
        sticky
            .iter()
            .all(|response| response.headers.get("x-maestro-replica") == Some(&sticky[0].body))
    );

    let counted = scrape_request_counts().await;
    runtime
        .stop(&spec.id, Duration::from_secs(5))
        .await
        .unwrap();
    runtime.remove(&spec.id).await.unwrap();
    assert!(!counted.contains_key("svc-web@file"));
    assert_eq!(
        counted.keys().cloned().collect::<BTreeSet<_>>(),
        REPLICAS
            .map(|replica| format!("svc-web-{replica}@file"))
            .into()
    );
    assert_eq!(
        counted.values().sum::<u64>(),
        (responses.len() + pinned.len() + sticky.len()) as u64
    );
}

async fn runtime(state_dir: &Path) -> ContainerdRuntime {
    let plugin_dirs = std::env::var("CNI_PATH")
        .map(|paths| std::env::split_paths(&paths).collect())
        .unwrap_or_else(|_| {
            vec![
                PathBuf::from("/usr/local/libexec/cni"),
                PathBuf::from("/opt/cni/bin"),
            ]
        });
    let network = CniNetwork::bridge("maestro-test1", "10.254.8.0/24", plugin_dirs, state_dir);
    ContainerdRuntime::connect(
        ContainerdSettings {
            socket: PathBuf::from("/run/containerd/containerd.sock"),
            namespace: "maestro-test".to_string(),
            snapshotter: "overlayfs".to_string(),
            state_dir: state_dir.to_path_buf(),
            resolv_conf: PathBuf::from("/etc/resolv.conf"),
            log_uri: None,
        },
        network,
    )
    .await
    .unwrap()
}

fn traefik_spec(config_dir: &Path) -> maestro_runtime::ContainerSpec {
    let mut spec = traefik_container(config_dir, &[INGRESS_PORT]);
    spec.args = spec.args.map(|args| {
        args.into_iter()
            .map(
                |arg| match arg.starts_with("--entryPoints.metrics.address=") {
                    true => format!("--entryPoints.metrics.address={METRICS_ADDRESS}"),
                    false => arg,
                },
            )
            .collect()
    });
    spec
}

async fn start_backends() -> Vec<Backend> {
    let mut backends = Vec::new();
    for replica in REPLICAS {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        backends.push(Backend {
            replica_id: ReplicaId::from(replica),
            address: listener.local_addr().unwrap(),
        });
        tokio::spawn(async move {
            loop {
                let (mut connection, _) = listener.accept().await.unwrap();
                tokio::spawn(async move {
                    let mut request = [0u8; 4096];
                    if connection.read(&mut request).await.is_ok() {
                        let response = format!(
                            "HTTP/1.1 200 OK\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{replica}",
                            replica.len()
                        );
                        let _ = connection.write_all(response.as_bytes()).await;
                    }
                });
            }
        });
    }
    backends
}

fn route_table(backends: &[Backend]) -> RouteTable {
    RouteTable {
        routes: BTreeMap::from([(
            ServiceId::from("web"),
            ServiceRoute {
                hosts: vec![HOST.to_string()],
                backends: backends.to_vec(),
                affinity_header: Some(AFFINITY_HEADER.to_string()),
            },
        )]),
        blocked_ips: Vec::new(),
    }
}

async fn wait_until_routed() -> Response {
    tokio::time::timeout(READY_TIMEOUT, async {
        loop {
            match get(&[]).await {
                Some(response) if response.headers.contains_key("x-maestro-replica") => {
                    break response;
                }
                _ => tokio::time::sleep(Duration::from_millis(250)).await,
            }
        }
    })
    .await
    .expect("traefik never routed a request to a replica")
}

async fn requests(headers: &[(&str, &str)], count: usize) -> Vec<Response> {
    let mut responses = Vec::new();
    for _ in 0..count {
        responses.push(get(headers).await.expect("request failed"));
    }
    responses
}

async fn get(headers: &[(&str, &str)]) -> Option<Response> {
    let mut stream = TcpStream::connect(("127.0.0.1", INGRESS_PORT)).await.ok()?;
    let extra = headers
        .iter()
        .map(|(name, value)| format!("{name}: {value}\r\n"))
        .collect::<String>();
    let request = format!("GET / HTTP/1.1\r\nHost: {HOST}\r\nConnection: close\r\n{extra}\r\n");
    stream.write_all(request.as_bytes()).await.ok()?;
    let mut raw = String::new();
    stream.read_to_string(&mut raw).await.ok()?;
    let (head, body) = raw.split_once("\r\n\r\n")?;
    let mut lines = head.lines();
    let status = lines.next()?;
    let headers = lines
        .filter_map(|line| line.split_once(':'))
        .map(|(name, value)| (name.trim().to_lowercase(), value.trim().to_string()))
        .collect();
    status.contains(" 200 ").then(|| Response {
        headers,
        body: body.to_string(),
    })
}

async fn scrape_request_counts() -> BTreeMap<String, u64> {
    let mut stream = TcpStream::connect(METRICS_ADDRESS).await.unwrap();
    stream
        .write_all(b"GET /metrics HTTP/1.0\r\nHost: metrics\r\n\r\n")
        .await
        .unwrap();
    let mut response = String::new();
    stream.read_to_string(&mut response).await.unwrap();
    response
        .lines()
        .filter(|line| line.starts_with("traefik_service_requests_total{"))
        .filter_map(|line| {
            let service = line.split("service=\"").nth(1)?.split('"').next()?;
            let count = line.rsplit_once(' ')?.1.parse::<f64>().ok()?;
            Some((service.to_string(), count as u64))
        })
        .fold(BTreeMap::new(), |mut counted, (service, count)| {
            *counted.entry(service).or_default() += count;
            counted
        })
}
