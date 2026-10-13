use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use containerd_client::services::v1::container::Runtime as ContainerRuntime;
use containerd_client::services::v1::snapshots::{PrepareSnapshotRequest, RemoveSnapshotRequest};
use containerd_client::services::v1::{
    Container, CreateContainerRequest, CreateTaskRequest, DeleteContainerRequest,
    DeleteTaskRequest, GetImageRequest, GetRequest, KillRequest, ListContainersRequest,
    ReadContentRequest, StartRequest, SubscribeRequest, TransferRequest, WaitRequest,
};
use containerd_client::services::v1::{
    containers_client::ContainersClient, content_client::ContentClient,
    events_client::EventsClient, images_client::ImagesClient,
    snapshots::snapshots_client::SnapshotsClient, tasks_client::TasksClient,
    transfer_client::TransferClient,
};
use containerd_client::tonic::transport::Channel;
use containerd_client::tonic::{Code, Request, Status};
use containerd_client::types::transfer::{ImageStore, OciRegistry, UnpackConfiguration};
use containerd_client::types::{Descriptor, Platform};
use prost::Message;
use prost_types::Any;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;

use crate::cni::CniNetwork;
use crate::oci;
use crate::reference;
use crate::spec::{
    ContainerFiles, ContainerSpec, ContainerState, ContainerStatus, ImageConfig, Runtime,
    RuntimeError, RuntimeEvent, StartedContainer,
};

const NAMESPACE_HEADER: &str = "containerd-namespace";
const SPEC_TYPE_URL: &str = "types.containerd.io/opencontainers/runtime-spec/1/Spec";
const RUNC_OPTIONS_TYPE_URL: &str = "containerd.runc.v1.Options";
const RUNC_RUNTIME: &str = "io.containerd.runc.v2";
const EXIT_TOPIC: &str = "/tasks/exit";
const OOM_TOPIC: &str = "/tasks/oom";
const EVENT_BUFFER: usize = 256;
const EVENT_RESUBSCRIBE_DELAY: Duration = Duration::from_secs(1);
const SIGTERM: u32 = 15;
const SIGKILL: u32 = 9;
const KILL_WAIT: Duration = Duration::from_secs(10);
const INDEX_MEDIA_TYPES: [&str; 2] = [
    "application/vnd.oci.image.index.v1+json",
    "application/vnd.docker.distribution.manifest.list.v2+json",
];

#[derive(Clone, Debug)]
pub struct ContainerdSettings {
    pub socket: PathBuf,
    pub namespace: String,
    pub snapshotter: String,
    pub state_dir: PathBuf,
    pub resolv_conf: PathBuf,
}

pub struct ContainerdRuntime {
    channel: Channel,
    settings: ContainerdSettings,
    network: CniNetwork,
    platform: Platform,
}

#[derive(Clone, PartialEq, Message)]
struct RuncOptions {
    #[prost(bool, tag = "9")]
    systemd_cgroup: bool,
}

#[derive(Deserialize)]
struct ManifestIndex {
    manifests: Vec<PlatformManifest>,
}

#[derive(Deserialize)]
struct PlatformManifest {
    digest: String,
    platform: Option<ManifestPlatform>,
}

#[derive(Deserialize)]
struct ManifestPlatform {
    os: String,
    architecture: String,
    variant: Option<String>,
}

#[derive(Deserialize)]
struct Manifest {
    config: ManifestConfig,
}

#[derive(Deserialize)]
struct ManifestConfig {
    digest: String,
}

#[derive(Deserialize)]
struct ImageBlob {
    #[serde(default)]
    config: ImageConfig,
    rootfs: RootFs,
}

#[derive(Deserialize)]
struct RootFs {
    diff_ids: Vec<String>,
}

impl ContainerdRuntime {
    pub async fn connect(
        settings: ContainerdSettings,
        network: CniNetwork,
    ) -> Result<Self, RuntimeError> {
        let channel = containerd_client::connect(&settings.socket)
            .await
            .map_err(|error| {
                RuntimeError::Connect(format!("{}: {error}", settings.socket.display()))
            })?;
        Ok(Self {
            channel,
            settings,
            network,
            platform: host_platform(),
        })
    }

    fn request<T>(&self, message: T) -> Request<T> {
        let mut request = Request::new(message);
        if let Ok(namespace) = self.settings.namespace.parse() {
            request.metadata_mut().insert(NAMESPACE_HEADER, namespace);
        }
        request
    }

    async fn image_exists(&self, name: &str) -> Result<bool, RuntimeError> {
        let response = ImagesClient::new(self.channel.clone())
            .get(self.request(GetImageRequest {
                name: name.to_string(),
            }))
            .await;
        match response {
            Ok(_) => Ok(true),
            Err(status) if status.code() == Code::NotFound => Ok(false),
            Err(status) => Err(status.into()),
        }
    }

    async fn read_blob(&self, digest: &str) -> Result<Vec<u8>, RuntimeError> {
        let mut stream = ContentClient::new(self.channel.clone())
            .read(self.request(ReadContentRequest {
                digest: digest.to_string(),
                offset: 0,
                size: 0,
            }))
            .await?
            .into_inner();
        let mut blob = Vec::new();
        while let Some(chunk) = stream.message().await? {
            blob.extend_from_slice(&chunk.data);
        }
        Ok(blob)
    }

    async fn image_config(&self, image: &str) -> Result<(ImageConfig, String), RuntimeError> {
        let target = ImagesClient::new(self.channel.clone())
            .get(self.request(GetImageRequest {
                name: image.to_string(),
            }))
            .await?
            .into_inner()
            .image
            .and_then(|image| image.target)
            .ok_or_else(|| image_error(image, "image has no target descriptor"))?;
        let manifest_digest = self.platform_manifest(image, &target).await?;
        let manifest: Manifest = serde_json::from_slice(&self.read_blob(&manifest_digest).await?)?;
        let blob: ImageBlob =
            serde_json::from_slice(&self.read_blob(&manifest.config.digest).await?)?;
        let chain_id = chain_id(&blob.rootfs.diff_ids)
            .ok_or_else(|| image_error(image, "image has no layers"))?;
        Ok((blob.config, chain_id))
    }

    async fn platform_manifest(
        &self,
        image: &str,
        target: &Descriptor,
    ) -> Result<String, RuntimeError> {
        match INDEX_MEDIA_TYPES.contains(&target.media_type.as_str()) {
            false => Ok(target.digest.clone()),
            true => {
                let index: ManifestIndex =
                    serde_json::from_slice(&self.read_blob(&target.digest).await?)?;
                index
                    .manifests
                    .into_iter()
                    .find(|manifest| {
                        manifest
                            .platform
                            .as_ref()
                            .is_some_and(|platform| matches_platform(platform, &self.platform))
                    })
                    .map(|manifest| manifest.digest)
                    .ok_or_else(|| {
                        image_error(
                            image,
                            &format!(
                                "no manifest for {}/{}",
                                self.platform.os, self.platform.architecture
                            ),
                        )
                    })
            }
        }
    }

    async fn create_and_start(
        &self,
        spec: &ContainerSpec,
        image: &str,
    ) -> Result<StartedContainer, RuntimeError> {
        let (image_config, chain_id) = self.image_config(image).await?;
        let files = self.write_container_files(&spec.id).await?;
        let oci_spec = oci::build_spec(spec, &image_config, &files)?;
        let mounts = SnapshotsClient::new(self.channel.clone())
            .prepare(self.request(PrepareSnapshotRequest {
                snapshotter: self.settings.snapshotter.clone(),
                key: spec.id.clone(),
                parent: chain_id,
                labels: HashMap::new(),
            }))
            .await?
            .into_inner()
            .mounts;
        let runc_options = RuncOptions {
            systemd_cgroup: true,
        };
        ContainersClient::new(self.channel.clone())
            .create(self.request(CreateContainerRequest {
                container: Some(Container {
                    id: spec.id.clone(),
                    labels: spec.labels.clone().into_iter().collect(),
                    image: image.to_string(),
                    runtime: Some(ContainerRuntime {
                        name: RUNC_RUNTIME.to_string(),
                        options: Some(Any {
                            type_url: RUNC_OPTIONS_TYPE_URL.to_string(),
                            value: runc_options.encode_to_vec(),
                        }),
                    }),
                    spec: Some(Any {
                        type_url: SPEC_TYPE_URL.to_string(),
                        value: serde_json::to_vec(&oci_spec)?,
                    }),
                    snapshotter: self.settings.snapshotter.clone(),
                    snapshot_key: spec.id.clone(),
                    ..Default::default()
                }),
            }))
            .await?;
        let log_dir = self.settings.state_dir.join("logs").join(&spec.id);
        tokio::fs::create_dir_all(&log_dir).await?;
        let pid = TasksClient::new(self.channel.clone())
            .create(self.request(CreateTaskRequest {
                container_id: spec.id.clone(),
                rootfs: mounts,
                stdout: file_uri(&log_dir.join("stdout.log")),
                stderr: file_uri(&log_dir.join("stderr.log")),
                ..Default::default()
            }))
            .await?
            .into_inner()
            .pid;
        let ip = self
            .network
            .add(&spec.id, &format!("/proc/{pid}/ns/net"))
            .await?;
        write_hosts(&files.hosts, &spec.id, ip).await?;
        TasksClient::new(self.channel.clone())
            .start(self.request(StartRequest {
                container_id: spec.id.clone(),
                exec_id: String::new(),
            }))
            .await?;
        Ok(StartedContainer {
            id: spec.id.clone(),
            ip,
        })
    }

    async fn write_container_files(&self, id: &str) -> Result<ContainerFiles, RuntimeError> {
        let directory = self.settings.state_dir.join("containers").join(id);
        tokio::fs::create_dir_all(&directory).await?;
        let files = ContainerFiles {
            hosts: directory.join("hosts"),
            hostname: directory.join("hostname"),
            resolv_conf: directory.join("resolv.conf"),
        };
        write_hosts(&files.hosts, id, None).await?;
        tokio::fs::write(&files.hostname, format!("{id}\n")).await?;
        tokio::fs::copy(&self.settings.resolv_conf, &files.resolv_conf).await?;
        Ok(files)
    }

    async fn task_pid(&self, id: &str) -> Result<Option<u32>, RuntimeError> {
        let response = TasksClient::new(self.channel.clone())
            .get(self.request(GetRequest {
                container_id: id.to_string(),
                exec_id: String::new(),
            }))
            .await;
        ignore_not_found(response.map(|response| {
            response
                .into_inner()
                .process
                .map(|process| process.pid)
                .filter(|pid| *pid > 0)
        }))
    }

    async fn signal_and_wait(
        &self,
        id: &str,
        signal: u32,
        timeout: Duration,
    ) -> Result<bool, RuntimeError> {
        let mut tasks = TasksClient::new(self.channel.clone());
        let killed = tasks
            .kill(self.request(KillRequest {
                container_id: id.to_string(),
                exec_id: String::new(),
                signal,
                all: true,
            }))
            .await;
        ignore_not_found(killed.map(|_| ()))?;
        let wait = tasks.wait(self.request(WaitRequest {
            container_id: id.to_string(),
            exec_id: String::new(),
        }));
        match tokio::time::timeout(timeout, wait).await {
            Ok(result) => ignore_not_found(result.map(|_| ())).map(|_| true),
            Err(_) => Ok(false),
        }
    }
}

impl Runtime for ContainerdRuntime {
    async fn pull(&self, image: &str) -> Result<(), RuntimeError> {
        let name = reference::normalize(image);
        match self.image_exists(&name).await? {
            true => Ok(()),
            false => {
                let source = OciRegistry {
                    reference: name.clone(),
                    resolver: None,
                };
                let destination = ImageStore {
                    name: name.clone(),
                    platforms: vec![self.platform.clone()],
                    unpacks: vec![UnpackConfiguration {
                        platform: Some(self.platform.clone()),
                        snapshotter: self.settings.snapshotter.clone(),
                    }],
                    ..Default::default()
                };
                TransferClient::new(self.channel.clone())
                    .transfer(self.request(TransferRequest {
                        source: Some(containerd_client::to_any(&source)),
                        destination: Some(containerd_client::to_any(&destination)),
                        options: None,
                    }))
                    .await
                    .map_err(|status| image_error(&name, status.message()))?;
                Ok(())
            }
        }
    }

    async fn start(&self, spec: &ContainerSpec) -> Result<StartedContainer, RuntimeError> {
        let image = reference::normalize(&spec.image);
        self.remove(&spec.id).await?;
        let started = self.create_and_start(spec, &image).await;
        match started {
            Ok(container) => Ok(container),
            Err(error) => {
                let _ = self.remove(&spec.id).await;
                Err(error)
            }
        }
    }

    async fn stop(&self, id: &str, grace: Duration) -> Result<(), RuntimeError> {
        let exited = self.signal_and_wait(id, SIGTERM, grace).await?;
        match exited {
            true => Ok(()),
            false => self
                .signal_and_wait(id, SIGKILL, KILL_WAIT)
                .await
                .map(|_| ()),
        }
    }

    async fn remove(&self, id: &str) -> Result<(), RuntimeError> {
        let netns = self
            .task_pid(id)
            .await?
            .map(|pid| format!("/proc/{pid}/ns/net"))
            .unwrap_or_default();
        let task_deleted = TasksClient::new(self.channel.clone())
            .delete(self.request(DeleteTaskRequest {
                container_id: id.to_string(),
            }))
            .await;
        let deleted_while_running = task_deleted
            .as_ref()
            .is_err_and(|status| status.code() == Code::FailedPrecondition);
        match deleted_while_running {
            true => {
                self.stop(id, Duration::ZERO).await?;
                ignore_not_found(
                    TasksClient::new(self.channel.clone())
                        .delete(self.request(DeleteTaskRequest {
                            container_id: id.to_string(),
                        }))
                        .await
                        .map(|_| ()),
                )?;
            }
            false => ignore_not_found(task_deleted.map(|_| ()))?,
        }
        self.network.del(id, &netns).await?;
        ignore_not_found(
            ContainersClient::new(self.channel.clone())
                .delete(self.request(DeleteContainerRequest { id: id.to_string() }))
                .await
                .map(|_| ()),
        )?;
        ignore_not_found(
            SnapshotsClient::new(self.channel.clone())
                .remove(self.request(RemoveSnapshotRequest {
                    snapshotter: self.settings.snapshotter.clone(),
                    key: id.to_string(),
                }))
                .await
                .map(|_| ()),
        )?;
        match tokio::fs::remove_dir_all(self.settings.state_dir.join("containers").join(id)).await {
            Err(error) if error.kind() != std::io::ErrorKind::NotFound => Err(error.into()),
            _ => Ok(()),
        }
    }

    async fn list(&self) -> Result<Vec<ContainerStatus>, RuntimeError> {
        let containers = ContainersClient::new(self.channel.clone())
            .list(self.request(ListContainersRequest { filters: vec![] }))
            .await?
            .into_inner()
            .containers;
        let mut statuses = Vec::with_capacity(containers.len());
        for container in containers {
            let response = TasksClient::new(self.channel.clone())
                .get(self.request(GetRequest {
                    container_id: container.id.clone(),
                    exec_id: String::new(),
                }))
                .await;
            let process = ignore_not_found(response.map(|response| response.into_inner().process))?;
            statuses.push(ContainerStatus {
                id: container.id,
                labels: container.labels.into_iter().collect(),
                state: process.map_or(ContainerState::Created, |process| {
                    task_state(process.status, process.exit_status)
                }),
            });
        }
        Ok(statuses)
    }

    async fn events(&self) -> Result<mpsc::Receiver<RuntimeEvent>, RuntimeError> {
        let filters = vec![
            format!(
                "topic==\"{EXIT_TOPIC}\",namespace==\"{}\"",
                self.settings.namespace
            ),
            format!(
                "topic==\"{OOM_TOPIC}\",namespace==\"{}\"",
                self.settings.namespace
            ),
        ];
        let channel = self.channel.clone();
        let (sender, receiver) = mpsc::channel(EVENT_BUFFER);
        tokio::spawn(async move {
            while !sender.is_closed() {
                let subscribed = EventsClient::new(channel.clone())
                    .subscribe(SubscribeRequest {
                        filters: filters.clone(),
                    })
                    .await;
                if let Ok(response) = subscribed {
                    forward_events(response.into_inner(), &sender).await;
                }
                tokio::time::sleep(EVENT_RESUBSCRIBE_DELAY).await;
            }
        });
        Ok(receiver)
    }
}

async fn forward_events(
    mut stream: containerd_client::tonic::codec::Streaming<containerd_client::types::Envelope>,
    sender: &mpsc::Sender<RuntimeEvent>,
) {
    while let Ok(Some(envelope)) = stream.message().await {
        let event = envelope
            .event
            .and_then(|event| decode_event(&envelope.topic, &event.value));
        let delivered = match event {
            Some(event) => sender.send(event).await.is_ok(),
            None => true,
        };
        if !delivered {
            break;
        }
    }
}

fn decode_event(topic: &str, payload: &[u8]) -> Option<RuntimeEvent> {
    match topic {
        EXIT_TOPIC => containerd_client::events::TaskExit::decode(payload)
            .ok()
            .filter(|exit| exit.id == exit.container_id)
            .map(|exit| RuntimeEvent::Exited {
                id: exit.container_id,
                exit_code: exit.exit_status,
            }),
        OOM_TOPIC => containerd_client::events::TaskOom::decode(payload)
            .ok()
            .map(|oom| RuntimeEvent::OomKilled {
                id: oom.container_id,
            }),
        _ => None,
    }
}

fn task_state(status: i32, exit_status: u32) -> ContainerState {
    match containerd_client::types::v1::Status::try_from(status) {
        Ok(containerd_client::types::v1::Status::Created) => ContainerState::Created,
        Ok(containerd_client::types::v1::Status::Running) => ContainerState::Running,
        Ok(containerd_client::types::v1::Status::Stopped) => ContainerState::Stopped {
            exit_code: exit_status,
        },
        _ => ContainerState::Unknown,
    }
}

async fn write_hosts(path: &Path, hostname: &str, ip: Option<IpAddr>) -> Result<(), RuntimeError> {
    let own_entry = ip
        .map(|ip| format!("{ip}\t{hostname}\n"))
        .unwrap_or_default();
    let contents = format!("127.0.0.1\tlocalhost\n::1\tlocalhost ip6-localhost\n{own_entry}");
    Ok(tokio::fs::write(path, contents).await?)
}

fn chain_id(diff_ids: &[String]) -> Option<String> {
    let (first, rest) = diff_ids.split_first()?;
    Some(rest.iter().fold(first.clone(), |parent, diff_id| {
        let digest = Sha256::digest(format!("{parent} {diff_id}").as_bytes());
        format!("sha256:{}", hex::encode(digest))
    }))
}

fn host_platform() -> Platform {
    let (architecture, variant) = match std::env::consts::ARCH {
        "x86_64" => ("amd64", ""),
        "aarch64" => ("arm64", "v8"),
        other => (other, ""),
    };
    Platform {
        os: "linux".to_string(),
        architecture: architecture.to_string(),
        variant: variant.to_string(),
        os_version: String::new(),
    }
}

fn matches_platform(candidate: &ManifestPlatform, host: &Platform) -> bool {
    let variant_matches = candidate
        .variant
        .as_deref()
        .is_none_or(|variant| variant.is_empty() || variant == host.variant);
    candidate.os == host.os && candidate.architecture == host.architecture && variant_matches
}

fn file_uri(path: &Path) -> String {
    format!("file://{}", path.display())
}

fn image_error(image: &str, reason: &str) -> RuntimeError {
    RuntimeError::Image {
        image: image.to_string(),
        reason: reason.to_string(),
    }
}

fn ignore_not_found<T: Default>(result: Result<T, Status>) -> Result<T, RuntimeError> {
    match result {
        Err(status) if status.code() == Code::NotFound => Ok(T::default()),
        other => other.map_err(RuntimeError::from),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn computes_chain_id_like_containerd() {
        let diff_ids = vec!["sha256:aaa".to_string(), "sha256:bbb".to_string()];
        let expected_second = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(b"sha256:aaa sha256:bbb"))
        );
        assert_eq!(chain_id(&diff_ids[..1]), Some("sha256:aaa".to_string()));
        assert_eq!(chain_id(&diff_ids), Some(expected_second));
        assert_eq!(chain_id(&[]), None);
    }

    #[test]
    fn decodes_init_process_exit_only() {
        let init = containerd_client::events::TaskExit {
            container_id: "c1".to_string(),
            id: "c1".to_string(),
            exit_status: 137,
            ..Default::default()
        };
        let exec = containerd_client::events::TaskExit {
            container_id: "c1".to_string(),
            id: "exec-1".to_string(),
            ..Default::default()
        };
        assert_eq!(
            decode_event(EXIT_TOPIC, &init.encode_to_vec()),
            Some(RuntimeEvent::Exited {
                id: "c1".to_string(),
                exit_code: 137
            })
        );
        assert_eq!(decode_event(EXIT_TOPIC, &exec.encode_to_vec()), None);
    }

    #[test]
    fn matches_platform_variants() {
        let host = Platform {
            os: "linux".to_string(),
            architecture: "arm64".to_string(),
            variant: "v8".to_string(),
            os_version: String::new(),
        };
        let candidate = |variant: Option<&str>| ManifestPlatform {
            os: "linux".to_string(),
            architecture: "arm64".to_string(),
            variant: variant.map(str::to_string),
        };
        assert!(matches_platform(&candidate(None), &host));
        assert!(matches_platform(&candidate(Some("v8")), &host));
        assert!(!matches_platform(&candidate(Some("v7")), &host));
    }
}
