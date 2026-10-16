use std::collections::HashMap;
use std::net::IpAddr;
use std::path::{Path, PathBuf};
use std::time::Duration;

use containerd_client::services::v1::StreamInit;
use containerd_client::services::v1::container::Runtime as ContainerRuntime;
use containerd_client::services::v1::snapshots::{PrepareSnapshotRequest, RemoveSnapshotRequest};
use containerd_client::services::v1::streaming_client::StreamingClient;
use containerd_client::services::v1::{
    CloseIoRequest, Container, CreateContainerRequest, CreateTaskRequest, DeleteContainerRequest,
    DeleteImageRequest, DeleteProcessRequest, DeleteTaskRequest, ExecProcessRequest,
    GetContainerRequest, GetImageRequest, GetRequest, KillRequest, ListContainersRequest,
    ReadContentRequest, ResizePtyRequest, StartRequest, SubscribeRequest, TransferRequest,
    WaitRequest,
};
use containerd_client::services::v1::{
    containers_client::ContainersClient, content_client::ContentClient,
    events_client::EventsClient, images_client::ImagesClient,
    snapshots::snapshots_client::SnapshotsClient, tasks_client::TasksClient,
    transfer_client::TransferClient,
};
use containerd_client::tonic::transport::Channel;
use containerd_client::tonic::{Code, Request, Status};
use containerd_client::types::transfer::{
    AuthRequest, AuthResponse, AuthType, Data, ImageExportStream, ImageImportStream,
    ImageReference, ImageStore, OciRegistry, RegistryResolver, UnpackConfiguration, WindowUpdate,
};
use containerd_client::types::{Descriptor, Platform};
use prost::Message;
use prost_types::Any;
use serde::Deserialize;
use sha2::{Digest, Sha256};
use tokio::sync::mpsc;
use tokio_stream::wrappers::ReceiverStream;

use crate::cni::CniNetwork;
use crate::docker_auth::registry_credentials;
use crate::oci;
use crate::reference;
use crate::spec::{
    ContainerFiles, ContainerSpec, ContainerState, ContainerStatus, ExecRequest, ExecSession,
    ImageConfig, NetworkMode, Runtime, RuntimeError, RuntimeEvent, StartedContainer,
};
use crate::user::{AccountFiles, ProcessUser, numeric_user, read_account_files, resolve_user};

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
const STREAM_CHUNK: usize = 32 * 1024;
const PROCESS_TYPE_URL: &str = "types.containerd.io/opencontainers/runtime-spec/1/Process";
const STREAM_BUFFER: usize = 16;
const EXPORT_WINDOW: i64 = 64 << 20;
const EXPORT_DRAIN_IDLE: Duration = Duration::from_millis(500);
const DATA_TYPE: &str = "containerd.types.transfer.Data";
const WINDOW_TYPE: &str = "containerd.types.transfer.WindowUpdate";
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
    pub log_uri: Option<String>,
}

pub struct ContainerdRuntime {
    channel: Channel,
    settings: ContainerdSettings,
    network: CniNetwork,
    platform: Platform,
    users: std::sync::Mutex<HashMap<(String, String), ProcessUser>>,
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
    #[serde(default)]
    layers: Vec<ManifestConfig>,
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
            users: std::sync::Mutex::default(),
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

    async fn process_user(
        &self,
        image_user: Option<&str>,
        chain_id: &str,
        layers: &[String],
    ) -> Result<ProcessUser, RuntimeError> {
        let cache_key = (
            chain_id.to_string(),
            image_user.unwrap_or_default().to_string(),
        );
        let cached = self.lock_users().get(&cache_key).cloned();
        match (numeric_user(image_user), cached) {
            (Some(user), _) | (None, Some(user)) => Ok(user),
            (None, None) => {
                let mut files = AccountFiles::default();
                for layer in layers.iter().rev() {
                    if files.passwd.is_none() || files.group.is_none() {
                        let blob = self.read_blob(layer).await?;
                        files = tokio::task::spawn_blocking(move || {
                            read_account_files(&blob, &mut files).map(|()| files)
                        })
                        .await
                        .map_err(|error| RuntimeError::Io(std::io::Error::other(error)))??;
                    }
                }
                let user = resolve_user(image_user.unwrap_or_default(), &files)?;
                self.lock_users().insert(cache_key, user.clone());
                Ok(user)
            }
        }
    }

    fn lock_users(&self) -> std::sync::MutexGuard<'_, HashMap<(String, String), ProcessUser>> {
        self.users
            .lock()
            .unwrap_or_else(|poisoned| poisoned.into_inner())
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

    async fn image_config(
        &self,
        image: &str,
    ) -> Result<(ImageConfig, String, Vec<String>), RuntimeError> {
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
        let layers = manifest
            .layers
            .into_iter()
            .map(|layer| layer.digest)
            .collect();
        Ok((blob.config, chain_id, layers))
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
        let (image_config, chain_id, layers) = self.image_config(image).await?;
        let user = self
            .process_user(image_config.user.as_deref(), &chain_id, &layers)
            .await?;
        let files = self.write_container_files(&spec.id).await?;
        let oci_spec = oci::build_spec(spec, &image_config, &files, &user);
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
        let (stdout, stderr) = match &self.settings.log_uri {
            Some(uri) => (uri.clone(), uri.clone()),
            None => (
                file_uri(&log_dir.join("stdout.log")),
                file_uri(&log_dir.join("stderr.log")),
            ),
        };
        let pid = TasksClient::new(self.channel.clone())
            .create(self.request(CreateTaskRequest {
                container_id: spec.id.clone(),
                rootfs: mounts,
                stdout,
                stderr,
                ..Default::default()
            }))
            .await?
            .into_inner()
            .pid;
        let ip = match spec.network {
            NetworkMode::Bridge => {
                self.network
                    .add(&spec.id, &format!("/proc/{pid}/ns/net"))
                    .await?
            }
            NetworkMode::Host => None,
        };
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

impl ContainerdRuntime {
    pub async fn has_image(&self, image: &str) -> Result<bool, RuntimeError> {
        self.image_exists(&reference::normalize(image)).await
    }

    pub async fn remove_image(&self, image: &str) -> Result<(), RuntimeError> {
        ignore_not_found(
            ImagesClient::new(self.channel.clone())
                .delete(self.request(DeleteImageRequest {
                    name: reference::normalize(image),
                    sync: true,
                    target: None,
                }))
                .await
                .map(|_| ()),
        )
    }

    pub async fn export_image(
        &self,
        image: &str,
    ) -> Result<mpsc::Receiver<Result<Vec<u8>, RuntimeError>>, RuntimeError> {
        let name = reference::normalize(image);
        let stream_id = stream_id("export");
        let (outbound, mut inbound) = self.open_stream(&stream_id).await?;
        let (chunks, receiver) = mpsc::channel(STREAM_BUFFER);
        let source = ImageStore {
            name: name.clone(),
            ..Default::default()
        };
        let destination = ImageExportStream {
            stream: stream_id,
            platforms: vec![self.platform.clone()],
            ..Default::default()
        };
        let request = self.request(TransferRequest {
            source: Some(containerd_client::to_any(&source)),
            destination: Some(containerd_client::to_any(&destination)),
            options: None,
        });
        let mut client = TransferClient::new(self.channel.clone());
        let transfer_chunks = chunks.clone();
        let (done, mut transferred) = tokio::sync::oneshot::channel();
        tokio::spawn(async move {
            let result = client.transfer(request).await;
            if let Err(status) = &result {
                let _ = transfer_chunks
                    .send(Err(image_error(&name, status.message())))
                    .await;
            }
            let _ = done.send(result.is_ok());
        });
        tokio::spawn(async move {
            let mut window = 0i64;
            let mut finished = false;
            loop {
                if window < EXPORT_WINDOW / 2 {
                    let update = WindowUpdate {
                        update: EXPORT_WINDOW as i32,
                    };
                    if outbound
                        .send(containerd_client::to_any(&update))
                        .await
                        .is_ok()
                    {
                        window += EXPORT_WINDOW;
                    }
                }
                let next = tokio::select! {
                    message = inbound.message() => message,
                    succeeded = &mut transferred, if !finished => match succeeded {
                        Ok(true) => {
                            finished = true;
                            continue;
                        }
                        _ => break,
                    },
                    _ = tokio::time::sleep(EXPORT_DRAIN_IDLE), if finished => break,
                };
                match next {
                    Ok(Some(message)) if message.type_url.ends_with(DATA_TYPE) => {
                        if let Ok(data) = Data::decode(message.value.as_slice()) {
                            window -= data.data.len() as i64;
                            if chunks.send(Ok(data.data)).await.is_err() {
                                break;
                            }
                        }
                    }
                    Ok(Some(_)) => {}
                    Ok(None) => break,
                    Err(status) => {
                        let _ = chunks.send(Err(status.into())).await;
                        break;
                    }
                }
            }
        });
        Ok(receiver)
    }

    pub async fn import_image(
        &self,
        image: &str,
        mut chunks: mpsc::Receiver<Vec<u8>>,
    ) -> Result<(), RuntimeError> {
        let name = reference::normalize(image);
        let repository = image_repository(&name);
        let stream_id = stream_id("import");
        let (outbound, mut inbound) = self.open_stream(&stream_id).await?;
        let sender = tokio::spawn(async move {
            let mut window = 0i64;
            let mut pending = Vec::new();
            let mut offset = 0usize;
            let mut source_open = true;
            loop {
                if offset == pending.len() && source_open {
                    match chunks.recv().await {
                        Some(chunk) => {
                            pending = chunk;
                            offset = 0;
                        }
                        None => source_open = false,
                    }
                }
                if offset == pending.len() && !source_open {
                    break;
                }
                while window <= 0 {
                    match inbound.message().await {
                        Ok(Some(message)) if message.type_url.ends_with(WINDOW_TYPE) => {
                            window += WindowUpdate::decode(message.value.as_slice())
                                .map_or(0, |update| update.update as i64);
                        }
                        Ok(Some(_)) => {}
                        _ => return,
                    }
                }
                let length = (pending.len() - offset)
                    .min(STREAM_CHUNK)
                    .min(window as usize);
                let data = Data {
                    data: pending[offset..offset + length].to_vec(),
                };
                if outbound
                    .send(containerd_client::to_any(&data))
                    .await
                    .is_err()
                {
                    return;
                }
                offset += length;
                window -= length as i64;
            }
            drop(outbound);
            while let Ok(Some(_)) = inbound.message().await {}
        });
        let source = ImageImportStream {
            stream: stream_id,
            ..Default::default()
        };
        let destination = ImageStore {
            name: name.clone(),
            platforms: vec![self.platform.clone()],
            extra_references: vec![ImageReference {
                name: repository,
                is_prefix: true,
                ..Default::default()
            }],
            unpacks: vec![UnpackConfiguration {
                platform: Some(self.platform.clone()),
                snapshotter: self.settings.snapshotter.clone(),
            }],
            ..Default::default()
        };
        let transferred = TransferClient::new(self.channel.clone())
            .transfer(self.request(TransferRequest {
                source: Some(containerd_client::to_any(&source)),
                destination: Some(containerd_client::to_any(&destination)),
                options: None,
            }))
            .await;
        sender.abort();
        transferred.map_err(|status| image_error(&name, status.message()))?;
        Ok(())
    }

    async fn open_stream(
        &self,
        stream_id: &str,
    ) -> Result<(mpsc::Sender<Any>, containerd_client::tonic::Streaming<Any>), RuntimeError> {
        let (outbound, outbound_receiver) = mpsc::channel(STREAM_BUFFER);
        outbound
            .send(containerd_client::to_any(&StreamInit {
                id: stream_id.to_string(),
            }))
            .await
            .map_err(|_| RuntimeError::Connect("stream closed before init".to_string()))?;
        let mut inbound = StreamingClient::new(self.channel.clone())
            .stream(self.request(ReceiverStream::new(outbound_receiver)))
            .await?
            .into_inner();
        inbound.message().await?;
        Ok((outbound, inbound))
    }
}

impl ContainerdRuntime {
    pub async fn exec(&self, request: ExecRequest) -> Result<ExecSession, RuntimeError> {
        let exec_id = stream_id("exec");
        let directory = self.settings.state_dir.join("exec").join(&exec_id);
        tokio::fs::create_dir_all(&directory).await?;
        let stdin_path = directory.join("stdin");
        let stdout_path = directory.join("stdout");
        let stderr_path = directory.join("stderr");
        let fifos = match request.tty {
            true => vec![&stdin_path, &stdout_path],
            false => vec![&stdin_path, &stdout_path, &stderr_path],
        };
        fifos.into_iter().try_for_each(|path| make_fifo(path))?;
        let process = self.exec_process(&request).await?;
        let stdin = open_fifo_sender(&stdin_path)?;
        let (output_sender, output) = mpsc::channel(STREAM_BUFFER);
        let readers = match request.tty {
            true => vec![&stdout_path],
            false => vec![&stdout_path, &stderr_path],
        };
        for path in readers {
            let receiver = open_fifo_receiver(path)?;
            tokio::spawn(forward_output(receiver, output_sender.clone()));
        }
        let mut tasks = TasksClient::new(self.channel.clone());
        tasks
            .exec(self.request(ExecProcessRequest {
                container_id: request.container_id.clone(),
                stdin: stdin_path.to_string_lossy().to_string(),
                stdout: stdout_path.to_string_lossy().to_string(),
                stderr: match request.tty {
                    true => String::new(),
                    false => stderr_path.to_string_lossy().to_string(),
                },
                terminal: request.tty,
                spec: Some(process),
                exec_id: exec_id.clone(),
            }))
            .await?;
        let (exit_sender, exit) = tokio::sync::oneshot::channel();
        let mut waiter = TasksClient::new(self.channel.clone());
        let wait_request = self.request(WaitRequest {
            container_id: request.container_id.clone(),
            exec_id: exec_id.clone(),
        });
        tokio::spawn(async move {
            let status = waiter
                .wait(wait_request)
                .await
                .map(|response| response.into_inner().exit_status)
                .map_err(RuntimeError::from);
            let _ = exit_sender.send(status);
        });
        tasks
            .start(self.request(StartRequest {
                container_id: request.container_id.clone(),
                exec_id: exec_id.clone(),
            }))
            .await?;
        if let Some((cols, rows)) = request.size {
            self.resize_exec(&request.container_id, &exec_id, cols, rows)
                .await?;
        }
        Ok(ExecSession {
            container_id: request.container_id,
            exec_id,
            stdin: Some(stdin),
            output,
            exit,
        })
    }

    pub async fn resize_exec(
        &self,
        container_id: &str,
        exec_id: &str,
        cols: u16,
        rows: u16,
    ) -> Result<(), RuntimeError> {
        TasksClient::new(self.channel.clone())
            .resize_pty(self.request(ResizePtyRequest {
                container_id: container_id.to_string(),
                exec_id: exec_id.to_string(),
                width: cols.into(),
                height: rows.into(),
            }))
            .await?;
        Ok(())
    }

    pub async fn close_exec_stdin(
        &self,
        container_id: &str,
        exec_id: &str,
    ) -> Result<(), RuntimeError> {
        TasksClient::new(self.channel.clone())
            .close_io(self.request(CloseIoRequest {
                container_id: container_id.to_string(),
                exec_id: exec_id.to_string(),
                stdin: true,
            }))
            .await?;
        Ok(())
    }

    pub async fn remove_exec(&self, container_id: &str, exec_id: &str) {
        let mut tasks = TasksClient::new(self.channel.clone());
        let _ = tasks
            .kill(self.request(KillRequest {
                container_id: container_id.to_string(),
                exec_id: exec_id.to_string(),
                signal: SIGKILL,
                all: false,
            }))
            .await;
        let _ = tasks
            .delete_process(self.request(DeleteProcessRequest {
                container_id: container_id.to_string(),
                exec_id: exec_id.to_string(),
            }))
            .await;
        let _ = tokio::fs::remove_dir_all(self.settings.state_dir.join("exec").join(exec_id)).await;
    }

    async fn exec_process(&self, request: &ExecRequest) -> Result<Any, RuntimeError> {
        let container = ContainersClient::new(self.channel.clone())
            .get(self.request(GetContainerRequest {
                id: request.container_id.clone(),
            }))
            .await?
            .into_inner()
            .container
            .ok_or_else(|| RuntimeError::NotFound(request.container_id.clone()))?;
        let spec: serde_json::Value = serde_json::from_slice(
            &container
                .spec
                .ok_or_else(|| RuntimeError::NotFound(request.container_id.clone()))?
                .value,
        )?;
        let mut process = spec["process"].clone();
        process["args"] = serde_json::json!(request.argv);
        process["terminal"] = serde_json::json!(request.tty);
        if let Some(term) = request.term.as_ref().filter(|_| request.tty) {
            process["env"] = serde_json::json!(with_term(&process["env"], term));
        }
        Ok(Any {
            type_url: PROCESS_TYPE_URL.to_string(),
            value: serde_json::to_vec(&process)?,
        })
    }
}

impl Runtime for ContainerdRuntime {
    async fn pull(&self, image: &str) -> Result<(), RuntimeError> {
        let name = reference::normalize(image);
        match self.image_exists(&name).await? {
            true => Ok(()),
            false => {
                let auth_stream = stream_id("auth");
                let (outbound, inbound) = self.open_stream(&auth_stream).await?;
                let responder = tokio::spawn(answer_auth_requests(outbound, inbound));
                let source = OciRegistry {
                    reference: name.clone(),
                    resolver: Some(RegistryResolver {
                        auth_stream,
                        default_scheme: registry_scheme(&name).to_string(),
                        ..Default::default()
                    }),
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
                let transferred = TransferClient::new(self.channel.clone())
                    .transfer(self.request(TransferRequest {
                        source: Some(containerd_client::to_any(&source)),
                        destination: Some(containerd_client::to_any(&destination)),
                        options: None,
                    }))
                    .await;
                responder.abort();
                transferred
                    .map(|_| ())
                    .map_err(|status| image_error(&name, status.message()))
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
            let ip = self.network.cached_ip(&container.id).await;
            statuses.push(ContainerStatus {
                id: container.id,
                labels: container.labels.into_iter().collect(),
                state: process.map_or(ContainerState::Created, |process| {
                    task_state(process.status, process.exit_status)
                }),
                ip,
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

#[cfg(target_os = "linux")]
fn make_fifo(path: &Path) -> std::io::Result<()> {
    rustix::fs::mkfifoat(
        rustix::fs::CWD,
        path,
        rustix::fs::Mode::from_raw_mode(0o600),
    )
    .map_err(std::io::Error::from)
}

#[cfg(target_os = "linux")]
fn open_fifo_sender(path: &Path) -> std::io::Result<tokio::net::unix::pipe::Sender> {
    tokio::net::unix::pipe::OpenOptions::new()
        .read_write(true)
        .open_sender(path)
}

#[cfg(target_os = "linux")]
fn open_fifo_receiver(path: &Path) -> std::io::Result<tokio::net::unix::pipe::Receiver> {
    tokio::net::unix::pipe::OpenOptions::new()
        .read_write(true)
        .open_receiver(path)
}

#[cfg(not(target_os = "linux"))]
fn make_fifo(_path: &Path) -> std::io::Result<()> {
    Err(exec_unsupported())
}

#[cfg(not(target_os = "linux"))]
fn open_fifo_sender(_path: &Path) -> std::io::Result<tokio::net::unix::pipe::Sender> {
    Err(exec_unsupported())
}

#[cfg(not(target_os = "linux"))]
fn open_fifo_receiver(_path: &Path) -> std::io::Result<tokio::net::unix::pipe::Receiver> {
    Err(exec_unsupported())
}

#[cfg(not(target_os = "linux"))]
fn exec_unsupported() -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::Unsupported, "exec needs Linux")
}

async fn forward_output(
    mut receiver: tokio::net::unix::pipe::Receiver,
    output: mpsc::Sender<Vec<u8>>,
) {
    let mut buffer = vec![0u8; STREAM_CHUNK];
    while let Ok(read) = tokio::io::AsyncReadExt::read(&mut receiver, &mut buffer).await
        && read > 0
        && output.send(buffer[..read].to_vec()).await.is_ok()
    {}
}

fn registry_scheme(reference: &str) -> &'static str {
    let host = reference.split('/').next().unwrap_or_default();
    match host.split(':').next().unwrap_or_default() {
        "localhost" | "127.0.0.1" => "http",
        _ => "https",
    }
}

async fn answer_auth_requests(
    outbound: mpsc::Sender<Any>,
    mut inbound: containerd_client::tonic::Streaming<Any>,
) {
    while let Ok(Some(message)) = inbound.message().await {
        let host = AuthRequest::decode(message.value.as_slice())
            .map(|request| request.host)
            .unwrap_or_default();
        let response = match registry_credentials(&host).await {
            Ok(Some(credentials)) => AuthResponse {
                auth_type: AuthType::Credentials as i32,
                username: credentials.username,
                secret: credentials.secret,
                expire_at: None,
            },
            Ok(None) | Err(_) => AuthResponse::default(),
        };
        if outbound
            .send(containerd_client::to_any(&response))
            .await
            .is_err()
        {
            break;
        }
    }
}

fn stream_id(prefix: &str) -> String {
    let nanos = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map_or(0, |elapsed| elapsed.as_nanos());
    format!("maestro-{prefix}-{nanos}")
}

fn image_repository(name: &str) -> String {
    let without_digest = name.split('@').next().unwrap_or(name);
    let last_segment = without_digest.rfind('/').map_or(0, |index| index + 1);
    match without_digest[last_segment..].rfind(':') {
        Some(colon) => without_digest[..last_segment + colon].to_string(),
        None => without_digest.to_string(),
    }
}

fn image_error(image: &str, reason: &str) -> RuntimeError {
    RuntimeError::Image {
        image: image.to_string(),
        reason: reason.to_string(),
    }
}

fn with_term(env: &serde_json::Value, term: &str) -> Vec<String> {
    env.as_array()
        .into_iter()
        .flatten()
        .filter_map(|entry| entry.as_str())
        .filter(|entry| !entry.starts_with("TERM="))
        .map(str::to_string)
        .chain([format!("TERM={term}")])
        .collect()
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

    #[test]
    fn strips_tags_and_digests_from_image_names() {
        assert_eq!(
            image_repository("maestro.local/web:d1"),
            "maestro.local/web"
        );
        assert_eq!(
            image_repository("registry.local:5000/team/api:v2@sha256:abc"),
            "registry.local:5000/team/api"
        );
        assert_eq!(image_repository("localhost:5000/api"), "localhost:5000/api");
    }

    #[test]
    fn exec_term_replaces_the_image_term() {
        let env = serde_json::json!(["PATH=/bin", "TERM=dumb"]);
        assert_eq!(
            with_term(&env, "xterm-256color"),
            ["PATH=/bin", "TERM=xterm-256color"]
        );
        assert_eq!(with_term(&serde_json::Value::Null, "xterm"), ["TERM=xterm"]);
    }
}
