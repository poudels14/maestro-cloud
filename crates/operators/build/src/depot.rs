use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::SecretValue;
use runtime::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactByteStream,
    ArtifactDigest, ArtifactReference, ArtifactStore, ArtifactStoreError,
    DiscardArtifactBuildOutput,
    build_context::{BuildContext as DepotBuildContext, prepare_context},
    forward_artifact_build_output,
};

const FILE_CHUNK_BYTES: usize = 64 * 1_024;
const MAX_METADATA_BYTES: u64 = 1024 * 1024;

/// Node-local limits and credentials for Depot remote builds.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DepotBuildSettings {
    /// Depot CLI executable or command name.
    pub executable: PathBuf,
    /// Depot API token supplied only through `DEPOT_TOKEN`.
    pub token: SecretValue,
    /// Owner-only root for extracted contexts and downloaded image archives.
    pub state_root: PathBuf,
    /// Single target platform downloaded into the local runtime.
    pub platform: String,
    /// Maximum wall-clock duration of one remote build.
    pub build_timeout: Duration,
    /// Maximum expanded bytes accepted from an uploaded context.
    pub max_context_bytes: u64,
    /// Maximum entries accepted from an uploaded context.
    pub max_context_entries: usize,
    /// Maximum image archive bytes accepted from Depot.
    pub max_output_bytes: u64,
}

impl DepotBuildSettings {
    /// Creates production defaults for the host architecture.
    pub fn new(token: SecretValue, state_root: PathBuf) -> Self {
        Self {
            executable: PathBuf::from("depot"),
            token,
            state_root,
            platform: host_platform().to_owned(),
            build_timeout: Duration::from_secs(30 * 60),
            max_context_bytes: 4 * 1_024 * 1_024 * 1_024,
            max_context_entries: 100_000,
            max_output_bytes: 20 * 1_024 * 1_024 * 1_024,
        }
    }

    fn validate(&self) -> Result<(), ArtifactStoreError> {
        if self.executable.as_os_str().is_empty()
            || self.executable.as_os_str().as_encoded_bytes().contains(&0)
        {
            return Err(rejected(
                "Depot executable cannot be empty or contain a null byte",
            ));
        }
        if !self.state_root.is_absolute() || self.state_root.to_str().is_none() {
            return Err(rejected("Depot state root must be an absolute UTF-8 path"));
        }
        if self.token.expose().trim().is_empty() || self.token.expose().contains('\0') {
            return Err(rejected(
                "Depot token cannot be empty or contain a null byte",
            ));
        }
        if !matches!(self.platform.as_str(), "linux/amd64" | "linux/arm64") {
            return Err(rejected(
                "Depot platform must be `linux/amd64` or `linux/arm64`",
            ));
        }
        if self.build_timeout.is_zero()
            || self.max_context_bytes == 0
            || self.max_context_entries == 0
            || self.max_output_bytes == 0
        {
            return Err(rejected(
                "Depot build deadlines and limits must be positive",
            ));
        }
        Ok(())
    }
}

/// Protected remote-build seam selected by the Build reconciler.
#[async_trait]
pub trait DepotBuildBackend: Send + Sync {
    /// Builds with one service project and imports the result into local artifact storage.
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Builds and pushes directly from Depot to one registry destination.
    async fn build_and_publish(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        destination: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError>;

    /// Builds while forwarding Depot's native progress to the caller.
    async fn build_with_output(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let _ = output;
        self.build(request, project).await
    }

    /// Builds and pushes directly while forwarding Depot's native progress.
    async fn build_and_publish_with_output(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        destination: &ArtifactReference,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let _ = output;
        self.build_and_publish(request, project, destination).await
    }
}

/// Process-backed Depot CLI adapter.
pub struct ProcessDepotBuildBackend {
    settings: DepotBuildSettings,
    artifacts: Arc<dyn ArtifactStore>,
    runner: Arc<dyn DepotRunner>,
}

impl ProcessDepotBuildBackend {
    /// Creates a protected Depot process adapter after validating static settings.
    pub fn new(
        settings: DepotBuildSettings,
        artifacts: Arc<dyn ArtifactStore>,
    ) -> Result<Self, ArtifactStoreError> {
        Self::with_runner(settings, artifacts, Arc::new(ProcessDepotRunner))
    }

    pub(crate) fn with_runner(
        settings: DepotBuildSettings,
        artifacts: Arc<dyn ArtifactStore>,
        runner: Arc<dyn DepotRunner>,
    ) -> Result<Self, ArtifactStoreError> {
        settings.validate()?;
        Ok(Self {
            settings,
            artifacts,
            runner,
        })
    }

    async fn build_artifact(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        output_sink: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        validate_request(request, project)?;
        let workspace = create_workspace(&self.settings.state_root).await?;
        let context = prepare_context(
            &request.source,
            workspace.path(),
            self.settings.max_context_bytes,
            self.settings.max_context_entries,
            "Depot",
        )
        .await?;
        let output = workspace.path().join("image.docker.tar");
        create_private_output(&output).await?;
        let invocation = build_invocation(request, project, &self.settings, &context, &output)?;
        self.runner
            .run(invocation, self.settings.build_timeout, output_sink)
            .await?;
        validate_output(&output, self.settings.max_output_bytes).await?;
        self.artifacts
            .import(Box::new(DepotOutputStream {
                file: tokio::fs::File::open(&output)
                    .await
                    .map_err(|error| unavailable_io("open Depot image output", &output, error))?,
                _workspace: workspace,
            }))
            .await
    }

    async fn build_and_publish_artifact(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        destination: &ArtifactReference,
        output_sink: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        validate_request(request, project)?;
        let workspace = create_workspace(&self.settings.state_root).await?;
        let context = prepare_context(
            &request.source,
            workspace.path(),
            self.settings.max_context_bytes,
            self.settings.max_context_entries,
            "Depot",
        )
        .await?;
        let metadata = workspace.path().join("metadata.json");
        create_private_output(&metadata).await?;
        let invocation = publish_invocation(
            request,
            project,
            &self.settings,
            &context,
            destination,
            &metadata,
        )?;
        self.runner
            .run(invocation, self.settings.build_timeout, output_sink)
            .await?;
        published_digest(&metadata, destination).await
    }
}

#[async_trait]
impl DepotBuildBackend for ProcessDepotBuildBackend {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, project, &DiscardArtifactBuildOutput)
            .await
    }

    async fn build_with_output(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, project, output).await
    }

    async fn build_and_publish(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        destination: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_and_publish_artifact(request, project, destination, &DiscardArtifactBuildOutput)
            .await
    }

    async fn build_and_publish_with_output(
        &self,
        request: &ArtifactBuildRequest,
        project: &str,
        destination: &ArtifactReference,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_and_publish_artifact(request, project, destination, output)
            .await
    }
}

pub(crate) struct DepotInvocation {
    pub(crate) executable: PathBuf,
    pub(crate) arguments: Vec<OsString>,
    pub(crate) directory: PathBuf,
    pub(crate) environment: Vec<(OsString, SecretValue)>,
    pub(crate) redactions: Vec<SecretValue>,
    pub(crate) result_path: PathBuf,
}

#[async_trait]
pub(crate) trait DepotRunner: Send + Sync {
    async fn run(
        &self,
        invocation: DepotInvocation,
        timeout: Duration,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<(), ArtifactStoreError>;
}

struct ProcessDepotRunner;

#[async_trait]
impl DepotRunner for ProcessDepotRunner {
    async fn run(
        &self,
        invocation: DepotInvocation,
        timeout: Duration,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<(), ArtifactStoreError> {
        let mut command = tokio::process::Command::new(&invocation.executable);
        command
            .args(&invocation.arguments)
            .current_dir(&invocation.directory)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        for (name, value) in &invocation.environment {
            command.env(name, value.expose());
        }
        let mut child = command
            .spawn()
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!(
                    "start Depot client `{}`: {error}",
                    invocation.executable.display()
                ),
            })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: "Depot stdout pipe was not created".to_owned(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: "Depot stderr pipe was not created".to_owned(),
            })?;
        let completion = async {
            tokio::try_join!(
                async {
                    child
                        .wait()
                        .await
                        .map_err(|error| ArtifactStoreError::Unavailable {
                            message: format!("wait for Depot client: {error}"),
                        })
                },
                forward_artifact_build_output(
                    stdout,
                    ArtifactBuildOutputStream::Stdout,
                    output,
                    &invocation.redactions,
                    "Depot stdout",
                ),
                forward_artifact_build_output(
                    stderr,
                    ArtifactBuildOutputStream::Stderr,
                    output,
                    &invocation.redactions,
                    "Depot stderr",
                )
            )
        };
        let status = match tokio::time::timeout(timeout, completion).await {
            Ok(Ok((status, (), ()))) => status,
            Ok(Err(error)) => {
                let _ignored = child.kill().await;
                let _ignored = child.wait().await;
                return Err(error);
            }
            Err(_) => {
                let _ignored = child.kill().await;
                let _ignored = child.wait().await;
                return Err(ArtifactStoreError::Unavailable {
                    message: format!("Depot build exceeded its {}s deadline", timeout.as_secs()),
                });
            }
        };
        if status.success() {
            Ok(())
        } else {
            Err(rejected(format!(
                "Depot build producing `{}` failed with status {status}",
                invocation.result_path.display()
            )))
        }
    }
}

fn build_invocation(
    request: &ArtifactBuildRequest,
    project: &str,
    settings: &DepotBuildSettings,
    context: &DepotBuildContext,
    output: &Path,
) -> Result<DepotInvocation, ArtifactStoreError> {
    let output = path_text(output, "Depot output")?;
    let mut invocation = common_invocation(
        request,
        project,
        settings,
        context,
        "maestro.local/builds/output:latest",
    )?;
    invocation.arguments.extend([
        OsString::from("--output"),
        OsString::from(format!("type=docker,dest={output}")),
        OsString::from("."),
    ]);
    invocation.result_path = PathBuf::from(output);
    Ok(invocation)
}

fn common_invocation(
    request: &ArtifactBuildRequest,
    project: &str,
    settings: &DepotBuildSettings,
    context: &DepotBuildContext,
    tag: &str,
) -> Result<DepotInvocation, ArtifactStoreError> {
    let mut arguments = vec![
        OsString::from("build"),
        OsString::from("--project"),
        OsString::from(project),
        OsString::from("--platform"),
        OsString::from(&settings.platform),
        OsString::from("--progress"),
        OsString::from("plain"),
        OsString::from("--file"),
        OsString::from(&context.definition),
        OsString::from("--tag"),
        OsString::from(tag),
    ];
    for (key, value) in &request.arguments {
        arguments.push(OsString::from("--build-arg"));
        arguments.push(OsString::from(format!("{key}={}", value.expose())));
    }
    let mut environment = vec![
        (OsString::from("DEPOT_TOKEN"), settings.token.clone()),
        (
            OsString::from("DEPOT_NO_SUMMARY_LINK"),
            SecretValue::new("1"),
        ),
        (
            OsString::from("DEPOT_NO_UPDATE_NOTIFIER"),
            SecretValue::new("1"),
        ),
    ];
    for (index, (key, value)) in request.secrets.iter().enumerate() {
        let variable = format!("MAESTRO_DEPOT_BUILD_SECRET_{index}");
        arguments.push(OsString::from("--secret"));
        arguments.push(OsString::from(format!("id={key},env={variable}")));
        environment.push((OsString::from(variable), value.clone()));
    }
    Ok(DepotInvocation {
        executable: settings.executable.clone(),
        arguments,
        directory: context.root.clone(),
        environment,
        redactions: std::iter::once(settings.token.clone())
            .chain(request.secrets.values().cloned())
            .collect(),
        result_path: PathBuf::new(),
    })
}

fn publish_invocation(
    request: &ArtifactBuildRequest,
    project: &str,
    settings: &DepotBuildSettings,
    context: &DepotBuildContext,
    destination: &ArtifactReference,
    metadata: &Path,
) -> Result<DepotInvocation, ArtifactStoreError> {
    let metadata = path_text(metadata, "Depot metadata output")?;
    let mut invocation =
        common_invocation(request, project, settings, context, destination.as_str())?;
    invocation.arguments.extend([
        OsString::from("--metadata-file"),
        OsString::from(metadata),
        OsString::from("--push"),
        OsString::from("."),
    ]);
    invocation.result_path = PathBuf::from(metadata);
    Ok(invocation)
}

fn validate_request(
    request: &ArtifactBuildRequest,
    project: &str,
) -> Result<(), ArtifactStoreError> {
    if project.trim().is_empty()
        || project != project.trim()
        || project.chars().any(char::is_whitespace)
        || project.chars().any(char::is_control)
    {
        return Err(rejected(
            "Depot project must be non-empty and contain no whitespace or control characters",
        ));
    }
    for (key, value) in &request.arguments {
        validate_key(DepotKeyKind::Argument, key)?;
        if value.expose().contains('\0') {
            return Err(rejected(format!(
                "Depot argument `{key}` contains a null byte"
            )));
        }
    }
    for (key, value) in &request.secrets {
        validate_key(DepotKeyKind::Secret, key)?;
        if value.expose().contains('\0') {
            return Err(rejected(format!(
                "Depot secret `{key}` contains a null byte"
            )));
        }
    }
    if !request.tags.is_empty() {
        return Err(rejected(
            "Depot builds assign references only after local import",
        ));
    }
    Ok(())
}

#[derive(Clone, Copy)]
pub(crate) enum DepotKeyKind {
    Argument,
    Secret,
}

pub(crate) fn validate_key(kind: DepotKeyKind, key: &str) -> Result<(), ArtifactStoreError> {
    let (kind_name, invalid_delimiter) = match kind {
        DepotKeyKind::Argument => ("argument", key.contains('=')),
        DepotKeyKind::Secret => ("secret", key.contains('=') || key.contains(',')),
    };
    if key.is_empty() || invalid_delimiter || key.chars().any(char::is_control) {
        Err(rejected(format!(
            "Depot {kind_name} name `{key}` is invalid"
        )))
    } else {
        Ok(())
    }
}

async fn create_workspace(state_root: &Path) -> Result<tempfile::TempDir, ArtifactStoreError> {
    let root = state_root.join("builds");
    tokio::task::spawn_blocking(move || {
        std::fs::create_dir_all(&root).map_err(|error| unavailable_io("create", &root, error))?;
        let metadata = std::fs::symlink_metadata(&root)
            .map_err(|error| unavailable_io("inspect", &root, error))?;
        let canonical = std::fs::canonicalize(&root)
            .map_err(|error| unavailable_io("resolve", &root, error))?;
        if !metadata.file_type().is_dir() || canonical != root {
            return Err(rejected(format!(
                "Depot workspace root `{}` must be a real directory",
                root.display()
            )));
        }
        std::fs::set_permissions(&root, std::fs::Permissions::from_mode(0o700))
            .map_err(|error| unavailable_io("protect", &root, error))?;
        tempfile::Builder::new()
            .prefix("build-")
            .tempdir_in(&root)
            .map_err(|error| unavailable_io("create temporary workspace in", &root, error))
    })
    .await
    .map_err(|error| ArtifactStoreError::Unavailable {
        message: format!("Depot workspace preparation stopped unexpectedly: {error}"),
    })?
}

async fn create_private_output(path: &Path) -> Result<(), ArtifactStoreError> {
    tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await
        .map(|_| ())
        .map_err(|error| unavailable_io("create private Depot output", path, error))
}

async fn validate_output(path: &Path, max_bytes: u64) -> Result<(), ArtifactStoreError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| unavailable_io("inspect Depot output", path, error))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(ArtifactStoreError::Unavailable {
            message: format!(
                "Depot output must be a non-empty regular file no larger than {max_bytes} bytes"
            ),
        });
    }
    Ok(())
}

async fn published_digest(
    metadata: &Path,
    destination: &ArtifactReference,
) -> Result<ArtifactDigest, ArtifactStoreError> {
    validate_output(metadata, MAX_METADATA_BYTES).await?;
    let encoded = tokio::fs::read(metadata)
        .await
        .map_err(|error| unavailable_io("read Depot metadata", metadata, error))?;
    let document: serde_json::Value = serde_json::from_slice(&encoded)
        .map_err(|_| rejected("Depot metadata is not valid JSON"))?;
    let digest = document
        .get("containerimage.digest")
        .and_then(serde_json::Value::as_str)
        .ok_or_else(|| rejected("Depot metadata omitted `containerimage.digest`"))?;
    let encoded_digest = digest
        .strip_prefix("sha256:")
        .filter(|digest| digest.len() == 64 && digest.bytes().all(|byte| byte.is_ascii_hexdigit()))
        .ok_or_else(|| rejected("Depot metadata returned an invalid image digest"))?;
    ArtifactDigest::new(format!("sha256:{}", encoded_digest.to_ascii_lowercase()))?
        .for_reference(destination)
}

fn path_text<'a>(path: &'a Path, kind: &str) -> Result<&'a str, ArtifactStoreError> {
    path.to_str()
        .ok_or_else(|| rejected(format!("{kind} must be valid UTF-8")))
}

fn host_platform() -> &'static str {
    match std::env::consts::ARCH {
        "aarch64" => "linux/arm64",
        "x86_64" => "linux/amd64",
        _ => "unsupported",
    }
}

fn unavailable_io(operation: &str, path: &Path, error: std::io::Error) -> ArtifactStoreError {
    ArtifactStoreError::Unavailable {
        message: format!("{operation} `{}`: {error}", path.display()),
    }
}

fn rejected(message: impl Into<String>) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: message.into(),
    }
}

struct DepotOutputStream {
    file: tokio::fs::File,
    _workspace: tempfile::TempDir,
}

#[async_trait]
impl ArtifactByteStream for DepotOutputStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        use tokio::io::AsyncReadExt;

        let mut chunk = vec![0_u8; FILE_CHUNK_BYTES];
        let count =
            self.file
                .read(&mut chunk)
                .await
                .map_err(|error| ArtifactStoreError::Stream {
                    message: format!("read Depot image output: {error}"),
                })?;
        if count == 0 {
            Ok(None)
        } else {
            chunk.truncate(count);
            Ok(Some(chunk))
        }
    }
}
