use std::ffi::OsString;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::SecretValue;

use crate::containerd_build_context::prepare_context;
use crate::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactStoreError, ContainerdRuntimeSettings,
};

const FILE_CHUNK_BYTES: usize = 64 * 1_024;

#[derive(Clone, Copy)]
enum BuildParameterKind {
    Argument,
    Secret,
}

#[derive(Debug)]
pub(crate) struct BuildctlInvocation {
    pub(crate) executable: PathBuf,
    pub(crate) address: String,
    pub(crate) arguments: Vec<OsString>,
    pub(crate) environment: Vec<(OsString, SecretValue)>,
    pub(crate) output: PathBuf,
}

#[async_trait]
pub(crate) trait BuildctlRunner: Send + Sync {
    async fn run(
        &self,
        invocation: BuildctlInvocation,
        timeout: Duration,
    ) -> Result<(), ArtifactStoreError>;
}

pub(crate) struct ProcessBuildctlRunner;

#[async_trait]
impl BuildctlRunner for ProcessBuildctlRunner {
    async fn run(
        &self,
        invocation: BuildctlInvocation,
        timeout: Duration,
    ) -> Result<(), ArtifactStoreError> {
        probe_buildkit(&invocation, timeout.min(Duration::from_secs(5))).await?;
        let mut command = tokio::process::Command::new(&invocation.executable);
        command
            .args(&invocation.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .kill_on_drop(true);
        for (name, value) in &invocation.environment {
            command.env(name, value.expose());
        }
        let mut child = command
            .spawn()
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!(
                    "start BuildKit client `{}`: {error}",
                    invocation.executable.display()
                ),
            })?;
        let status = match tokio::time::timeout(timeout, child.wait()).await {
            Ok(result) => result.map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("wait for BuildKit client: {error}"),
            })?,
            Err(_) => {
                let _ignored = child.kill().await;
                let _ignored = child.wait().await;
                return Err(ArtifactStoreError::Unavailable {
                    message: format!(
                        "BuildKit build exceeded its {}s deadline",
                        timeout.as_secs()
                    ),
                });
            }
        };
        if status.success() {
            Ok(())
        } else {
            Err(ArtifactStoreError::Rejected {
                message: format!(
                    "BuildKit rejected OCI output `{}` with status {status}",
                    invocation.output.display()
                ),
            })
        }
    }
}

#[derive(Debug)]
pub(crate) struct BuildOutput {
    archive: PathBuf,
    workspace: tempfile::TempDir,
}

impl BuildOutput {
    pub(crate) async fn into_stream(
        self,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        let Self { archive, workspace } = self;
        let file = tokio::fs::File::open(&archive).await.map_err(|error| {
            ArtifactStoreError::Unavailable {
                message: format!("open BuildKit OCI output `{}`: {error}", archive.display()),
            }
        })?;
        Ok(Box::new(BuildOutputStream {
            file,
            _workspace: workspace,
        }))
    }
}

pub(crate) async fn run_build(
    request: &ArtifactBuildRequest,
    settings: &ContainerdRuntimeSettings,
    runner: Arc<dyn BuildctlRunner>,
) -> Result<BuildOutput, ArtifactStoreError> {
    validate_request(request)?;
    let workspace = create_workspace(&settings.state_root).await?;
    let context = prepare_context(
        &request.source,
        workspace.path(),
        settings.max_build_context_bytes,
        settings.max_build_context_entries,
    )
    .await?;
    let output = workspace.path().join("image.oci.tar");
    create_private_output(&output).await?;
    let invocation = build_invocation(request, settings, &context, output.clone())?;
    runner.run(invocation, settings.build_timeout).await?;
    validate_output(&output, settings.max_build_output_bytes).await?;
    Ok(BuildOutput {
        archive: output,
        workspace,
    })
}

fn build_invocation(
    request: &ArtifactBuildRequest,
    settings: &ContainerdRuntimeSettings,
    context: &crate::containerd_build_context::BuildContext,
    output: PathBuf,
) -> Result<BuildctlInvocation, ArtifactStoreError> {
    let root = path_text(&context.root, "BuildKit context")?;
    let destination = path_text(&output, "BuildKit output")?;
    let mut arguments = vec![
        OsString::from("--addr"),
        OsString::from(&settings.buildkit_address),
        OsString::from("build"),
        OsString::from("--progress"),
        OsString::from("plain"),
        OsString::from("--frontend"),
        OsString::from("dockerfile.v0"),
        OsString::from("--local"),
        OsString::from(format!("context={root}")),
        OsString::from("--local"),
        OsString::from(format!("dockerfile={root}")),
        OsString::from("--opt"),
        OsString::from(format!("filename={}", context.definition)),
    ];
    for (key, value) in &request.arguments {
        arguments.push(OsString::from("--opt"));
        arguments.push(OsString::from(format!("build-arg:{key}={value}")));
    }
    let mut environment = Vec::with_capacity(request.secrets.len());
    for (index, (key, value)) in request.secrets.iter().enumerate() {
        let variable = format!("MAESTRO_BUILDKIT_SECRET_{index}");
        arguments.push(OsString::from("--secret"));
        arguments.push(OsString::from(format!("id={key},type=env,env={variable}")));
        environment.push((OsString::from(variable), value.clone()));
    }
    arguments.push(OsString::from("--output"));
    arguments.push(OsString::from(format!(
        "type=oci,dest={destination},name=maestro.local/builds/output:latest"
    )));
    Ok(BuildctlInvocation {
        executable: settings.buildctl.clone(),
        address: settings.buildkit_address.clone(),
        arguments,
        environment,
        output,
    })
}

async fn probe_buildkit(
    invocation: &BuildctlInvocation,
    timeout: Duration,
) -> Result<(), ArtifactStoreError> {
    let mut command = tokio::process::Command::new(&invocation.executable);
    command
        .args([
            OsString::from("--addr"),
            OsString::from(&invocation.address),
            OsString::from("debug"),
            OsString::from("workers"),
        ])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let mut child = command
        .spawn()
        .map_err(|error| ArtifactStoreError::Unavailable {
            message: format!(
                "start BuildKit availability probe `{}`: {error}",
                invocation.executable.display()
            ),
        })?;
    let status = match tokio::time::timeout(timeout, child.wait()).await {
        Ok(result) => result.map_err(|error| ArtifactStoreError::Unavailable {
            message: format!("wait for BuildKit availability probe: {error}"),
        })?,
        Err(_) => {
            let _ignored = child.kill().await;
            let _ignored = child.wait().await;
            return Err(ArtifactStoreError::Unavailable {
                message: "BuildKit availability probe timed out".to_owned(),
            });
        }
    };
    if status.success() {
        Ok(())
    } else {
        Err(ArtifactStoreError::Unavailable {
            message: format!("BuildKit daemon is unavailable ({status})"),
        })
    }
}

fn validate_request(request: &ArtifactBuildRequest) -> Result<(), ArtifactStoreError> {
    for (key, value) in &request.arguments {
        validate_key(BuildParameterKind::Argument, key)?;
        if value.contains('\0') {
            return Err(rejected(format!(
                "BuildKit argument `{key}` contains a null byte"
            )));
        }
    }
    for (key, value) in &request.secrets {
        validate_key(BuildParameterKind::Secret, key)?;
        if value.expose().contains('\0') {
            return Err(rejected(format!(
                "BuildKit secret `{key}` contains a null byte"
            )));
        }
    }
    for tag in &request.tags {
        validate_tag(tag.as_str())?;
    }
    Ok(())
}

fn validate_key(kind: BuildParameterKind, key: &str) -> Result<(), ArtifactStoreError> {
    let (name, contains_reserved_delimiter) = match kind {
        BuildParameterKind::Argument => ("argument", false),
        BuildParameterKind::Secret => ("secret", key.contains(',')),
    };
    if key.is_empty()
        || key.contains('=')
        || contains_reserved_delimiter
        || key.chars().any(char::is_control)
    {
        Err(rejected(format!("BuildKit {name} name `{key}` is invalid")))
    } else {
        Ok(())
    }
}

pub(crate) fn validate_tag(tag: &str) -> Result<(), ArtifactStoreError> {
    let slash = tag.rfind('/');
    let colon = tag.rfind(':');
    let empty_tag_part = colon
        .filter(|colon| slash.is_none_or(|slash| *colon > slash))
        .is_some_and(|colon| colon == 0 || colon + 1 == tag.len());
    if tag.contains('@')
        || tag.chars().any(char::is_whitespace)
        || tag.chars().any(char::is_control)
        || tag.ends_with('/')
        || empty_tag_part
    {
        Err(rejected(format!(
            "BuildKit destination `{tag}` must be a non-whitespace repository tag"
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
            return Err(ArtifactStoreError::Rejected {
                message: format!(
                    "BuildKit workspace root `{}` must be a real directory",
                    root.display()
                ),
            });
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
        message: format!("BuildKit workspace preparation stopped unexpectedly: {error}"),
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
        .map_err(|error| unavailable_io("create private OCI output", path, error))
}

async fn validate_output(path: &Path, max_bytes: u64) -> Result<(), ArtifactStoreError> {
    let metadata = tokio::fs::metadata(path)
        .await
        .map_err(|error| unavailable_io("inspect OCI output", path, error))?;
    if !metadata.is_file() || metadata.len() == 0 || metadata.len() > max_bytes {
        return Err(ArtifactStoreError::Unavailable {
            message: format!(
                "BuildKit OCI output must be a non-empty regular file no larger than {max_bytes} bytes"
            ),
        });
    }
    Ok(())
}

fn path_text<'a>(path: &'a Path, kind: &str) -> Result<&'a str, ArtifactStoreError> {
    path.to_str()
        .ok_or_else(|| rejected(format!("{kind} must be valid UTF-8")))
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

struct BuildOutputStream {
    file: tokio::fs::File,
    _workspace: tempfile::TempDir,
}

#[async_trait]
impl ArtifactByteStream for BuildOutputStream {
    async fn next(&mut self) -> Result<Option<Vec<u8>>, ArtifactStoreError> {
        use tokio::io::AsyncReadExt;

        let mut chunk = vec![0_u8; FILE_CHUNK_BYTES];
        let count =
            self.file
                .read(&mut chunk)
                .await
                .map_err(|error| ArtifactStoreError::Stream {
                    message: format!("read BuildKit OCI output: {error}"),
                })?;
        if count == 0 {
            Ok(None)
        } else {
            chunk.truncate(count);
            Ok(Some(chunk))
        }
    }
}
