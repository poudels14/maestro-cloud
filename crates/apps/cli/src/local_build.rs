use std::collections::BTreeMap;
use std::future::Future;
use std::io::Write;
use std::path::PathBuf;
use std::sync::Arc;

use async_trait::async_trait;
use build::{
    BuildSourceProvider, DepotBuildBackend, DepotBuildSettings, LocalBuildSourceProvider,
    ProcessDepotBuildBackend,
};
use clap::ValueEnum;
use kernel_api::{BuildId, BuildSource, SecretValue};
use runtime::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactDigest,
    ArtifactReference, ArtifactSource, ArtifactStore, ArtifactStoreError,
};
use tokio::sync::mpsc;

use crate::CliError;

const OUTPUT_BUFFER_FRAMES: usize = 64;

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum Builder {
    /// Use Maestro's platform-native artifact backend.
    Native,
    /// Use Maestro's Depot backend and import the result locally.
    Depot,
}

pub(crate) struct Options {
    pub(crate) repository: String,
    pub(crate) revision: String,
    pub(crate) dockerfile: PathBuf,
    pub(crate) builder: Builder,
    pub(crate) depot_project: Option<String>,
    pub(crate) build_arguments: Vec<String>,
    pub(crate) secrets: Vec<String>,
    pub(crate) push: Option<String>,
}

pub(crate) async fn run(options: Options, output: &mut dyn Write) -> Result<(), CliError> {
    validate_options(&options)?;
    let push = options
        .push
        .as_deref()
        .map(ArtifactReference::new)
        .transpose()
        .map_err(|error| CliError::invalid_input(format!("invalid --push image: {error}")))?;
    let github_token = required_token("GH_TOKEN")?;
    let arguments = environment_values(&options.build_arguments, "build argument")?;
    let secrets = environment_values(&options.secrets, "build secret")?;
    let depot_token = match options.builder {
        Builder::Native => None,
        Builder::Depot => Some(required_token("DEPOT_TOKEN")?),
    };

    let temporary = tempfile::Builder::new()
        .prefix("maestro-local-build-")
        .tempdir()
        .map_err(|error| CliError::io("failed to create local build directory", error))?;
    let root = std::fs::canonicalize(temporary.path())
        .map_err(|error| CliError::io("failed to resolve local build directory", error))?;
    let source_provider =
        LocalBuildSourceProvider::new(root.join("sources"), root.join("archives"))
            .map_err(|error| build_error("source setup", error))?;
    let source = BuildSource::Git {
        repository: options.repository,
        revision: options.revision,
    };

    writeln!(
        output,
        "[maestro]: cloning source with isolated GH_TOKEN authentication"
    )
    .map_err(|error| CliError::io("failed to write local build output", error))?;
    let prepared = source_provider
        .prepare(
            &BuildId::new("local-build")
                .map_err(|error| CliError::invalid_input(error.to_string()))?,
            &source,
            None,
            Some(&github_token),
        )
        .await
        .map_err(|error| build_error("source clone", error))?;
    writeln!(
        output,
        "[maestro]: building revision {} with {}",
        prepared.revision,
        builder_name(options.builder)
    )
    .map_err(|error| CliError::io("failed to write local build output", error))?;

    let request = ArtifactBuildRequest {
        source: with_definition(prepared.artifact_source, options.dockerfile),
        arguments,
        secrets,
        tags: Vec::new(),
    };
    let artifacts = host_artifacts(root.join("runtime")).await?;
    let (digest, published_directly) = match options.builder {
        Builder::Native => {
            let (sink, receiver) = channel_output();
            (
                stream_build(
                    artifacts.build_with_output(&request, &sink),
                    receiver,
                    output,
                )
                .await?,
                false,
            )
        }
        Builder::Depot => {
            let project = options
                .depot_project
                .as_deref()
                .ok_or_else(|| CliError::invalid_input("--depot-project is required"))?;
            let settings = DepotBuildSettings::new(
                depot_token.ok_or_else(|| CliError::invalid_input("DEPOT_TOKEN is required"))?,
                root.join("depot"),
            );
            let depot = ProcessDepotBuildBackend::new(settings, artifacts.clone())
                .map_err(|error| build_error("Depot setup", error))?;
            let (sink, receiver) = channel_output();
            match push.as_ref() {
                Some(destination) => {
                    writeln!(output, "[maestro]: pushing {}", destination.as_str()).map_err(
                        |error| CliError::io("failed to write local build output", error),
                    )?;
                    (
                        stream_build(
                            depot.build_and_publish_with_output(
                                &request,
                                project,
                                destination,
                                &sink,
                            ),
                            receiver,
                            output,
                        )
                        .await?,
                        true,
                    )
                }
                None => (
                    stream_build(
                        depot.build_with_output(&request, project, &sink),
                        receiver,
                        output,
                    )
                    .await?,
                    false,
                ),
            }
        }
    };

    writeln!(output, "[maestro]: build succeeded: {digest}")
        .map_err(|error| CliError::io("failed to write local build output", error))?;
    if let Some(destination) = push {
        let published = if published_directly {
            digest
        } else {
            writeln!(output, "[maestro]: pushing {}", destination.as_str())
                .map_err(|error| CliError::io("failed to write local build output", error))?;
            artifacts
                .publish(&digest, &destination)
                .await
                .map_err(|error| build_error("publish", error))?
        };
        writeln!(output, "[maestro]: published {published}")
            .map_err(|error| CliError::io("failed to write local build output", error))?;
    }
    Ok(())
}

fn validate_options(options: &Options) -> Result<(), CliError> {
    if options.repository.trim().is_empty() {
        return Err(CliError::invalid_input("REPOSITORY cannot be empty"));
    }
    if options.revision.trim().is_empty() {
        return Err(CliError::invalid_input("--revision cannot be empty"));
    }
    if options.dockerfile.as_os_str().is_empty() {
        return Err(CliError::invalid_input("--dockerfile cannot be empty"));
    }
    match (options.builder, options.depot_project.as_deref()) {
        (Builder::Depot, None | Some("")) => Err(CliError::invalid_input(
            "--depot-project is required with --builder depot",
        )),
        (Builder::Native, Some(_)) => Err(CliError::invalid_input(
            "--depot-project requires --builder depot",
        )),
        _ => Ok(()),
    }
}

fn environment_values(
    names: &[String],
    kind: &str,
) -> Result<BTreeMap<String, SecretValue>, CliError> {
    names
        .iter()
        .map(|name| {
            validate_environment_name(name, kind)?;
            Ok((name.clone(), required_environment(name)?))
        })
        .collect()
}

fn validate_environment_name(name: &str, kind: &str) -> Result<(), CliError> {
    if name.is_empty()
        || name.contains('=')
        || name.contains(',')
        || name.chars().any(char::is_control)
    {
        Err(CliError::invalid_input(format!(
            "invalid {kind} environment variable name `{name}`"
        )))
    } else {
        Ok(())
    }
}

fn required_token(name: &str) -> Result<SecretValue, CliError> {
    let value = required_environment(name)?;
    if value.expose().trim().is_empty() || value.expose().chars().any(char::is_whitespace) {
        Err(CliError::invalid_input(format!(
            "environment variable {name} must contain a non-empty token without whitespace"
        )))
    } else {
        Ok(value)
    }
}

fn required_environment(name: &str) -> Result<SecretValue, CliError> {
    std::env::var(name)
        .map(SecretValue::new)
        .map_err(|_| CliError::invalid_input(format!("environment variable {name} is required")))
}

fn with_definition(source: ArtifactSource, definition: PathBuf) -> ArtifactSource {
    match source {
        ArtifactSource::Directory { root, .. } => ArtifactSource::Directory { root, definition },
        ArtifactSource::Archive { path, .. } => ArtifactSource::Archive { path, definition },
    }
}

fn builder_name(builder: Builder) -> &'static str {
    match builder {
        Builder::Native => "Maestro native builder",
        Builder::Depot => "Maestro Depot builder",
    }
}

fn build_error(stage: &str, error: impl std::fmt::Display) -> CliError {
    CliError::cluster(format!("local build {stage}"), error.to_string())
}

struct ChannelBuildOutput {
    sender: mpsc::Sender<(ArtifactBuildOutputStream, Vec<u8>)>,
}

#[async_trait]
impl ArtifactBuildOutputSink for ChannelBuildOutput {
    async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>) {
        let _ignored = self.sender.send((stream, output)).await;
    }
}

fn channel_output() -> (
    ChannelBuildOutput,
    mpsc::Receiver<(ArtifactBuildOutputStream, Vec<u8>)>,
) {
    let (sender, receiver) = mpsc::channel(OUTPUT_BUFFER_FRAMES);
    (ChannelBuildOutput { sender }, receiver)
}

async fn stream_build(
    build: impl Future<Output = Result<ArtifactDigest, ArtifactStoreError>>,
    mut receiver: mpsc::Receiver<(ArtifactBuildOutputStream, Vec<u8>)>,
    output: &mut dyn Write,
) -> Result<ArtifactDigest, CliError> {
    tokio::pin!(build);
    let result = loop {
        tokio::select! {
            result = &mut build => break result,
            frame = receiver.recv() => {
                if let Some((_stream, frame)) = frame {
                    write_frame(output, &frame)?;
                }
            }
        }
    };
    while let Ok((_stream, frame)) = receiver.try_recv() {
        write_frame(output, &frame)?;
    }
    result.map_err(|error| build_error("execution", error))
}

fn write_frame(output: &mut dyn Write, frame: &[u8]) -> Result<(), CliError> {
    output
        .write_all(frame)
        .and_then(|()| output.write_all(b"\n"))
        .map_err(|error| CliError::io("failed to write local build output", error))
}

#[cfg(target_os = "macos")]
async fn host_artifacts(_state_root: PathBuf) -> Result<Arc<dyn ArtifactStore>, CliError> {
    runtime::DockerRuntime::connect_with_defaults()
        .map(|runtime| Arc::new(runtime) as Arc<dyn ArtifactStore>)
        .map_err(|error| build_error("native backend setup", error))
}

#[cfg(target_os = "linux")]
async fn host_artifacts(state_root: PathBuf) -> Result<Arc<dyn ArtifactStore>, CliError> {
    let settings = runtime::ContainerdRuntimeSettings {
        namespace: "maestro-local-build".to_owned(),
        state_root,
        ..runtime::ContainerdRuntimeSettings::default()
    };
    runtime::ContainerdRuntime::connect(settings, Arc::new(runtime::TokioRuntimeClock::new()))
        .await
        .map(|runtime| Arc::new(runtime) as Arc<dyn ArtifactStore>)
        .map_err(|error| build_error("native backend setup", error))
}

#[cfg(not(any(target_os = "linux", target_os = "macos")))]
async fn host_artifacts(_state_root: PathBuf) -> Result<Arc<dyn ArtifactStore>, CliError> {
    Err(CliError::invalid_input(
        "local builds are supported only on Linux and macOS",
    ))
}

#[cfg(test)]
mod tests {
    use super::{Builder, Options, validate_environment_name, validate_options};

    fn options(builder: Builder) -> Options {
        Options {
            repository: "git@github.com:acme/api.git".to_owned(),
            revision: "main".to_owned(),
            dockerfile: "Dockerfile".into(),
            builder,
            depot_project: None,
            build_arguments: Vec::new(),
            secrets: Vec::new(),
            push: None,
        }
    }

    #[test]
    fn depot_requires_a_project_and_native_rejects_one() {
        assert!(validate_options(&options(Builder::Depot)).is_err());
        let mut depot = options(Builder::Depot);
        depot.depot_project = Some("project-123".to_owned());
        assert!(validate_options(&depot).is_ok());
        let mut native = options(Builder::Native);
        native.depot_project = Some("project-123".to_owned());
        assert!(validate_options(&native).is_err());
    }

    #[test]
    fn environment_names_cannot_change_backend_argument_structure() {
        assert!(validate_environment_name("NPM_TOKEN", "secret").is_ok());
        assert!(validate_environment_name("TOKEN=value", "secret").is_err());
        assert!(validate_environment_name("TOKEN,env=HOME", "secret").is_err());
    }
}
