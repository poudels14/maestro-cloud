use std::collections::BTreeMap;
use std::ffi::OsString;
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::SecretValue;
use runtime::{
    ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest, ArtifactPrunePolicy,
    ArtifactPruneReport, ArtifactReference, ArtifactSource, ArtifactStore, ArtifactStoreError,
};

use super::support::TestResult;
use crate::depot::{DepotInvocation, DepotKeyKind, DepotRunner, validate_key};
use crate::{DepotBuildBackend, DepotBuildSettings, ProcessDepotBuildBackend};

#[test]
fn depot_key_kinds_apply_distinct_delimiter_rules() {
    assert!(validate_key(DepotKeyKind::Argument, "PROFILE,NAME").is_ok());
    assert!(validate_key(DepotKeyKind::Secret, "PRIVATE,TOKEN").is_err());
    assert!(validate_key(DepotKeyKind::Argument, "PROFILE=NAME").is_err());
    assert!(validate_key(DepotKeyKind::Secret, "PRIVATE=TOKEN").is_err());
}

#[tokio::test]
async fn depot_cli_keeps_credentials_out_of_argv_and_imports_output() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let state_root = temporary.path().join("state");
    let context = temporary.path().join("context");
    std::fs::create_dir(&context)?;
    std::fs::write(context.join("Dockerfile"), b"FROM scratch\n")?;
    let context = std::fs::canonicalize(context)?;
    let state_root = absolute_without_symlinks(state_root)?;
    let artifacts = Arc::new(ImportingArtifacts::default());
    let runner = Arc::new(RecordingDepotRunner::default());
    let mut settings =
        DepotBuildSettings::new(SecretValue::new("depot-token-never-in-argv"), state_root);
    settings.executable = PathBuf::from("/opt/depot/bin/depot");
    settings.platform = "linux/arm64".to_owned();
    settings.build_timeout = Duration::from_secs(42);
    let backend =
        ProcessDepotBuildBackend::with_runner(settings, artifacts.clone(), runner.clone())?;
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: context.clone(),
            definition: PathBuf::from("Dockerfile"),
        },
        arguments: BTreeMap::from([("PROFILE".to_owned(), SecretValue::new("release"))]),
        secrets: BTreeMap::from([(
            "PRIVATE_TOKEN".to_owned(),
            SecretValue::new("build-secret-never-in-argv"),
        )]),
        tags: Vec::new(),
    };

    let digest = backend.build(&request, "project-123").await?;

    assert_eq!(digest, ArtifactDigest::new("sha256:depot-import")?);
    assert_eq!(artifacts.imports(), [b"fake-docker-archive".to_vec()]);
    let invocation = runner.only_invocation()?;
    assert_eq!(invocation.executable, PathBuf::from("/opt/depot/bin/depot"));
    assert_eq!(invocation.directory, context);
    assert_eq!(invocation.timeout, Duration::from_secs(42));
    assert_eq!(invocation.definition, b"FROM scratch\n");
    let arguments = invocation
        .arguments
        .iter()
        .map(|argument| argument.to_string_lossy().into_owned())
        .collect::<Vec<_>>();
    assert_eq!(arguments.first().map(String::as_str), Some("build"));
    assert!(contains_pair(&arguments, "--project", "project-123"));
    assert!(contains_pair(&arguments, "--platform", "linux/arm64"));
    assert!(contains_pair(&arguments, "--file", "Dockerfile"));
    assert!(contains_pair(
        &arguments,
        "--tag",
        "maestro.local/builds/output:latest"
    ));
    assert!(contains_pair(&arguments, "--build-arg", "PROFILE=release"));
    assert!(contains_pair(
        &arguments,
        "--secret",
        "id=PRIVATE_TOKEN,env=MAESTRO_DEPOT_BUILD_SECRET_0"
    ));
    assert!(
        arguments
            .iter()
            .any(|argument| argument.starts_with("type=docker,dest="))
    );
    assert!(
        arguments
            .iter()
            .all(|argument| !argument.contains("depot-token-never-in-argv"))
    );
    assert!(
        arguments
            .iter()
            .all(|argument| !argument.contains("build-secret-never-in-argv"))
    );
    assert_eq!(
        invocation
            .environment
            .get("DEPOT_TOKEN")
            .map(String::as_str),
        Some("depot-token-never-in-argv")
    );
    assert_eq!(
        invocation
            .environment
            .get("MAESTRO_DEPOT_BUILD_SECRET_0")
            .map(String::as_str),
        Some("build-secret-never-in-argv")
    );
    Ok(())
}

#[tokio::test]
async fn depot_cli_extracts_an_uploaded_context_before_building() -> TestResult {
    let temporary = tempfile::tempdir()?;
    let archive = temporary.path().join("context.tar");
    let file = std::fs::File::create(&archive)?;
    let mut tar = tar::Builder::new(file);
    let dockerfile = b"FROM scratch\n";
    let mut header = tar::Header::new_gnu();
    header.set_path("nested/Dockerfile")?;
    header.set_size(u64::try_from(dockerfile.len())?);
    header.set_mode(0o600);
    header.set_cksum();
    tar.append(&header, dockerfile.as_slice())?;
    tar.finish()?;
    drop(tar);
    let archive = std::fs::canonicalize(archive)?;
    let state_root = absolute_without_symlinks(temporary.path().join("state"))?;
    let artifacts = Arc::new(ImportingArtifacts::default());
    let runner = Arc::new(RecordingDepotRunner::default());
    let backend = ProcessDepotBuildBackend::with_runner(
        DepotBuildSettings::new(SecretValue::new("depot-token"), state_root),
        artifacts,
        runner.clone(),
    )?;
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Archive {
            path: archive,
            definition: PathBuf::from("nested/Dockerfile"),
        },
        arguments: BTreeMap::new(),
        secrets: BTreeMap::new(),
        tags: Vec::new(),
    };

    backend.build(&request, "project-upload").await?;

    let invocation = runner.only_invocation()?;
    assert_eq!(invocation.definition, dockerfile);
    assert!(contains_pair(
        &invocation
            .arguments
            .iter()
            .map(|argument| argument.to_string_lossy().into_owned())
            .collect::<Vec<_>>(),
        "--file",
        "nested/Dockerfile"
    ));
    Ok(())
}

fn absolute_without_symlinks(path: PathBuf) -> TestResult<PathBuf> {
    std::fs::create_dir_all(&path)?;
    Ok(std::fs::canonicalize(path)?)
}

fn contains_pair(arguments: &[String], key: &str, value: &str) -> bool {
    arguments.windows(2).any(|pair| {
        pair.first().map(String::as_str) == Some(key)
            && pair.get(1).map(String::as_str) == Some(value)
    })
}

#[derive(Clone)]
struct InvocationSnapshot {
    executable: PathBuf,
    arguments: Vec<OsString>,
    directory: PathBuf,
    environment: BTreeMap<String, String>,
    timeout: Duration,
    definition: Vec<u8>,
}

#[derive(Default)]
struct RecordingDepotRunner {
    invocations: Mutex<Vec<InvocationSnapshot>>,
}

impl RecordingDepotRunner {
    fn only_invocation(&self) -> TestResult<InvocationSnapshot> {
        let invocations = lock(&self.invocations);
        if invocations.len() != 1 {
            return Err(
                format!("expected one Depot invocation, found {}", invocations.len()).into(),
            );
        }
        invocations
            .first()
            .cloned()
            .ok_or_else(|| "Depot invocation is missing".into())
    }
}

#[async_trait]
impl DepotRunner for RecordingDepotRunner {
    async fn run(
        &self,
        invocation: DepotInvocation,
        timeout: Duration,
    ) -> Result<(), ArtifactStoreError> {
        let environment = invocation
            .environment
            .iter()
            .map(|(key, value)| {
                (
                    key.to_string_lossy().into_owned(),
                    value.expose().to_owned(),
                )
            })
            .collect();
        let definition = tokio::fs::read(
            invocation
                .directory
                .join(definition_argument(&invocation.arguments)?),
        )
        .await
        .map_err(|error| ArtifactStoreError::Rejected {
            message: format!("read Depot definition: {error}"),
        })?;
        lock(&self.invocations).push(InvocationSnapshot {
            executable: invocation.executable,
            arguments: invocation.arguments,
            directory: invocation.directory,
            environment,
            timeout,
            definition,
        });
        tokio::fs::write(&invocation.output, b"fake-docker-archive")
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("write fake Depot output: {error}"),
            })
    }
}

fn definition_argument(arguments: &[OsString]) -> Result<PathBuf, ArtifactStoreError> {
    arguments
        .windows(2)
        .find_map(|pair| {
            (pair.first().and_then(|value| value.to_str()) == Some("--file"))
                .then(|| pair.get(1))
                .flatten()
        })
        .map(PathBuf::from)
        .ok_or_else(|| ArtifactStoreError::Rejected {
            message: "Depot definition argument is missing".to_owned(),
        })
}

#[derive(Default)]
struct ImportingArtifacts {
    imports: Mutex<Vec<Vec<u8>>>,
}

impl ImportingArtifacts {
    fn imports(&self) -> Vec<Vec<u8>> {
        lock(&self.imports).clone()
    }
}

#[async_trait]
impl ArtifactStore for ImportingArtifacts {
    async fn build(
        &self,
        _request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("build"))
    }

    async fn pull(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("pull"))
    }

    async fn push(
        &self,
        _digest: &ArtifactDigest,
        _destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        Err(unused("push"))
    }

    async fn resolve_digest(
        &self,
        _reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        Err(unused("resolve digest"))
    }

    async fn contains(&self, _digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        Ok(false)
    }

    async fn export(
        &self,
        _digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        Err(unused("export"))
    }

    async fn import(
        &self,
        mut source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let mut bytes = Vec::new();
        while let Some(chunk) = source.next().await? {
            bytes.extend(chunk);
        }
        lock(&self.imports).push(bytes);
        ArtifactDigest::new("sha256:depot-import")
    }

    async fn prune(
        &self,
        _policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        Err(unused("prune"))
    }
}

fn unused(operation: &str) -> ArtifactStoreError {
    ArtifactStoreError::Rejected {
        message: format!("unexpected {operation} call"),
    }
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}
