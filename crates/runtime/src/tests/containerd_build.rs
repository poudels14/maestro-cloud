use std::collections::BTreeMap;
use std::ffi::OsString;
use std::io::Write;
use std::os::unix::fs::PermissionsExt;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use async_trait::async_trait;
use kernel_api::SecretValue;

use crate::containerd_build::{
    BuildctlInvocation, BuildctlRunner, ProcessBuildctlRunner, run_build,
};
use crate::containerd_build_context::prepare_context;
use crate::{
    ArtifactBuildOutputSink, ArtifactBuildOutputStream, ArtifactBuildRequest, ArtifactReference,
    ArtifactSource, ArtifactStoreError, ContainerdRuntimeSettings, DiscardArtifactBuildOutput,
};

#[derive(Default)]
struct RecordingRunner {
    calls: Mutex<Vec<BuildctlInvocation>>,
    output: Vec<u8>,
}

#[derive(Default)]
struct RecordingBuildOutput(Mutex<Vec<(ArtifactBuildOutputStream, String)>>);

impl RecordingBuildOutput {
    fn frames(&self) -> Vec<(ArtifactBuildOutputStream, String)> {
        self.0.lock().unwrap().clone()
    }
}

#[async_trait]
impl ArtifactBuildOutputSink for RecordingBuildOutput {
    async fn write(&self, stream: ArtifactBuildOutputStream, output: Vec<u8>) {
        self.0
            .lock()
            .unwrap()
            .push((stream, String::from_utf8_lossy(&output).into_owned()));
    }
}

#[async_trait]
impl BuildctlRunner for RecordingRunner {
    async fn run(
        &self,
        invocation: BuildctlInvocation,
        _timeout: Duration,
        _output: &dyn ArtifactBuildOutputSink,
    ) -> Result<(), ArtifactStoreError> {
        tokio::fs::write(&invocation.output, &self.output)
            .await
            .map_err(|error| ArtifactStoreError::Unavailable {
                message: format!("write fake BuildKit output: {error}"),
            })?;
        self.calls.lock().unwrap().push(invocation);
        Ok(())
    }
}

#[tokio::test]
async fn buildkit_runner_receives_exact_cli_artifact_and_environment_only_secrets() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("Containerfile"), "FROM scratch\n").unwrap();
    let runner = Arc::new(RecordingRunner {
        calls: Mutex::new(Vec::new()),
        output: b"oci-archive".to_vec(),
    });
    let settings = settings(temporary.path().join("state"));
    let request = ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root: source.clone(),
            definition: PathBuf::from("Containerfile"),
        },
        arguments: BTreeMap::from([
            ("CHANNEL,NAME".to_owned(), SecretValue::new("stable")),
            ("MODE".to_owned(), SecretValue::new("release")),
        ]),
        secrets: BTreeMap::from([(
            "registry-token".to_owned(),
            SecretValue::new("never-appear-in-argv"),
        )]),
        tags: vec![ArtifactReference::new("registry.example/app:v1").unwrap()],
    };

    let output = run_build(
        &request,
        &settings,
        runner.clone(),
        &DiscardArtifactBuildOutput,
    )
    .await
    .unwrap();
    let mut stream = output.into_stream().await.unwrap();
    assert_eq!(stream.next().await.unwrap(), Some(b"oci-archive".to_vec()));
    assert_eq!(stream.next().await.unwrap(), None);

    let calls = runner.calls.lock().unwrap();
    assert_eq!(calls.len(), 1);
    let invocation = calls.first().unwrap();
    let args = invocation
        .arguments
        .iter()
        .map(|value| value.to_str().unwrap())
        .collect::<Vec<_>>();
    assert_eq!(
        invocation.executable,
        PathBuf::from("/usr/local/bin/buildctl")
    );
    assert_eq!(invocation.address, "unix:///run/buildkit-test.sock");
    assert_eq!(
        args.get(..13).unwrap(),
        [
            "--addr",
            "unix:///run/buildkit-test.sock",
            "build",
            "--progress",
            "plain",
            "--frontend",
            "dockerfile.v0",
            "--local",
            &format!("context={}", source.display()),
            "--local",
            &format!("dockerfile={}", source.display()),
            "--opt",
            "filename=Containerfile",
        ]
    );
    assert!(args.contains(&"build-arg:CHANNEL,NAME=stable"));
    assert!(args.contains(&"build-arg:MODE=release"));
    assert!(args.contains(&"id=registry-token,type=env,env=MAESTRO_BUILDKIT_SECRET_0"));
    assert!(args.last().unwrap().starts_with("type=oci,dest="));
    assert!(!args.join(" ").contains("never-appear-in-argv"));
    assert_eq!(
        invocation.environment,
        vec![(
            OsString::from("MAESTRO_BUILDKIT_SECRET_0"),
            SecretValue::new("never-appear-in-argv")
        )]
    );
    assert!(!format!("{invocation:?}").contains("never-appear-in-argv"));
}

#[tokio::test]
async fn buildkit_rejects_injected_secret_fields_before_running() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("Dockerfile"), "FROM scratch\n").unwrap();
    let runner = Arc::new(RecordingRunner {
        calls: Mutex::new(Vec::new()),
        output: b"unused".to_vec(),
    });
    let mut request = request(source);
    request.secrets.insert(
        "TOKEN,env=HOST_SECRET".to_owned(),
        SecretValue::new("protected"),
    );

    let error = run_build(
        &request,
        &settings(temporary.path().join("state")),
        runner.clone(),
        &DiscardArtifactBuildOutput,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ArtifactStoreError::Rejected { .. }));
    assert!(!error.to_string().contains("protected"));
    assert!(runner.calls.lock().unwrap().is_empty());
}

#[tokio::test]
async fn missing_buildkit_client_is_retryable_without_exposing_secrets() {
    let temporary = tempfile::tempdir().unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("Dockerfile"), "FROM scratch\n").unwrap();
    let mut request = request(source);
    request
        .secrets
        .insert("TOKEN".to_owned(), SecretValue::new("protected-value"));
    let mut settings = settings(temporary.path().join("state"));
    settings.buildctl = temporary.path().join("missing-buildctl");

    let error = run_build(
        &request,
        &settings,
        Arc::new(ProcessBuildctlRunner),
        &DiscardArtifactBuildOutput,
    )
    .await
    .unwrap_err();

    assert!(matches!(error, ArtifactStoreError::Unavailable { .. }));
    assert!(!error.to_string().contains("protected-value"));
}

#[tokio::test]
async fn buildkit_process_streams_both_outputs_and_redacts_build_secrets() {
    let temporary = tempfile::tempdir().unwrap();
    let executable = temporary.path().join("fake-buildctl");
    std::fs::write(
        &executable,
        r#"#!/bin/sh
if [ "$3" = 'debug' ]; then
  exit 0
fi
printf 'buildkit build started\n'
printf 'token %s\n' "$MAESTRO_BUILDKIT_SECRET_0" >&2
output=''
while [ "$#" -gt 0 ]; do
  if [ "$1" = '--output' ]; then
    shift
    output="$1"
  fi
  shift
done
destination="${output#*dest=}"
destination="${destination%%,*}"
printf 'fake-oci-archive' > "$destination"
"#,
    )
    .unwrap();
    std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
    let source = temporary.path().join("source");
    std::fs::create_dir(&source).unwrap();
    std::fs::write(source.join("Dockerfile"), "FROM scratch\n").unwrap();
    let mut request = request(std::fs::canonicalize(source).unwrap());
    request.secrets.insert(
        "NPM_TOKEN".to_owned(),
        SecretValue::new("npm-token-must-not-leak"),
    );
    let mut settings = settings(temporary.path().join("state"));
    settings.buildctl = executable;
    let output = RecordingBuildOutput::default();

    let artifact = run_build(
        &request,
        &settings,
        Arc::new(ProcessBuildctlRunner),
        &output,
    )
    .await
    .unwrap();
    let mut archive = artifact.into_stream().await.unwrap();
    assert_eq!(
        archive.next().await.unwrap(),
        Some(b"fake-oci-archive".to_vec())
    );

    let frames = output.frames();
    assert!(frames.iter().any(|(stream, text)| {
        *stream == ArtifactBuildOutputStream::Stdout && text == "buildkit build started"
    }));
    assert!(frames.iter().any(|(stream, text)| {
        *stream == ArtifactBuildOutputStream::Stderr && text == "token [REDACTED]"
    }));
    assert!(
        frames
            .iter()
            .all(|(_, text)| !text.contains("npm-token-must-not-leak"))
    );
}

#[tokio::test]
async fn buildkit_extracts_gzip_tarballs_with_bounded_regular_entries() {
    let temporary = tempfile::tempdir().unwrap();
    let archive = temporary.path().join("source.tar.gz");
    write_gzip_archive(
        &archive,
        &[("Dockerfile", b"FROM scratch\n"), ("app", b"hello")],
    );
    let workspace = temporary.path().join("workspace");
    std::fs::create_dir(&workspace).unwrap();
    let source = ArtifactSource::Archive {
        path: archive,
        definition: PathBuf::new(),
    };

    let context = prepare_context(&source, &workspace, 1_024, 10)
        .await
        .unwrap();

    assert_eq!(context.definition, "Dockerfile");
    assert_eq!(std::fs::read(context.root.join("app")).unwrap(), b"hello");
}

#[tokio::test]
async fn buildkit_archive_limits_and_link_rejection_are_matchable() {
    let temporary = tempfile::tempdir().unwrap();
    let archive = temporary.path().join("large.tar");
    write_tar_archive(&archive, &[("Dockerfile", b"FROM scratch\n")]);
    let first_workspace = temporary.path().join("first");
    std::fs::create_dir(&first_workspace).unwrap();
    let source = ArtifactSource::Archive {
        path: archive,
        definition: PathBuf::new(),
    };
    let error = prepare_context(&source, &first_workspace, 4, 10)
        .await
        .unwrap_err();
    assert!(matches!(error, ArtifactStoreError::Rejected { .. }));
    assert!(error.to_string().contains("expanded-size limit"));

    let link_archive = temporary.path().join("link.tar");
    write_link_archive(&link_archive);
    let second_workspace = temporary.path().join("second");
    std::fs::create_dir(&second_workspace).unwrap();
    let source = ArtifactSource::Archive {
        path: link_archive,
        definition: PathBuf::new(),
    };
    let error = prepare_context(&source, &second_workspace, 1_024, 10)
        .await
        .unwrap_err();
    assert!(matches!(error, ArtifactStoreError::Rejected { .. }));
    assert!(error.to_string().contains("escapes the context"));
}

fn request(root: PathBuf) -> ArtifactBuildRequest {
    ArtifactBuildRequest {
        source: ArtifactSource::Directory {
            root,
            definition: PathBuf::new(),
        },
        arguments: BTreeMap::new(),
        secrets: BTreeMap::new(),
        tags: Vec::new(),
    }
}

fn settings(state_root: PathBuf) -> ContainerdRuntimeSettings {
    ContainerdRuntimeSettings {
        state_root,
        buildctl: PathBuf::from("/usr/local/bin/buildctl"),
        buildkit_address: "unix:///run/buildkit-test.sock".to_owned(),
        ..ContainerdRuntimeSettings::default()
    }
}

fn write_gzip_archive(path: &Path, entries: &[(&str, &[u8])]) {
    let file = std::fs::File::create(path).unwrap();
    let encoder = flate2::write::GzEncoder::new(file, flate2::Compression::default());
    let mut archive = tar::Builder::new(encoder);
    append_files(&mut archive, entries);
    archive.into_inner().unwrap().finish().unwrap();
}

fn write_tar_archive(path: &Path, entries: &[(&str, &[u8])]) {
    let file = std::fs::File::create(path).unwrap();
    let mut archive = tar::Builder::new(file);
    append_files(&mut archive, entries);
    archive.finish().unwrap();
}

fn append_files(writer: &mut tar::Builder<impl Write>, entries: &[(&str, &[u8])]) {
    for (path, contents) in entries {
        let mut header = tar::Header::new_gnu();
        header.set_size(contents.len() as u64);
        header.set_mode(0o644);
        header.set_cksum();
        writer.append_data(&mut header, path, *contents).unwrap();
    }
}

fn write_link_archive(path: &Path) {
    let file = std::fs::File::create(path).unwrap();
    let mut archive = tar::Builder::new(file);
    let mut header = tar::Header::new_gnu();
    header.set_entry_type(tar::EntryType::Symlink);
    header.set_size(0);
    header.set_mode(0o777);
    header.set_cksum();
    archive
        .append_link(&mut header, "escape", "../outside")
        .unwrap();
    archive.finish().unwrap();
}
