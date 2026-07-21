use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

use async_trait::async_trait;
use kernel_api::{ArtifactArchiveId, BuildId, BuildSource, SecretValue};
use runtime::ArtifactSource;

use super::*;

type TestResult<T = ()> = Result<T, Box<dyn std::error::Error>>;

#[tokio::test]
async fn git_source_clones_fetches_pins_and_resets_with_header_auth() -> TestResult {
    let paths = TestPaths::new()?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Success));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces.clone(),
        paths.archives.clone(),
        Some(SecretValue::new("github-secret")),
        runner.clone(),
    )?;
    let build_id = BuildId::new("build-1")?;
    let source = git_source("git@github.com:acme/api.git", "main");

    let prepared = provider.prepare(&build_id, &source, None).await?;
    assert_eq!(prepared.revision, TEST_REVISION);
    assert_eq!(
        prepared.artifact_source,
        ArtifactSource::Directory {
            root: paths.workspaces.join("build-1"),
            definition: PathBuf::new(),
        }
    );
    provider
        .prepare(&build_id, &source, Some(TEST_REVISION))
        .await?;

    let calls = runner.calls();
    assert_eq!(
        command_names(&calls),
        [
            "clone",
            "fetch",
            "rev-parse",
            "reset",
            "clean",
            "cat-file",
            "reset",
            "clean"
        ]
    );
    assert!(
        calls
            .iter()
            .flat_map(|call| &call.arguments)
            .all(|argument| { !argument.to_string_lossy().contains("github-secret") })
    );
    let clone = calls.first().ok_or("clone call missing")?;
    assert_eq!(
        clone.arguments.get(3).and_then(|value| value.to_str()),
        Some("https://github.com/acme/api.git")
    );
    assert_environment(clone)?;
    let fetch = calls.get(1).ok_or("fetch call missing")?;
    assert_environment(fetch)?;
    assert_eq!(
        fetch.directory.as_deref(),
        Some(paths.workspaces.join("build-1").as_path())
    );
    Ok(())
}

#[tokio::test]
async fn remote_revision_resolves_only_the_exact_branch_head() -> TestResult {
    let paths = TestPaths::new()?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Success));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces,
        paths.archives,
        Some(SecretValue::new("github-secret")),
        runner.clone(),
    )?;

    let revision = provider
        .resolve_revision(&git_source("https://github.com/acme/api.git", "main"))
        .await?;

    assert_eq!(revision.as_deref(), Some(TEST_REVISION));
    let calls = runner.calls();
    let invocation = calls.first().ok_or("ls-remote call missing")?;
    assert_eq!(command(invocation), Some("ls-remote"));
    assert_eq!(
        invocation.arguments.last().and_then(|value| value.to_str()),
        Some("refs/heads/main")
    );
    assert_environment(invocation)?;
    Ok(())
}

#[tokio::test]
async fn archive_source_hashes_content_and_rejects_mutation() -> TestResult {
    let paths = TestPaths::new()?;
    tokio::fs::create_dir_all(&paths.archives).await?;
    let archive_id = ArtifactArchiveId::new("archive-1")?;
    let archive = paths.archives.join(archive_id.as_str());
    tokio::fs::write(&archive, b"first archive").await?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Success));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces,
        paths.archives,
        None,
        runner.clone(),
    )?;
    let source = BuildSource::Tarball {
        archive_id: archive_id.clone(),
    };

    let prepared = provider
        .prepare(&BuildId::new("build-1")?, &source, None)
        .await?;
    assert!(prepared.revision.starts_with("sha256:"));
    assert_eq!(
        prepared.artifact_source,
        ArtifactSource::Archive {
            path: archive.clone(),
            definition: PathBuf::new(),
        }
    );
    provider
        .prepare(&BuildId::new("build-1")?, &source, Some(&prepared.revision))
        .await?;
    tokio::fs::write(&archive, b"changed archive").await?;

    let error = expect_source_error(
        provider
            .prepare(&BuildId::new("build-1")?, &source, Some(&prepared.revision))
            .await,
        "archive mutation must fail",
    )?;
    assert!(matches!(error, BuildSourceError::Rejected { .. }));
    assert!(runner.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn cleanup_removes_only_the_build_workspace() -> TestResult {
    let paths = TestPaths::new()?;
    let workspace = paths.workspaces.join("build-1");
    let archive = paths.archives.join("archive-1");
    tokio::fs::create_dir_all(workspace.join(".git")).await?;
    tokio::fs::create_dir_all(&paths.archives).await?;
    tokio::fs::write(&archive, b"retained").await?;
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces,
        paths.archives,
        None,
        Arc::new(FakeGitRunner::new(FakeGitMode::Success)),
    )?;

    provider.cleanup(&BuildId::new("build-1")?).await?;
    provider.cleanup(&BuildId::new("build-1")?).await?;

    assert!(!workspace.exists());
    assert!(archive.exists());
    Ok(())
}

#[tokio::test]
async fn git_source_rejects_credential_urls_and_option_revisions_before_spawn() -> TestResult {
    let paths = TestPaths::new()?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Success));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces,
        paths.archives,
        None,
        runner.clone(),
    )?;
    let build_id = BuildId::new("build-1")?;

    let credential_error = expect_source_error(
        provider
            .prepare(
                &build_id,
                &git_source("https://user@github.com/acme/api.git", "main"),
                None,
            )
            .await,
        "credential URL must fail",
    )?;
    let option_error = expect_source_error(
        provider
            .prepare(
                &build_id,
                &git_source("https://github.com/acme/api.git", "--upload-pack=bad"),
                None,
            )
            .await,
        "option revision must fail",
    )?;

    assert!(matches!(
        credential_error,
        BuildSourceError::Rejected { .. }
    ));
    assert!(matches!(option_error, BuildSourceError::Rejected { .. }));
    assert!(runner.calls().is_empty());
    Ok(())
}

#[tokio::test]
async fn git_transport_failure_is_retryable_and_does_not_create_checkout() -> TestResult {
    let paths = TestPaths::new()?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Unavailable));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces.clone(),
        paths.archives,
        None,
        runner,
    )?;

    let error = expect_source_error(
        provider
            .prepare(
                &BuildId::new("build-1")?,
                &git_source("https://github.com/acme/api.git", "main"),
                None,
            )
            .await,
        "transport failure must retry",
    )?;

    assert!(matches!(error, BuildSourceError::Unavailable { .. }));
    assert!(!paths.workspaces.join("build-1").exists());
    Ok(())
}

#[tokio::test]
async fn incomplete_checkout_is_removed_and_cloned_again() -> TestResult {
    let paths = TestPaths::new()?;
    let workspace = paths.workspaces.join("build-1");
    tokio::fs::create_dir_all(&workspace).await?;
    tokio::fs::write(workspace.join("partial"), b"clone interrupted").await?;
    let runner = Arc::new(FakeGitRunner::new(FakeGitMode::Success));
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces,
        paths.archives,
        None,
        runner.clone(),
    )?;

    provider
        .prepare(
            &BuildId::new("build-1")?,
            &git_source("https://github.com/acme/api.git", "main"),
            None,
        )
        .await?;

    assert!(!workspace.join("partial").exists());
    assert!(workspace.join(".git").is_dir());
    assert_eq!(
        command_names(&runner.calls()).first().copied(),
        Some("clone")
    );
    Ok(())
}

fn git_source(repository: &str, revision: &str) -> BuildSource {
    BuildSource::Git {
        repository: repository.to_string(),
        revision: revision.to_string(),
    }
}

fn command_names(calls: &[GitInvocation]) -> Vec<&str> {
    calls
        .iter()
        .filter_map(|call| call.arguments.first().and_then(|value| value.to_str()))
        .collect()
}

fn assert_environment(invocation: &GitInvocation) -> TestResult {
    let values = invocation
        .environment
        .iter()
        .map(|(key, value)| (key.to_string_lossy().into_owned(), value.expose()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get("GIT_TERMINAL_PROMPT").copied(), Some("0"));
    assert_eq!(values.get("GIT_CONFIG_COUNT").copied(), Some("1"));
    assert_eq!(
        values.get("GIT_CONFIG_KEY_0").copied(),
        Some("http.https://github.com/.extraHeader")
    );
    let header = values
        .get("GIT_CONFIG_VALUE_0")
        .ok_or("Git authorization header missing")?;
    assert!(header.starts_with("Authorization: basic "));
    assert!(!header.contains("github-secret"));
    Ok(())
}

const TEST_REVISION: &str = "0123456789abcdef0123456789abcdef01234567";

#[derive(Clone, Copy)]
enum FakeGitMode {
    Success,
    Unavailable,
}

struct FakeGitRunner {
    mode: FakeGitMode,
    calls: Mutex<Vec<GitInvocation>>,
}

impl FakeGitRunner {
    fn new(mode: FakeGitMode) -> Self {
        Self {
            mode,
            calls: Mutex::new(Vec::new()),
        }
    }

    fn calls(&self) -> Vec<GitInvocation> {
        lock(&self.calls).clone()
    }
}

#[async_trait]
impl GitRunner for FakeGitRunner {
    async fn run(&self, invocation: GitInvocation) -> Result<GitOutput, GitRunError> {
        lock(&self.calls).push(invocation.clone());
        if matches!(self.mode, FakeGitMode::Unavailable) {
            return Err(GitRunError::Unavailable {
                message: "injected network failure".to_string(),
            });
        }
        if command(&invocation) == Some("clone") {
            let target = invocation
                .arguments
                .last()
                .map(PathBuf::from)
                .ok_or_else(|| GitRunError::Exit {
                    message: "clone target missing".to_string(),
                })?;
            tokio::fs::create_dir_all(target.join(".git"))
                .await
                .map_err(|error| GitRunError::Unavailable {
                    message: error.to_string(),
                })?;
        }
        let stdout = if command(&invocation) == Some("rev-parse") {
            format!("{TEST_REVISION}\n")
        } else if command(&invocation) == Some("ls-remote") {
            format!("{TEST_REVISION}\trefs/heads/main\n")
        } else {
            String::new()
        };
        Ok(GitOutput { stdout })
    }
}

fn command(invocation: &GitInvocation) -> Option<&str> {
    invocation
        .arguments
        .first()
        .and_then(|value| value.to_str())
}

fn lock<T>(mutex: &Mutex<T>) -> MutexGuard<'_, T> {
    match mutex.lock() {
        Ok(guard) => guard,
        Err(poisoned) => poisoned.into_inner(),
    }
}

fn expect_source_error<T>(
    result: Result<T, BuildSourceError>,
    context: &str,
) -> TestResult<BuildSourceError> {
    match result {
        Ok(_) => Err(context.to_string().into()),
        Err(error) => Ok(error),
    }
}

struct TestPaths {
    _temp: tempfile::TempDir,
    workspaces: PathBuf,
    archives: PathBuf,
}

impl TestPaths {
    fn new() -> TestResult<Self> {
        let temp = tempfile::tempdir()?;
        let root = std::fs::canonicalize(temp.path())?;
        Ok(Self {
            workspaces: root.join("workspaces"),
            archives: root.join("archives"),
            _temp: temp,
        })
    }
}

#[cfg(unix)]
#[tokio::test]
async fn cleanup_refuses_a_symlinked_workspace() -> TestResult {
    let paths = TestPaths::new()?;
    let outside = paths.workspaces.with_file_name("outside");
    tokio::fs::create_dir_all(&paths.workspaces).await?;
    tokio::fs::create_dir_all(&outside).await?;
    std::os::unix::fs::symlink(&outside, paths.workspaces.join("build-1"))?;
    let provider = LocalBuildSourceProvider::with_runner(
        paths.workspaces.clone(),
        paths.archives,
        None,
        Arc::new(FakeGitRunner::new(FakeGitMode::Success)),
    )?;

    let error = expect_source_error(
        provider.cleanup(&BuildId::new("build-1")?).await,
        "symlink cleanup must fail closed",
    )?;

    assert!(matches!(error, BuildSourceError::Rejected { .. }));
    assert!(outside.exists());
    assert!(paths.workspaces.join("build-1").is_symlink());
    Ok(())
}
