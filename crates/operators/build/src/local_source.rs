use std::ffi::OsString;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::sync::Arc;

use async_trait::async_trait;
use kernel_api::{BuildId, BuildSource, SecretValue};
use runtime::ArtifactSource;

use crate::git_process::{
    GitInvocation, GitOutput, GitRunError, GitRunner, ProcessGitRunner, git_environment,
    normalize_repository,
};
use crate::local_fs::{
    WorkspaceState, ensure_root, hash_file, io_unavailable, path_text, validate_git_directory,
    validate_root, validate_workspace,
};
use crate::{BuildSourceError, BuildSourceProvider, PreparedBuildSource};

/// Filesystem-backed Git checkout and uploaded-archive source provider.
pub struct LocalBuildSourceProvider {
    workspace_root: PathBuf,
    archive_root: PathBuf,
    github_token: Option<SecretValue>,
    runner: Arc<dyn GitRunner>,
}

impl LocalBuildSourceProvider {
    /// Creates a provider with disjoint absolute roots owned by Maestro.
    pub fn new(
        workspace_root: PathBuf,
        archive_root: PathBuf,
        github_token: Option<SecretValue>,
    ) -> Result<Self, BuildSourceError> {
        Self::with_runner(
            workspace_root,
            archive_root,
            github_token,
            Arc::new(ProcessGitRunner),
        )
    }

    fn with_runner(
        workspace_root: PathBuf,
        archive_root: PathBuf,
        github_token: Option<SecretValue>,
        runner: Arc<dyn GitRunner>,
    ) -> Result<Self, BuildSourceError> {
        validate_root("build workspace", &workspace_root)?;
        validate_root("build archive", &archive_root)?;
        if workspace_root.starts_with(&archive_root) || archive_root.starts_with(&workspace_root) {
            return Err(BuildSourceError::rejected(
                "build workspace and archive roots must be disjoint",
            ));
        }
        Ok(Self {
            workspace_root,
            archive_root,
            github_token,
            runner,
        })
    }

    async fn prepare_git(
        &self,
        build_id: &BuildId,
        repository: &str,
        requested_revision: &str,
        resolved_revision: Option<&str>,
    ) -> Result<PreparedBuildSource, BuildSourceError> {
        validate_requested_revision(requested_revision)?;
        let repository = normalize_repository(repository)?;
        let root = ensure_root(&self.workspace_root).await?;
        let workspace = root.join(build_id.as_str());
        let state = validate_workspace(&workspace).await?;
        let environment = git_environment(&repository, self.github_token.as_ref())?;
        if matches!(state, WorkspaceState::Incomplete) {
            tokio::fs::remove_dir_all(&workspace)
                .await
                .map_err(|error| {
                    io_unavailable("remove incomplete build workspace", &workspace, error)
                })?;
        }
        if !matches!(state, WorkspaceState::Checkout) {
            self.run_network(
                GitInvocation::new([
                    "clone",
                    "--no-checkout",
                    "--",
                    repository.as_str(),
                    path_text(&workspace)?,
                ])
                .with_environment(environment.clone()),
            )
            .await?;
            validate_git_directory(&workspace).await?;
        }

        let revision = match resolved_revision {
            Some(revision) => {
                validate_resolved_revision(revision)?;
                if !self.commit_exists(&workspace, revision).await? {
                    self.fetch(&workspace, revision, environment.clone())
                        .await?;
                    if !self.commit_exists(&workspace, revision).await? {
                        return Err(BuildSourceError::rejected(format!(
                            "pinned source revision `{revision}` is unavailable after fetch"
                        )));
                    }
                }
                revision.to_string()
            }
            None => {
                self.fetch(&workspace, requested_revision, environment)
                    .await?;
                let output = self
                    .run_local(
                        GitInvocation::new(["rev-parse", "--verify", "FETCH_HEAD^{commit}"])
                            .in_directory(workspace.clone()),
                    )
                    .await
                    .map_err(|error| {
                        BuildSourceError::rejected(format!(
                            "requested source revision `{requested_revision}` could not be resolved: {error}"
                        ))
                    })?;
                parse_revision(&output.stdout)?
            }
        };

        self.run_local(
            GitInvocation::new(["reset", "--hard", revision.as_str(), "--"])
                .in_directory(workspace.clone()),
        )
        .await?;
        self.run_local(GitInvocation::new(["clean", "-ffd", "--"]).in_directory(workspace.clone()))
            .await?;
        Ok(PreparedBuildSource {
            artifact_source: ArtifactSource::Directory {
                root: workspace,
                definition: PathBuf::new(),
            },
            revision,
        })
    }

    async fn fetch(
        &self,
        workspace: &Path,
        revision: &str,
        environment: Vec<(OsString, SecretValue)>,
    ) -> Result<(), BuildSourceError> {
        self.run_network(
            GitInvocation::new(["fetch", "--force", "--tags", "origin", "--", revision])
                .in_directory(workspace.to_path_buf())
                .with_environment(environment),
        )
        .await
        .map(|_| ())
    }

    async fn commit_exists(
        &self,
        workspace: &Path,
        revision: &str,
    ) -> Result<bool, BuildSourceError> {
        let object = format!("{revision}^{{commit}}");
        match self
            .runner
            .run(
                GitInvocation::new(["cat-file", "-e", object.as_str()])
                    .in_directory(workspace.to_path_buf()),
            )
            .await
        {
            Ok(_) => Ok(true),
            Err(GitRunError::Exit { .. }) => Ok(false),
            Err(error) => Err(BuildSourceError::unavailable(error.to_string())),
        }
    }

    async fn prepare_archive(
        &self,
        archive_id: &kernel_api::ArtifactArchiveId,
        resolved_revision: Option<&str>,
    ) -> Result<PreparedBuildSource, BuildSourceError> {
        let root = ensure_root(&self.archive_root).await?;
        let archive = root.join(archive_id.as_str());
        let metadata = tokio::fs::symlink_metadata(&archive)
            .await
            .map_err(|error| match error.kind() {
                ErrorKind::NotFound => BuildSourceError::rejected(format!(
                    "uploaded build archive `{archive_id}` does not exist"
                )),
                _ => io_unavailable("inspect build archive", &archive, error),
            })?;
        if !metadata.file_type().is_file() {
            return Err(BuildSourceError::rejected(format!(
                "uploaded build archive `{archive_id}` must be a regular file"
            )));
        }
        let revision = hash_file(&archive).await?;
        if let Some(expected) = resolved_revision
            && revision != expected
        {
            return Err(BuildSourceError::rejected(format!(
                "uploaded build archive `{archive_id}` changed after source preparation"
            )));
        }
        Ok(PreparedBuildSource {
            artifact_source: ArtifactSource::Archive {
                path: archive,
                definition: PathBuf::new(),
            },
            revision,
        })
    }

    async fn run_network(&self, invocation: GitInvocation) -> Result<GitOutput, BuildSourceError> {
        self.runner
            .run(invocation)
            .await
            .map_err(|error| BuildSourceError::unavailable(error.to_string()))
    }

    async fn run_local(&self, invocation: GitInvocation) -> Result<GitOutput, BuildSourceError> {
        self.runner
            .run(invocation)
            .await
            .map_err(|error| BuildSourceError::rejected(error.to_string()))
    }
}

#[async_trait]
impl BuildSourceProvider for LocalBuildSourceProvider {
    async fn prepare(
        &self,
        build_id: &BuildId,
        source: &BuildSource,
        resolved_revision: Option<&str>,
    ) -> Result<PreparedBuildSource, BuildSourceError> {
        match source {
            BuildSource::Git {
                repository,
                revision,
            } => {
                self.prepare_git(build_id, repository, revision, resolved_revision)
                    .await
            }
            BuildSource::Tarball { archive_id } => {
                self.prepare_archive(archive_id, resolved_revision).await
            }
        }
    }

    async fn cleanup(&self, build_id: &BuildId) -> Result<(), BuildSourceError> {
        let root = ensure_root(&self.workspace_root).await?;
        let workspace = root.join(build_id.as_str());
        match tokio::fs::symlink_metadata(&workspace).await {
            Ok(metadata) if metadata.file_type().is_dir() => tokio::fs::remove_dir_all(&workspace)
                .await
                .map_err(|error| io_unavailable("remove build workspace", &workspace, error)),
            Ok(_) => Err(BuildSourceError::rejected(format!(
                "refusing to remove non-directory build workspace `{}`",
                workspace.display()
            ))),
            Err(error) if error.kind() == ErrorKind::NotFound => Ok(()),
            Err(error) => Err(io_unavailable("inspect build workspace", &workspace, error)),
        }
    }
}

fn validate_requested_revision(revision: &str) -> Result<(), BuildSourceError> {
    if revision.trim().is_empty()
        || revision.starts_with('-')
        || revision.chars().any(char::is_control)
    {
        Err(BuildSourceError::rejected(
            "Git revision must be non-empty and must not begin with an option",
        ))
    } else {
        Ok(())
    }
}

fn validate_resolved_revision(revision: &str) -> Result<(), BuildSourceError> {
    if matches!(revision.len(), 40 | 64) && revision.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        Ok(())
    } else {
        Err(BuildSourceError::rejected(format!(
            "resolved Git revision `{revision}` is not a complete object ID"
        )))
    }
}

fn parse_revision(stdout: &str) -> Result<String, BuildSourceError> {
    let revision = stdout.trim();
    validate_resolved_revision(revision)?;
    Ok(revision.to_ascii_lowercase())
}

#[cfg(test)]
#[path = "tests/local_source.rs"]
mod tests;
