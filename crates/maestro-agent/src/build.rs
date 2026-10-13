use std::collections::BTreeMap;
use std::path::{Path, PathBuf};
use std::process::Stdio;

use tokio::process::Command;

const LOCAL_REGISTRY: &str = "maestro.local";
const FAILURE_TAIL_LINES: usize = 20;

#[derive(Clone, Debug)]
pub struct BuildKitSettings {
    pub buildctl: PathBuf,
    pub address: String,
    pub work_dir: PathBuf,
    pub log_dir: PathBuf,
    pub git_token: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildRequest {
    pub deployment_id: String,
    pub service_id: String,
    pub repo: Option<String>,
    pub branch: Option<String>,
    pub context_dir: Option<PathBuf>,
    pub dockerfile: String,
    pub registry: Option<String>,
    pub build_args: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, String>,
}

pub struct BuildKitBuilder {
    settings: BuildKitSettings,
}

impl BuildKitBuilder {
    pub fn new(settings: BuildKitSettings) -> Self {
        Self { settings }
    }

    pub async fn build(&self, request: &BuildRequest) -> Result<String, String> {
        let workspace = self.settings.work_dir.join(&request.deployment_id);
        let log_path = self
            .settings
            .log_dir
            .join(format!("{}.log", request.deployment_id));
        let result = self.run(request, &workspace, &log_path).await;
        let _ = tokio::fs::remove_dir_all(&workspace).await;
        match result {
            Ok(image) => Ok(image),
            Err(error) => {
                let tail = log_tail(&log_path).await;
                Err(match tail.is_empty() {
                    true => error,
                    false => format!("{error}\n{tail}"),
                })
            }
        }
    }

    async fn run(
        &self,
        request: &BuildRequest,
        workspace: &Path,
        log_path: &Path,
    ) -> Result<String, String> {
        let _ = tokio::fs::remove_dir_all(workspace).await;
        tokio::fs::create_dir_all(workspace)
            .await
            .map_err(|error| format!("failed to create build workspace: {error}"))?;
        tokio::fs::create_dir_all(&self.settings.log_dir)
            .await
            .map_err(|error| format!("failed to create build log dir: {error}"))?;
        let log = std::fs::File::create(log_path)
            .map_err(|error| format!("failed to create build log: {error}"))?;
        let context = match (&request.repo, &request.context_dir) {
            (Some(repo), _) => {
                let checkout = workspace.join("src");
                self.clone_repo(repo, request.branch.as_deref(), &checkout, &log)
                    .await?;
                checkout
            }
            (None, Some(context_dir)) => context_dir.clone(),
            (None, None) => Err("build needs a repo or a local context".to_string())?,
        };
        let image = image_name(request);
        let arguments = buildctl_args(&self.settings.address, request, &context, &image);
        let status = Command::new(&self.settings.buildctl)
            .args(&arguments)
            .envs(&request.secrets)
            .stdin(Stdio::null())
            .stdout(clone_handle(&log)?)
            .stderr(clone_handle(&log)?)
            .status()
            .await
            .map_err(|error| format!("failed to run buildctl: {error}"))?;
        match status.success() {
            true => Ok(image),
            false => Err(format!("buildctl exited with {status}")),
        }
    }

    async fn clone_repo(
        &self,
        repo: &str,
        branch: Option<&str>,
        destination: &Path,
        log: &std::fs::File,
    ) -> Result<(), String> {
        let url = authenticated_url(repo, self.settings.git_token.as_deref());
        let branch_args = branch
            .map(|branch| vec!["--branch".to_string(), branch.to_string()])
            .unwrap_or_default();
        let status = Command::new("git")
            .args(["clone", "--depth", "1", "--single-branch"])
            .args(branch_args)
            .arg(url)
            .arg(destination)
            .env("GIT_TERMINAL_PROMPT", "0")
            .stdin(Stdio::null())
            .stdout(clone_handle(log)?)
            .stderr(clone_handle(log)?)
            .status()
            .await
            .map_err(|error| format!("failed to run git: {error}"))?;
        match status.success() {
            true => Ok(()),
            false => Err(format!("git clone of {repo} failed")),
        }
    }
}

pub fn image_name(request: &BuildRequest) -> String {
    let registry = request.registry.as_deref().unwrap_or(LOCAL_REGISTRY);
    format!(
        "{}/{}:{}",
        registry.trim_end_matches('/'),
        request.service_id,
        request.deployment_id
    )
}

fn buildctl_args(
    address: &str,
    request: &BuildRequest,
    context: &Path,
    image: &str,
) -> Vec<String> {
    let dockerfile = context.join(&request.dockerfile);
    let dockerfile_dir = dockerfile.parent().unwrap_or(context).to_path_buf();
    let filename = dockerfile
        .file_name()
        .map(|name| name.to_string_lossy().to_string())
        .unwrap_or_else(|| "Dockerfile".to_string());
    let build_args = request
        .build_args
        .iter()
        .flat_map(|(name, value)| ["--opt".to_string(), format!("build-arg:{name}={value}")]);
    let secrets = request
        .secrets
        .keys()
        .flat_map(|name| ["--secret".to_string(), format!("id={name},env={name}")]);
    [
        "--addr".to_string(),
        address.to_string(),
        "build".to_string(),
        "--frontend".to_string(),
        "dockerfile.v0".to_string(),
        "--local".to_string(),
        format!("context={}", context.display()),
        "--local".to_string(),
        format!("dockerfile={}", dockerfile_dir.display()),
        "--opt".to_string(),
        format!("filename={filename}"),
    ]
    .into_iter()
    .chain(build_args)
    .chain(secrets)
    .chain([
        "--output".to_string(),
        format!(
            "type=image,name={image},unpack=true,push={}",
            request.registry.is_some()
        ),
    ])
    .collect()
}

fn authenticated_url(repo: &str, token: Option<&str>) -> String {
    match (repo.strip_prefix("https://github.com/"), token) {
        (Some(path), Some(token)) => format!("https://x-access-token:{token}@github.com/{path}"),
        _ => repo.to_string(),
    }
}

fn clone_handle(log: &std::fs::File) -> Result<std::fs::File, String> {
    log.try_clone()
        .map_err(|error| format!("failed to share build log: {error}"))
}

async fn log_tail(path: &Path) -> String {
    let contents = tokio::fs::read_to_string(path).await.unwrap_or_default();
    let lines = contents.lines().collect::<Vec<_>>();
    lines[lines.len().saturating_sub(FAILURE_TAIL_LINES)..].join("\n")
}

#[cfg(test)]
mod tests {
    use super::*;

    fn request() -> BuildRequest {
        BuildRequest {
            deployment_id: "d1".to_string(),
            service_id: "api".to_string(),
            repo: Some("https://github.com/acme/api.git".to_string()),
            branch: Some("main".to_string()),
            context_dir: None,
            dockerfile: "docker/Dockerfile.prod".to_string(),
            registry: None,
            build_args: BTreeMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            secrets: BTreeMap::from([("NPM_TOKEN".to_string(), "secret".to_string())]),
        }
    }

    #[test]
    fn builds_buildctl_arguments() {
        let arguments = buildctl_args(
            "unix:///run/buildkit/buildkitd.sock",
            &request(),
            Path::new("/work/d1/src"),
            "maestro.local/api:d1",
        );
        let joined = arguments.join(" ");
        assert!(joined.contains("--local context=/work/d1/src"));
        assert!(joined.contains("--local dockerfile=/work/d1/src/docker"));
        assert!(joined.contains("--opt filename=Dockerfile.prod"));
        assert!(joined.contains("--opt build-arg:RUST_LOG=info"));
        assert!(joined.contains("--secret id=NPM_TOKEN,env=NPM_TOKEN"));
        assert!(!joined.contains("secret,"));
        assert!(joined.ends_with("--output type=image,name=maestro.local/api:d1,unpack=true,push=false"));
    }

    #[test]
    fn names_images_by_registry_service_and_deployment() {
        let mut pushed = request();
        assert_eq!(image_name(&pushed), "maestro.local/api:d1");
        pushed.registry = Some("ghcr.io/acme/".to_string());
        assert_eq!(image_name(&pushed), "ghcr.io/acme/api:d1");
    }

    #[test]
    fn adds_token_only_to_github_https_urls() {
        assert_eq!(
            authenticated_url("https://github.com/acme/api.git", Some("tok")),
            "https://x-access-token:tok@github.com/acme/api.git"
        );
        assert_eq!(
            authenticated_url("git@github.com:acme/api.git", Some("tok")),
            "git@github.com:acme/api.git"
        );
        assert_eq!(
            authenticated_url("https://github.com/acme/api.git", None),
            "https://github.com/acme/api.git"
        );
    }
}
