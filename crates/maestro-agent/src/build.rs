use std::collections::BTreeMap;
use std::os::unix::fs::OpenOptionsExt;
use std::path::{Path, PathBuf};
use std::process::{ExitStatus, Stdio};
use std::sync::Arc;

use base64::Engine;
use base64::engine::general_purpose::STANDARD as BASE64;
use maestro_core::GitCommit;
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncWriteExt, BufReader};
use tokio::process::Command;
use tokio::sync::Mutex;

const GITHUB_HOST: &str = "github.com";
const GIT_SAFETY_ENV: [(&str, &str); 3] = [
    ("GIT_TERMINAL_PROMPT", "0"),
    ("GIT_ALLOW_PROTOCOL", "https:ssh"),
    ("GIT_PROTOCOL_FROM_USER", "0"),
];
const LOCAL_REGISTRY: &str = "maestro.local";
const FAILURE_TAIL_LINES: usize = 20;
const GIT_TOKEN_SECRET: &str = "GH_TOKEN";
const DEPOT_PROGRAM: &str = "depot";
const DEPOT_TOKEN_ENV: &str = "DEPOT_TOKEN";

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
    pub image: Option<String>,
    pub build_args: BTreeMap<String, String>,
    pub secrets: BTreeMap<String, String>,
    pub depot: Option<DepotBuild>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DepotBuild {
    pub project: String,
    pub token: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BuildOutput {
    pub image: String,
    pub commit: Option<GitCommit>,
    pub archive: Option<PathBuf>,
}

pub struct BuildKitBuilder {
    settings: BuildKitSettings,
}

impl BuildKitBuilder {
    pub fn new(settings: BuildKitSettings) -> Self {
        Self { settings }
    }

    pub async fn build(&self, request: &BuildRequest) -> Result<BuildOutput, String> {
        let workspace = self.settings.work_dir.join(&request.deployment_id);
        let log_path = self
            .settings
            .log_dir
            .join(format!("{}.log", request.deployment_id));
        let result = self.run(request, &workspace, &log_path).await;
        let _ = tokio::fs::remove_dir_all(&workspace).await;
        match result {
            Ok(output) => Ok(output),
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
    ) -> Result<BuildOutput, String> {
        let _ = tokio::fs::remove_dir_all(workspace).await;
        tokio::fs::create_dir_all(workspace)
            .await
            .map_err(|error| format!("failed to create build workspace: {error}"))?;
        tokio::fs::create_dir_all(&self.settings.log_dir)
            .await
            .map_err(|error| format!("failed to create build log dir: {error}"))?;
        let log = std::fs::OpenOptions::new()
            .write(true)
            .create(true)
            .truncate(true)
            .mode(0o600)
            .open(log_path)
            .map_err(|error| format!("failed to create build log: {error}"))?;
        let mut commit = None;
        let context = match (&request.repo, &request.context_dir) {
            (Some(repo), _) => {
                let checkout = workspace.join("src");
                let cluster_token = self
                    .settings
                    .git_token
                    .as_ref()
                    .filter(|_| repo_host(repo).as_deref() == Some(GITHUB_HOST));
                let token = request.secrets.get(GIT_TOKEN_SECRET).or(cluster_token);
                clone_repo(repo, request.branch.as_deref(), token, &checkout, &log).await?;
                commit = head_commit(&checkout).await;
                checkout
            }
            (None, Some(context_dir)) => context_dir.clone(),
            (None, None) => Err("build needs a repo or a local context".to_string())?,
        };
        let image = image_name(request);
        let archive = (request.depot.is_some() && request.registry.is_none()).then(|| {
            self.settings
                .work_dir
                .join(format!("{}.oci.tar", request.deployment_id))
        });
        let (program, arguments, token) = match &request.depot {
            Some(depot) => (
                PathBuf::from(DEPOT_PROGRAM),
                depot_args(request, depot, &context, &image, archive.as_deref()),
                Some(depot.token.clone()),
            ),
            None => (
                self.settings.buildctl.clone(),
                buildctl_args(&self.settings.address, request, &context, &image),
                None,
            ),
        };
        let status = run_logged(
            Command::new(&program)
                .args(&arguments)
                .envs(&request.secrets)
                .envs(token.map(|token| (DEPOT_TOKEN_ENV, token))),
            &log,
        )
        .await
        .map_err(|error| format!("failed to run {}: {error}", program.display()))?;
        match status.success() {
            true => Ok(BuildOutput {
                image,
                commit,
                archive,
            }),
            false => Err(format!("{} exited with {status}", program.display())),
        }
    }
}

pub fn image_name(request: &BuildRequest) -> String {
    let registry = request.registry.as_deref().unwrap_or(LOCAL_REGISTRY);
    request.image.clone().unwrap_or_else(|| {
        format!(
            "{}/{}:{}",
            registry.trim_end_matches('/'),
            request.service_id,
            request.deployment_id
        )
    })
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

fn depot_args(
    request: &BuildRequest,
    depot: &DepotBuild,
    context: &Path,
    image: &str,
    archive: Option<&Path>,
) -> Vec<String> {
    let output = match archive {
        Some(archive) => vec![
            "--output".to_string(),
            format!("type=oci,dest={},name={image}", archive.display()),
        ],
        None => vec!["--push".to_string()],
    };
    let build_args = request
        .build_args
        .iter()
        .flat_map(|(name, value)| ["--build-arg".to_string(), format!("{name}={value}")]);
    let secrets = request
        .secrets
        .keys()
        .flat_map(|name| ["--secret".to_string(), format!("id={name},env={name}")]);
    [
        "build".to_string(),
        "--project".to_string(),
        depot.project.clone(),
        "--platform".to_string(),
        host_platform(),
        "-t".to_string(),
        image.to_string(),
        "-f".to_string(),
        context.join(&request.dockerfile).display().to_string(),
    ]
    .into_iter()
    .chain(output)
    .chain(build_args)
    .chain(secrets)
    .chain([context.display().to_string()])
    .collect()
}

fn host_platform() -> String {
    let architecture = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        other => other,
    };
    format!("linux/{architecture}")
}

async fn clone_repo(
    repo: &str,
    branch: Option<&str>,
    token: Option<&String>,
    destination: &Path,
    log: &std::fs::File,
) -> Result<(), String> {
    let (url, auth_env) = git_auth(repo, token);
    let branch_args = branch
        .map(|branch| vec!["--branch".to_string(), branch.to_string()])
        .unwrap_or_default();
    let mut command = Command::new("git");
    command
        .args(["clone", "--depth", "1", "--single-branch"])
        .args(branch_args)
        .arg("--")
        .arg(url)
        .arg(destination)
        .envs(auth_env)
        .envs(GIT_SAFETY_ENV);
    let status = run_logged(&mut command, log)
        .await
        .map_err(|error| format!("failed to run git: {error}"))?;
    match status.success() {
        true => Ok(()),
        false => Err(format!("git clone of {repo} failed")),
    }
}

pub async fn remote_head(
    repo: &str,
    branch: &str,
    token: Option<&String>,
) -> Result<String, String> {
    let (url, auth_env) = git_auth(repo, token);
    let output = Command::new("git")
        .args(["ls-remote", "--", &url, &format!("refs/heads/{branch}")])
        .envs(auth_env)
        .envs(GIT_SAFETY_ENV)
        .stdin(Stdio::null())
        .output()
        .await
        .map_err(|error| format!("failed to run git: {error}"))?;
    let stdout = String::from_utf8_lossy(&output.stdout);
    match (output.status.success(), stdout.split_whitespace().next()) {
        (true, Some(sha)) => Ok(sha.to_string()),
        (true, None) => Err(format!("branch {branch} not found in {repo}")),
        (false, _) => Err(format!(
            "git ls-remote {repo} failed: {}",
            String::from_utf8_lossy(&output.stderr).trim()
        )),
    }
}

async fn head_commit(checkout: &Path) -> Option<GitCommit> {
    let output = Command::new("git")
        .arg("-C")
        .arg(checkout)
        .args(["log", "-1", "--format=%H%n%s"])
        .output()
        .await
        .ok()
        .filter(|output| output.status.success())?;
    let text = String::from_utf8_lossy(&output.stdout);
    let mut lines = text.lines();
    Some(GitCommit {
        reference: lines.next()?.to_string(),
        message: lines.next().unwrap_or_default().to_string(),
    })
}

pub fn repo_host(repo: &str) -> Option<String> {
    repo.strip_prefix("git@")
        .and_then(|rest| rest.split_once(':'))
        .map(|(host, _)| host)
        .or_else(|| {
            repo.strip_prefix("https://")
                .and_then(|rest| rest.split('/').next())
        })
        .map(str::to_ascii_lowercase)
}

fn git_auth(repo: &str, token: Option<&String>) -> (String, Vec<(String, String)>) {
    let https = repo
        .strip_prefix("git@")
        .and_then(|rest| rest.split_once(':'))
        .map(|(host, path)| format!("https://{host}/{path}"))
        .unwrap_or_else(|| repo.to_string());
    let host = https
        .strip_prefix("https://")
        .and_then(|rest| rest.split('/').next())
        .map(str::to_string);
    match (token, host) {
        (Some(token), Some(host)) => {
            let credentials = BASE64.encode(format!("x-access-token:{token}"));
            let env = vec![
                ("GIT_CONFIG_COUNT".to_string(), "1".to_string()),
                (
                    "GIT_CONFIG_KEY_0".to_string(),
                    format!("http.https://{host}/.extraHeader"),
                ),
                (
                    "GIT_CONFIG_VALUE_0".to_string(),
                    format!("Authorization: basic {credentials}"),
                ),
            ];
            (https, env)
        }
        _ => (repo.to_string(), Vec::new()),
    }
}

async fn run_logged(command: &mut Command, log: &std::fs::File) -> std::io::Result<ExitStatus> {
    let mut child = command
        .stdin(Stdio::null())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .spawn()?;
    let log = Arc::new(Mutex::new(tokio::fs::File::from_std(log.try_clone()?)));
    tokio::join!(
        copy_timestamped(child.stdout.take(), log.clone()),
        copy_timestamped(child.stderr.take(), log.clone()),
    );
    child.wait().await
}

async fn copy_timestamped(
    output: Option<impl AsyncRead + Unpin>,
    log: Arc<Mutex<tokio::fs::File>>,
) {
    if let Some(output) = output {
        let mut lines = BufReader::new(output).split(b'\n');
        while let Ok(Some(line)) = lines.next_segment().await {
            let stamped = format!(
                "{} {}\n",
                chrono::Utc::now().format("%Y-%m-%dT%H:%M:%S%.3fZ"),
                strip_ansi(String::from_utf8_lossy(&line).trim_end_matches('\r'))
            );
            let _ = log.lock().await.write_all(stamped.as_bytes()).await;
        }
    }
}

async fn log_tail(path: &Path) -> String {
    let contents = tokio::fs::read_to_string(path).await.unwrap_or_default();
    let lines = contents.lines().collect::<Vec<_>>();
    lines[lines.len().saturating_sub(FAILURE_TAIL_LINES)..].join("\n")
}

fn strip_ansi(text: &str) -> String {
    let mut plain = String::with_capacity(text.len());
    let mut characters = text.chars().peekable();
    while let Some(character) = characters.next() {
        match (character, characters.peek()) {
            ('\u{1b}', Some('[')) => {
                characters.next();
                while characters
                    .next()
                    .is_some_and(|code| !('@'..='~').contains(&code))
                {}
            }
            ('\u{1b}', Some(']')) => {
                while characters
                    .next()
                    .is_some_and(|code| code != '\u{7}' && code != '\\')
                {}
            }
            ('\u{1b}', Some(_)) => {
                characters.next();
            }
            ('\u{1b}', None) => {}
            (other, _) => plain.push(other),
        }
    }
    plain
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn strips_terminal_colors_and_links() {
        assert_eq!(
            strip_ansi(
                "\u{1b}[1;36mBuild Summary\u{1b}[0m: \u{1b}[4;39mhttps://depot.dev/x\u{1b}[0m"
            ),
            "Build Summary: https://depot.dev/x"
        );
        assert_eq!(
            strip_ansi("\u{1b}]8;;https://a\u{7}link\u{1b}]8;;\u{7} done\u{1b}"),
            "link done"
        );
        assert_eq!(strip_ansi("plain [33m text"), "plain [33m text");
    }

    #[tokio::test]
    async fn timestamps_every_line_of_stdout_and_stderr() {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("build.log");
        let log = std::fs::File::create(&path).unwrap();
        let status = run_logged(
            Command::new("sh").args(["-c", "echo out; echo err >&2; printf 'no newline'"]),
            &log,
        )
        .await
        .unwrap();
        assert!(status.success());
        let contents = std::fs::read_to_string(&path).unwrap();
        let mut lines = contents
            .lines()
            .map(|line| line.split_once(' ').unwrap())
            .collect::<Vec<_>>();
        lines.sort_by_key(|(_, text)| *text);
        assert_eq!(
            lines.iter().map(|(_, text)| *text).collect::<Vec<_>>(),
            ["err", "no newline", "out"]
        );
        assert!(lines.iter().all(|(stamp, _)| {
            chrono::DateTime::parse_from_rfc3339(stamp).is_ok() && stamp.ends_with('Z')
        }));
    }

    fn request() -> BuildRequest {
        BuildRequest {
            deployment_id: "d1".to_string(),
            service_id: "api".to_string(),
            repo: Some("https://github.com/acme/api.git".to_string()),
            branch: Some("main".to_string()),
            context_dir: None,
            dockerfile: "docker/Dockerfile.prod".to_string(),
            registry: None,
            image: None,
            build_args: BTreeMap::from([("RUST_LOG".to_string(), "info".to_string())]),
            secrets: BTreeMap::from([("NPM_TOKEN".to_string(), "secret".to_string())]),
            depot: None,
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
        assert!(
            joined
                .ends_with("--output type=image,name=maestro.local/api:d1,unpack=true,push=false")
        );
    }

    #[test]
    fn names_images_by_registry_service_and_deployment() {
        let mut pushed = request();
        assert_eq!(image_name(&pushed), "maestro.local/api:d1");
        pushed.registry = Some("ghcr.io/acme/".to_string());
        assert_eq!(image_name(&pushed), "ghcr.io/acme/api:d1");
    }

    #[test]
    fn authenticates_git_with_an_extra_header() {
        let token = "tok".to_string();
        let (url, env) = git_auth("git@github.com:acme/api.git", Some(&token));
        assert_eq!(url, "https://github.com/acme/api.git");
        assert_eq!(
            env[1],
            (
                "GIT_CONFIG_KEY_0".to_string(),
                "http.https://github.com/.extraHeader".to_string()
            )
        );
        assert_eq!(
            env[2].1,
            format!(
                "Authorization: basic {}",
                BASE64.encode("x-access-token:tok")
            )
        );
        let (url, env) = git_auth("https://gitlab.com/acme/api.git", None);
        assert_eq!(url, "https://gitlab.com/acme/api.git");
        assert!(env.is_empty());
    }

    #[test]
    fn builds_depot_arguments_for_push_and_archive_output() {
        let depot = DepotBuild {
            project: "proj1".to_string(),
            token: "tok".to_string(),
        };
        let context = Path::new("/work/src");
        let archived = depot_args(
            &request(),
            &depot,
            context,
            "maestro.local/api:d1",
            Some(Path::new("/work/d1.oci.tar")),
        )
        .join(" ");
        assert!(archived.starts_with("build --project proj1 --platform linux/"));
        assert!(archived.contains("-t maestro.local/api:d1"));
        assert!(
            archived.contains("--output type=oci,dest=/work/d1.oci.tar,name=maestro.local/api:d1")
        );
        assert!(archived.contains("--build-arg RUST_LOG=info"));
        assert!(archived.contains("--secret id=NPM_TOKEN,env=NPM_TOKEN"));
        assert!(archived.ends_with(" /work/src"));
        assert!(!archived.contains("tok"));

        let pushed = depot_args(&request(), &depot, context, "ghcr.io/acme/api:d1", None).join(" ");
        assert!(pushed.contains("--push"));
        assert!(!pushed.contains("--output"));
    }
}
