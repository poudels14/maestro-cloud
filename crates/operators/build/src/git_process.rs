use std::ffi::OsString;
use std::path::PathBuf;
use std::process::Stdio;

use async_trait::async_trait;
use base64::Engine;
use kernel_api::SecretValue;
use tokio::io::{AsyncRead, AsyncReadExt};
use tokio::process::Command;

use crate::BuildSourceError;

const MAX_GIT_ERROR_CHARS: usize = 2_048;
const MAX_GIT_STDOUT_BYTES: usize = 8 * 1_024;
const MAX_GIT_STDERR_BYTES: usize = 64 * 1_024;

#[derive(Clone)]
pub(crate) struct GitInvocation {
    pub(crate) arguments: Vec<OsString>,
    pub(crate) directory: Option<PathBuf>,
    pub(crate) environment: Vec<(OsString, SecretValue)>,
}

impl GitInvocation {
    pub(crate) fn new(arguments: impl IntoIterator<Item = impl Into<OsString>>) -> Self {
        Self {
            arguments: arguments.into_iter().map(Into::into).collect(),
            directory: None,
            environment: Vec::new(),
        }
    }

    pub(crate) fn in_directory(mut self, directory: PathBuf) -> Self {
        self.directory = Some(directory);
        self
    }

    pub(crate) fn with_environment(mut self, environment: Vec<(OsString, SecretValue)>) -> Self {
        self.environment = environment;
        self
    }
}

pub(crate) struct GitOutput {
    pub(crate) stdout: String,
}

#[derive(Debug, thiserror::Error)]
pub(crate) enum GitRunError {
    #[error("git could not be executed: {message}")]
    Unavailable { message: String },
    #[error("git command failed: {message}")]
    Exit { message: String },
}

#[async_trait]
pub(crate) trait GitRunner: Send + Sync {
    async fn run(&self, invocation: GitInvocation) -> Result<GitOutput, GitRunError>;
}

pub(crate) struct ProcessGitRunner;

#[async_trait]
impl GitRunner for ProcessGitRunner {
    async fn run(&self, invocation: GitInvocation) -> Result<GitOutput, GitRunError> {
        let redactions = secret_fragments(&invocation);
        let mut command = Command::new("git");
        command
            .args(&invocation.arguments)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        if let Some(directory) = invocation.directory {
            command.current_dir(directory);
        }
        for (key, value) in invocation.environment {
            command.env(key, value.expose());
        }
        let mut child = command.spawn().map_err(|error| GitRunError::Unavailable {
            message: error.to_string(),
        })?;
        let stdout = child
            .stdout
            .take()
            .ok_or_else(|| GitRunError::Unavailable {
                message: "git stdout pipe was not created".to_string(),
            })?;
        let stderr = child
            .stderr
            .take()
            .ok_or_else(|| GitRunError::Unavailable {
                message: "git stderr pipe was not created".to_string(),
            })?;
        let output = tokio::try_join!(
            child.wait(),
            read_bounded(stdout, MAX_GIT_STDOUT_BYTES),
            read_bounded(stderr, MAX_GIT_STDERR_BYTES)
        );
        let (status, stdout, stderr) = match output {
            Ok(output) => output,
            Err(error) => {
                let _ = child.kill().await;
                let _ = child.wait().await;
                return Err(GitRunError::Exit {
                    message: format!("git output could not be read safely: {error}"),
                });
            }
        };
        if !status.success() {
            return Err(GitRunError::Exit {
                message: safe_git_error(&stderr, &redactions),
            });
        }
        let stdout = String::from_utf8(stdout).map_err(|_| GitRunError::Exit {
            message: "git returned non-UTF-8 output".to_string(),
        })?;
        Ok(GitOutput { stdout })
    }
}

pub(crate) fn git_environment(
    repository: &str,
    github_token: Option<&SecretValue>,
) -> Result<Vec<(OsString, SecretValue)>, BuildSourceError> {
    let mut environment = vec![(OsString::from("GIT_TERMINAL_PROMPT"), SecretValue::new("0"))];
    let Some(token) = github_token else {
        return Ok(environment);
    };
    let host = repository
        .strip_prefix("https://")
        .and_then(|rest| rest.split('/').next())
        .ok_or_else(|| BuildSourceError::rejected("Git repository must use HTTPS"))?;
    if host.split(':').next() != Some("github.com") {
        return Ok(environment);
    }
    let credentials = base64::engine::general_purpose::STANDARD
        .encode(format!("x-access-token:{}", token.expose()));
    environment.extend([
        (OsString::from("GIT_CONFIG_COUNT"), SecretValue::new("1")),
        (
            OsString::from("GIT_CONFIG_KEY_0"),
            SecretValue::new(format!("http.https://{host}/.extraHeader")),
        ),
        (
            OsString::from("GIT_CONFIG_VALUE_0"),
            SecretValue::new(format!("Authorization: basic {credentials}")),
        ),
    ]);
    Ok(environment)
}

pub(crate) fn normalize_repository(repository: &str) -> Result<String, BuildSourceError> {
    let repository = repository.trim();
    let normalized = if let Some(rest) = repository.strip_prefix("git@") {
        let (host, path) = rest.split_once(':').ok_or_else(|| {
            BuildSourceError::rejected("Git SSH repository has no host/path separator")
        })?;
        format!("https://{host}/{path}")
    } else {
        repository.to_string()
    };
    let (authority, _) = normalized
        .strip_prefix("https://")
        .and_then(|rest| rest.split_once('/'))
        .filter(|(authority, path)| !authority.is_empty() && !path.is_empty())
        .ok_or_else(|| {
            BuildSourceError::rejected(
                "Git repository must use HTTPS and include a repository path",
            )
        })?;
    if authority.contains('@')
        || authority.contains('\\')
        || normalized.chars().any(char::is_whitespace)
        || normalized.chars().any(char::is_control)
    {
        return Err(BuildSourceError::rejected(
            "Git repository must not contain credentials or control characters",
        ));
    }
    Ok(normalized)
}

async fn read_bounded(
    mut reader: impl AsyncRead + Unpin,
    limit: usize,
) -> Result<Vec<u8>, std::io::Error> {
    let mut output = Vec::new();
    let mut buffer = [0_u8; 8 * 1_024];
    loop {
        let count = reader.read(&mut buffer).await?;
        if count == 0 {
            return Ok(output);
        }
        if output.len().saturating_add(count) > limit {
            return Err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                format!("git output exceeded {limit} bytes"),
            ));
        }
        let chunk = buffer.get(..count).ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "git reader returned an invalid byte count",
            )
        })?;
        output.extend_from_slice(chunk);
    }
}

fn secret_fragments(invocation: &GitInvocation) -> Vec<String> {
    invocation
        .environment
        .iter()
        .filter(|(key, _)| key == "GIT_CONFIG_VALUE_0")
        .flat_map(|(_, value)| {
            let header = value.expose().to_string();
            let credentials = header.split_whitespace().next_back().map(str::to_string);
            std::iter::once(header).chain(credentials)
        })
        .collect()
}

fn safe_git_error(stderr: &[u8], redactions: &[String]) -> String {
    let mut redacted = String::from_utf8_lossy(stderr).into_owned();
    for secret in redactions {
        if !secret.is_empty() {
            redacted = redacted.replace(secret, "[REDACTED]");
        }
    }
    let safe = redacted
        .chars()
        .map(|character| {
            if character.is_control() {
                ' '
            } else {
                character
            }
        })
        .take(MAX_GIT_ERROR_CHARS)
        .collect::<String>();
    if safe.trim().is_empty() {
        "git exited unsuccessfully".to_string()
    } else {
        safe.trim().to_string()
    }
}

#[cfg(test)]
mod tests {
    use super::{GitInvocation, git_environment, safe_git_error, secret_fragments};
    use kernel_api::SecretValue;

    #[test]
    fn git_errors_redact_header_and_encoded_credentials() {
        let invocation = GitInvocation::new(["fetch"]).with_environment(vec![(
            "GIT_CONFIG_VALUE_0".into(),
            SecretValue::new("Authorization: basic encoded-secret"),
        )]);
        let redactions = secret_fragments(&invocation);
        let safe = safe_git_error(
            b"server echoed Authorization: basic encoded-secret and encoded-secret\n",
            &redactions,
        );

        assert_eq!(safe, "server echoed [REDACTED] and [REDACTED]");
    }

    #[test]
    fn github_token_is_not_sent_to_other_repository_hosts() -> Result<(), crate::BuildSourceError> {
        let environment = git_environment(
            "https://git.example.com/acme/api.git",
            Some(&SecretValue::new("github-secret")),
        )?;

        assert_eq!(environment.len(), 1);
        assert_eq!(
            environment.first().and_then(|(key, _)| key.to_str()),
            Some("GIT_TERMINAL_PROMPT")
        );
        Ok(())
    }
}
