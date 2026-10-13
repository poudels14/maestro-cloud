use std::future::Future;
use std::process::Stdio;

use tokio::io::AsyncWriteExt;
use tokio::process::Command;

use crate::error::AgentError;

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct CommandOutput {
    pub success: bool,
    pub stdout: String,
    pub stderr: String,
}

pub trait CommandRunner: Send + Sync + 'static {
    fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> impl Future<Output = Result<CommandOutput, AgentError>> + Send;
}

#[derive(Clone, Copy, Debug, Default)]
pub struct HostCommandRunner;

impl CommandRunner for HostCommandRunner {
    async fn run(
        &self,
        program: &str,
        args: &[String],
        stdin: Option<&str>,
    ) -> Result<CommandOutput, AgentError> {
        let mut child = Command::new(program)
            .args(args)
            .stdin(match stdin {
                Some(_) => Stdio::piped(),
                None => Stdio::null(),
            })
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
            .map_err(|error| AgentError::Host(format!("failed to run {program}: {error}")))?;
        if let (Some(input), Some(mut pipe)) = (stdin, child.stdin.take()) {
            pipe.write_all(input.as_bytes()).await.map_err(|error| {
                AgentError::Host(format!("failed to write to {program}: {error}"))
            })?;
        }
        let output = child
            .wait_with_output()
            .await
            .map_err(|error| AgentError::Host(format!("failed to wait for {program}: {error}")))?;
        Ok(CommandOutput {
            success: output.status.success(),
            stdout: String::from_utf8_lossy(&output.stdout).into_owned(),
            stderr: String::from_utf8_lossy(&output.stderr).into_owned(),
        })
    }
}
