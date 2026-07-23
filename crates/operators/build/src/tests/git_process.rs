use kernel_api::SecretValue;

use crate::git_process::{GitInvocation, git_environment, safe_git_error, secret_fragments};

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
