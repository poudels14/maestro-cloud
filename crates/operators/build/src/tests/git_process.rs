use kernel_api::SecretValue;

use crate::git_process::{
    GitInvocation, git_environment, normalize_repository, safe_git_error, secret_fragments,
};

#[test]
fn repository_normalization_uses_git_remote_grammar_and_rejects_credentials() {
    assert_eq!(
        normalize_repository("git@github.com:acme/api.git")
            .ok()
            .as_deref(),
        Some("https://github.com/acme/api.git")
    );
    assert_eq!(
        normalize_repository("https://github.com/acme/api.git")
            .ok()
            .as_deref(),
        Some("https://github.com/acme/api.git")
    );
    assert!(normalize_repository("https://user@github.com/acme/api.git").is_err());
    assert!(normalize_repository("ssh://git@github.com/acme/api.git").is_err());
    assert!(normalize_repository("git@github.com:-upload-pack=bad").is_err());
}

#[test]
fn git_errors_redact_header_and_encoded_credentials() {
    let invocation = GitInvocation::new(["fetch"]).with_environment(vec![(
        "GIT_CONFIG_VALUE_1".into(),
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

    assert_eq!(environment.len(), 6);
    assert_eq!(
        environment.first().and_then(|(key, _)| key.to_str()),
        Some("GIT_TERMINAL_PROMPT")
    );
    let values = environment
        .iter()
        .map(|(key, value)| (key.to_string_lossy().into_owned(), value.expose()))
        .collect::<std::collections::BTreeMap<_, _>>();
    assert_eq!(values.get("GIT_ASKPASS").copied(), Some("/usr/bin/false"));
    assert_eq!(values.get("SSH_ASKPASS").copied(), Some("/usr/bin/false"));
    assert_eq!(values.get("GIT_CONFIG_COUNT").copied(), Some("1"));
    assert_eq!(
        values.get("GIT_CONFIG_KEY_0").copied(),
        Some("credential.helper")
    );
    assert_eq!(values.get("GIT_CONFIG_VALUE_0").copied(), Some(""));
    Ok(())
}
