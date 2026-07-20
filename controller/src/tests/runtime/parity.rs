use crate::config::RuntimeType;
use crate::runtime::{self, RunSpec};
use crate::supervisor::JobCommand;
use crate::utils::crypto::SecretString;

fn run_spec() -> RunSpec {
    RunSpec {
        container_name: "test-container".to_string(),
        hostname: "test-host".to_string(),
        dns_domain: Some("test.maestro.internal".to_string()),
        network: "test-net".to_string(),
        extra_flags: vec!["-p".to_string(), "8080:80".to_string()],
        image_and_args: vec!["nginx:latest".to_string(), "--debug".to_string()],
    }
}

#[test]
fn docker_and_nerdctl_produce_structurally_equivalent_run_commands() {
    let docker = runtime::create_provider(RuntimeType::Docker);
    let nerdctl = runtime::create_provider(RuntimeType::Nerdctl);
    let spec = run_spec();

    let docker_cmd = docker.run_command(&spec);
    let nerdctl_cmd = nerdctl.run_command(&spec);

    let (docker_prog, docker_args) = match docker_cmd {
        JobCommand::Exec { program, args } => (program, args),
        _ => panic!("expected Exec"),
    };
    let (nerdctl_prog, nerdctl_args) = match nerdctl_cmd {
        JobCommand::Exec { program, args } => (program, args),
        _ => panic!("expected Exec"),
    };

    assert_eq!(docker_prog, "docker");
    assert_eq!(nerdctl_prog, "nerdctl");
    assert_eq!(docker_args[1..], nerdctl_args[1..]);
}

#[test]
fn run_command_includes_all_spec_fields() {
    let provider = runtime::create_provider(RuntimeType::Docker);
    let spec = run_spec();
    let cmd = provider.run_command(&spec);

    let args = match cmd {
        JobCommand::Exec { args, .. } => args,
        _ => panic!("expected Exec"),
    };

    assert!(args.contains(&"run".to_string()));
    assert!(args.contains(&"--rm".to_string()));
    assert!(args.contains(&"test-container".to_string()));
    assert!(args.contains(&"test-host".to_string()));
    assert!(args.contains(&"test.maestro.internal".to_string()));
    assert!(args.contains(&"test-net".to_string()));
    assert!(args.contains(&"8080:80".to_string()));
    assert!(args.contains(&"nginx:latest".to_string()));
    assert!(args.contains(&"--debug".to_string()));
}

#[test]
fn run_command_omits_domainname_when_dns_domain_is_none() {
    let provider = runtime::create_provider(RuntimeType::Docker);
    let spec = RunSpec {
        container_name: "c".to_string(),
        hostname: "h".to_string(),
        dns_domain: None,
        network: "n".to_string(),
        extra_flags: vec![],
        image_and_args: vec!["img".to_string()],
    };
    let cmd = provider.run_command(&spec);
    let args = match cmd {
        JobCommand::Exec { args, .. } => args,
        _ => panic!("expected Exec"),
    };
    assert!(!args.contains(&"--domainname".to_string()));
}

#[test]
fn docker_does_not_require_explicit_dns() {
    let provider = runtime::create_provider(RuntimeType::Docker);
    assert!(!provider.requires_explicit_dns());
}

#[test]
fn nerdctl_requires_explicit_dns() {
    let provider = runtime::create_provider(RuntimeType::Nerdctl);
    assert!(provider.requires_explicit_dns());
}

#[test]
fn build_secrets_are_forwarded_as_env_backed_buildkit_secrets() {
    let secrets = std::collections::HashMap::from([
        (
            "NPM_TOKEN".to_string(),
            SecretString::new("npm-secret".to_string()),
        ),
        (
            "GH_TOKEN".to_string(),
            SecretString::new("github-secret".to_string()),
        ),
    ]);
    let mut args = vec!["build".to_string()];

    runtime::append_build_secret_args(&mut args, &secrets);

    assert_eq!(
        args,
        [
            "build",
            "--secret",
            "id=GH_TOKEN,env=GH_TOKEN",
            "--secret",
            "id=NPM_TOKEN,env=NPM_TOKEN"
        ]
    );
    assert!(!args.iter().any(|arg| arg.contains("npm-secret")));
    assert!(!args.iter().any(|arg| arg.contains("github-secret")));
}

#[test]
fn image_inspection_resolves_the_requested_repository_digest() {
    let wanted = format!("registry.example.com/team/app@sha256:{}", "a".repeat(64));
    let other = format!("registry.example.com/team/other@sha256:{}", "b".repeat(64));
    let inspect = serde_json::json!([{
        "RepoDigests": [other, wanted]
    }])
    .to_string();

    assert_eq!(
        runtime::immutable_image_reference("registry.example.com/team/app:latest", &inspect)
            .unwrap(),
        wanted
    );
}

#[test]
fn image_inspection_accepts_docker_hub_normalization() {
    let digest = format!("docker.io/library/nginx@sha256:{}", "c".repeat(64));
    let inspect = serde_json::json!([{"RepoDigests": [digest]}]).to_string();

    assert_eq!(
        runtime::immutable_image_reference("nginx:latest", &inspect).unwrap(),
        digest
    );
}

#[test]
fn image_inspection_rejects_a_digest_for_a_different_repository() {
    let digest = format!("registry.example.com/team/other@sha256:{}", "d".repeat(64));
    let inspect = serde_json::json!([{"RepoDigests": [digest]}]).to_string();
    let error = runtime::immutable_image_reference("app:latest", &inspect)
        .unwrap_err()
        .to_string();

    assert!(error.contains("no matching immutable repository digest"));
}

#[tokio::test]
async fn image_export_command_streams_stdout_without_buffering_an_archive() {
    use tokio::io::AsyncReadExt;

    let (writer, mut reader) = tokio::io::duplex(64);
    let export = tokio::spawn(runtime::stream_command_output(
        "sh",
        &["-c", "printf image-archive"],
        Box::pin(writer),
    ));
    let mut bytes = Vec::new();
    reader.read_to_end(&mut bytes).await.unwrap();

    export.await.unwrap().unwrap();
    assert_eq!(bytes, b"image-archive");
}

#[tokio::test]
async fn image_import_command_streams_the_archive_to_stdin() {
    use tokio::io::AsyncWriteExt;

    let (mut writer, reader) = tokio::io::duplex(64);
    writer.write_all(b"image-archive").await.unwrap();
    writer.shutdown().await.unwrap();
    drop(writer);

    runtime::stream_command_input(
        "sh",
        &["-c", "value=$(cat); test \"$value\" = image-archive"],
        Box::pin(reader),
    )
    .await
    .unwrap();
}

#[tokio::test]
#[ignore = "requires a running nerdctl/containerd runtime and the busybox:1.37 image"]
async fn nerdctl_peer_image_archive_round_trips_through_a_bounded_stream() {
    let provider = runtime::create_provider(RuntimeType::Nerdctl);
    let (writer, reader) = tokio::io::duplex(256 * 1024);

    let (export, import) = tokio::join!(
        provider.export_image("busybox:1.37", Box::pin(writer)),
        provider.import_image("busybox:1.37", Box::pin(reader)),
    );

    export.unwrap();
    import.unwrap();
}
