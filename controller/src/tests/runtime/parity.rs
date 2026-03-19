use crate::config::RuntimeType;
use crate::runtime::{self, BuildSpec, RunSpec, RuntimeProvider};
use crate::supervisor::JobCommand;

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
