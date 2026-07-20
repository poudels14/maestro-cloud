use docker::models::{MountType, RestartPolicyNameEnum};

use crate::docker_config::{METADATA_LABEL, SPEC_LABEL, container_config};
use crate::{MountSource, RuntimeError, WorkloadSpec};

use super::docker_fixture::container_spec;

#[test]
fn docker_config_preserves_identity_and_disables_runtime_restarts() {
    let spec = container_spec();
    let config = container_config(&spec).unwrap();
    assert_eq!(config.name, "maestro-workload-1");
    assert_eq!(config.body.hostname.as_deref(), Some("workload-1"));
    assert_eq!(config.body.user.as_deref(), Some("1000:1001"));
    assert_eq!(
        config.body.entrypoint.as_deref(),
        Some(["/bin/service".to_owned()].as_slice())
    );
    assert_eq!(
        config.body.cmd.as_deref(),
        Some(["--foreground".to_owned()].as_slice())
    );
    let environment = config.body.env.unwrap();
    assert!(environment.contains(&"PLAIN=visible".to_owned()));
    assert!(environment.contains(&"TOKEN=sensitive".to_owned()));
    assert!(environment.contains(&"MAESTRO_WORKLOAD_ADDRESS=10.42.0.8".to_owned()));
    let labels = config.body.labels.unwrap();
    assert_eq!(labels.get(SPEC_LABEL), Some(&config.fingerprint));
    assert!(!labels.get(METADATA_LABEL).unwrap().contains("sensitive"));

    let host = config.body.host_config.unwrap();
    assert_eq!(host.network_mode.as_deref(), Some("none"));
    assert_eq!(
        host.restart_policy.unwrap().name,
        Some(RestartPolicyNameEnum::NO)
    );
    let mounts = host.mounts.unwrap();
    assert_eq!(mounts.len(), 2);
    assert_eq!(mounts.first().unwrap().typ, Some(MountType::BIND));
    assert_eq!(mounts.first().unwrap().read_only, Some(true));
    assert_eq!(mounts.get(1).unwrap().typ, Some(MountType::VOLUME));
}

#[test]
fn docker_config_rejects_other_workload_kinds_and_relative_mounts() {
    let process = WorkloadSpec::Process(crate::ProcessWorkload {
        configuration: container_spec().configuration().clone(),
        command: kernel_api::CommandSpec {
            executable: "/bin/true".to_owned(),
            arguments: Vec::new(),
        },
    });
    assert!(matches!(
        container_config(&process),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let mut spec = container_spec();
    let WorkloadSpec::Container(container) = &mut spec else {
        unreachable!();
    };
    container.configuration.mounts.first_mut().unwrap().source =
        MountSource::HostPath("relative".into());
    assert!(matches!(
        container_config(&spec),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}
