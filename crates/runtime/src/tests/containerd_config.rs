use serde_json::Value;

use crate::containerd_config::{container_record, fingerprint};
use crate::containerd_image::ImageDefaults;
use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::containerd_volume::managed_volume_path;
use crate::{
    HostPortPublication, MountSource, PortProtocol, RuntimeCapability, RuntimeError, WorkloadSpec,
};

use super::containerd_fixture::container_spec;

#[test]
fn container_record_preserves_identity_and_oci_process_configuration() {
    let spec = container_spec();
    let image = ImageDefaults {
        environment: vec!["IMAGE=yes".to_owned(), "PLAIN=image".to_owned()],
        entrypoint: vec!["/image-entrypoint".to_owned()],
        command: vec!["image-argument".to_owned()],
        working_directory: Some("/workspace".to_owned()),
        user: "2000:2001".to_owned(),
    };
    let fingerprint = fingerprint(&spec).unwrap();
    let record = container_record(
        &spec,
        &image,
        &ContainerdRuntimeSettings::default(),
        "maestro-workload-1-rootfs".to_owned(),
        fingerprint.clone(),
    )
    .unwrap();

    assert_eq!(record.id, "maestro-workload-1");
    assert_eq!(record.snapshotter, "overlayfs");
    assert_eq!(record.snapshot_key, "maestro-workload-1-rootfs");
    assert_eq!(
        record.labels.get("com.maestro.spec-sha256"),
        Some(&fingerprint)
    );
    assert!(!record.labels.values().any(|value| value.contains("TOKEN")));

    let oci: Value = serde_json::from_slice(&record.spec.unwrap().value).unwrap();
    assert_eq!(oci.get("hostname").unwrap(), "workload-1");
    assert_eq!(
        oci.pointer("/process/args").unwrap(),
        &serde_json::json!(["/bin/service", "--foreground"])
    );
    assert_eq!(oci.pointer("/process/cwd").unwrap(), "/workspace");
    assert_eq!(oci.pointer("/root/path").unwrap(), "rootfs");
    assert_eq!(oci.pointer("/process/user/uid").unwrap(), 1000);
    assert_eq!(oci.pointer("/process/user/gid").unwrap(), 1001);
    let environment = oci.pointer("/process/env").unwrap().as_array().unwrap();
    assert!(environment.contains(&Value::String("IMAGE=yes".to_owned())));
    assert!(environment.contains(&Value::String("PLAIN=visible".to_owned())));
    assert!(!environment.iter().any(|value| {
        value
            .as_str()
            .is_some_and(|value| value.starts_with("TOKEN="))
    }));
    assert!(environment.contains(&Value::String(
        "MAESTRO_WORKLOAD_ADDRESS=10.42.0.8".to_owned()
    )));
    assert_eq!(
        oci.pointer("/linux/cgroupsPath").unwrap(),
        "/maestro/workload-1"
    );
    let mounts = oci.get("mounts").unwrap().as_array().unwrap();
    assert_eq!(mounts.len(), 9);
    assert_eq!(mounts.get(7).unwrap().pointer("/options/2").unwrap(), "ro");
    assert_eq!(
        mounts.get(8).unwrap().get("destination").unwrap(),
        "/etc/resolv.conf"
    );
    assert_eq!(
        mounts.get(8).unwrap().get("source").unwrap(),
        "/var/lib/maestro/runtime/containerd/workloads/workload-1/resolv.conf"
    );
}

#[test]
fn container_record_uses_image_defaults_and_managed_volume_bindings() {
    let mut spec = container_spec();
    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.command = None;
    workload.configuration.user = None;
    workload.configuration.mounts.clear();
    let image = ImageDefaults {
        environment: Vec::new(),
        entrypoint: vec!["/image-entrypoint".to_owned()],
        command: vec!["image-argument".to_owned()],
        working_directory: None,
        user: "2000:2001".to_owned(),
    };
    let record = container_record(
        &spec,
        &image,
        &ContainerdRuntimeSettings::default(),
        "snapshot".to_owned(),
        fingerprint(&spec).unwrap(),
    )
    .unwrap();
    let oci: Value = serde_json::from_slice(&record.spec.unwrap().value).unwrap();
    assert_eq!(
        oci.pointer("/process/args").unwrap(),
        &serde_json::json!(["/image-entrypoint", "image-argument"])
    );
    assert_eq!(oci.pointer("/process/user/uid").unwrap(), 2000);

    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.configuration.mounts.push(crate::WorkloadMount {
        source: MountSource::ManagedVolume("data".to_owned()),
        target: "/data".into(),
        access: crate::MountAccess::ReadWrite,
    });
    let settings = ContainerdRuntimeSettings::default();
    let record = container_record(
        &spec,
        &image,
        &settings,
        "snapshot".to_owned(),
        fingerprint(&spec).unwrap(),
    )
    .unwrap();
    let oci: Value = serde_json::from_slice(&record.spec.unwrap().value).unwrap();
    let managed = oci
        .get("mounts")
        .unwrap()
        .as_array()
        .unwrap()
        .iter()
        .find(|mount| mount.get("destination").unwrap() == "/data")
        .unwrap();
    assert_eq!(managed.get("type").unwrap(), "bind");
    assert_eq!(
        managed.get("source").unwrap(),
        managed_volume_path(
            &settings.state_root,
            &spec.configuration().metadata.cluster_id,
            "data"
        )
        .unwrap()
        .to_str()
        .unwrap()
    );

    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.configuration.mounts.clear();
    workload.configuration.mounts.push(crate::WorkloadMount {
        source: MountSource::HostPath("/tmp/resolv.conf".into()),
        target: "/etc/resolv.conf".into(),
        access: crate::MountAccess::ReadOnly,
    });
    assert!(matches!(
        container_record(
            &spec,
            &image,
            &ContainerdRuntimeSettings::default(),
            "snapshot".to_owned(),
            fingerprint(&spec).unwrap(),
        ),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}

#[test]
fn container_record_rejects_host_port_publication_without_capability() {
    let mut spec = container_spec();
    let WorkloadSpec::Container(workload) = &mut spec else {
        unreachable!();
    };
    workload.published_ports.push(HostPortPublication {
        container_port: 80,
        host_address: "0.0.0.0".parse().unwrap(),
        host_port: 80,
        protocol: PortProtocol::Tcp,
    });
    assert_eq!(
        container_record(
            &spec,
            &ImageDefaults {
                environment: Vec::new(),
                entrypoint: vec!["/bin/true".to_owned()],
                command: Vec::new(),
                working_directory: None,
                user: String::new(),
            },
            &ContainerdRuntimeSettings::default(),
            "snapshot".to_owned(),
            fingerprint(&spec).unwrap(),
        ),
        Err(RuntimeError::Unsupported {
            capability: RuntimeCapability::HostPortPublishing,
        })
    );
}
