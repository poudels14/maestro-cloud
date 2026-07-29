use kernel_api::CommandSpec;
use oci_spec::image::{ImageConfiguration, ImageIndex};

use crate::RuntimeError;
use crate::containerd_image::{ImageDefaults, chain_id, host_manifest_descriptor};

#[test]
fn image_configuration_combines_entrypoint_and_command() {
    let configuration = ImageDefaults {
        environment: Vec::new(),
        entrypoint: vec!["/bin/service".to_owned()],
        command: vec!["--serve".to_owned()],
        working_directory: None,
        user: String::new(),
    };
    assert_eq!(
        configuration.command().unwrap(),
        CommandSpec {
            executable: "/bin/service".to_owned(),
            arguments: vec!["--serve".to_owned()],
        }
    );
}

#[test]
fn oci_image_defaults_use_standard_config_keys() {
    let configuration: ImageConfiguration = serde_json::from_value(serde_json::json!({
        "architecture": "amd64",
        "os": "linux",
        "config": {
            "Env": ["IMAGE=yes"],
            "Entrypoint": ["/usr/bin/env"],
            "Cmd": ["httpd-foreground"],
            "WorkingDir": "/srv/http",
            "User": "33:33"
        },
        "rootfs": {
            "type": "layers",
            "diff_ids": ["sha256:a"]
        }
    }))
    .unwrap();

    let defaults = ImageDefaults::from_oci(&configuration);
    assert_eq!(defaults.environment, ["IMAGE=yes"]);
    assert_eq!(defaults.working_directory.as_deref(), Some("/srv/http"));
    assert_eq!(defaults.user, "33:33");
    assert_eq!(
        defaults.command().unwrap(),
        CommandSpec {
            executable: "/usr/bin/env".to_owned(),
            arguments: vec!["httpd-foreground".to_owned()],
        }
    );
}

#[test]
fn image_chain_id_is_derived_in_layer_order() {
    assert_eq!(
        chain_id(&["sha256:a".to_owned(), "sha256:b".to_owned()]).unwrap(),
        "sha256:970a948bffa8de94d6e22d747ba8c95030e6e546909f98f54e99a13005e173a8"
    );
    assert!(matches!(chain_id(&[]), Err(RuntimeError::Rejected { .. })));
    assert!(matches!(
        chain_id(&["sha256:a".to_owned(), "invalid".to_owned()]),
        Err(RuntimeError::Rejected { .. })
    ));
}

#[test]
fn image_index_selects_the_immutable_host_manifest() {
    let architecture = match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        architecture => architecture,
    };
    let index: ImageIndex = serde_json::from_value(serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
            "size": 123,
            "platform": {
                "architecture": architecture,
                "os": "linux"
            }
        }, {
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
            "size": 456,
            "platform": {
                "architecture": "other",
                "os": "linux"
            }
        }]
    }))
    .unwrap();

    let descriptor = host_manifest_descriptor(&index).unwrap();
    assert_eq!(
        descriptor.digest,
        "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    );
    assert_eq!(
        descriptor.media_type,
        "application/vnd.oci.image.manifest.v1+json"
    );
}

#[test]
fn single_platform_archive_index_selects_its_unannotated_manifest() {
    let index: ImageIndex = serde_json::from_value(serde_json::json!({
        "schemaVersion": 2,
        "mediaType": "application/vnd.oci.image.index.v1+json",
        "manifests": [{
            "mediaType": "application/vnd.oci.image.manifest.v1+json",
            "digest": "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc",
            "size": 123
        }]
    }))
    .unwrap();

    let descriptor = host_manifest_descriptor(&index).unwrap();
    assert_eq!(
        descriptor.digest,
        "sha256:cccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccccc"
    );
}
