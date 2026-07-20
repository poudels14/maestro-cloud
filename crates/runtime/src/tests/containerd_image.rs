use kernel_api::CommandSpec;

use crate::RuntimeError;
use crate::containerd_image::{ContainerdImageConfiguration, chain_id};

#[test]
fn image_configuration_combines_entrypoint_and_command() {
    let configuration = ContainerdImageConfiguration {
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
