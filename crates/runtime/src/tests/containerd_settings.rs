use std::path::PathBuf;

use crate::containerd_settings::ContainerdRuntimeSettings;
use crate::{RegistryCredential, RuntimeError};
use kernel_api::SecretValue;

#[test]
fn containerd_defaults_are_valid_and_explicit() {
    let settings = ContainerdRuntimeSettings::default();
    settings.validate().unwrap();
    assert_eq!(settings.namespace, "maestro");
    assert_eq!(settings.snapshotter, "overlayfs");
    assert_eq!(settings.runtime_name, "io.containerd.runc.v2");
    assert_eq!(
        settings.buildkit_address,
        "unix:///run/buildkit/buildkitd.sock"
    );
    assert!(settings.max_build_output_bytes > settings.max_build_context_bytes);
}

#[test]
fn containerd_settings_reject_relative_empty_and_zero_values() {
    let settings = ContainerdRuntimeSettings {
        state_root: PathBuf::from("relative"),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        namespace: String::new(),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        namespace: "invalid/namespace".to_owned(),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        kill_timeout: std::time::Duration::ZERO,
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        rpc_timeout: std::time::Duration::ZERO,
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        buildkit_address: "bad\naddress".to_owned(),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));

    let settings = ContainerdRuntimeSettings {
        registry_credentials: std::collections::BTreeMap::from([(
            "invalid/host".to_owned(),
            RegistryCredential::new("x-token", SecretValue::new("protected")),
        )]),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}

#[cfg(unix)]
#[test]
fn containerd_settings_reject_non_utf8_state_roots() {
    use std::ffi::OsString;
    use std::os::unix::ffi::OsStringExt;

    let settings = ContainerdRuntimeSettings {
        state_root: PathBuf::from(OsString::from_vec(b"/tmp/\xff".to_vec())),
        ..ContainerdRuntimeSettings::default()
    };
    assert!(matches!(
        settings.validate(),
        Err(RuntimeError::InvalidSpec { .. })
    ));
}
