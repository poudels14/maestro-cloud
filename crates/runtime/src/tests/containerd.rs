use containerd::services::v1::Plugin;

use crate::RuntimeError;
use crate::containerd::validate_snapshotter_plugin;

#[test]
fn snapshotter_probe_requires_fast_id_remapping() {
    let plugin = Plugin {
        r#type: "io.containerd.snapshotter.v1".to_owned(),
        id: "overlayfs".to_owned(),
        capabilities: vec!["only-remap-ids".to_owned()],
        ..Default::default()
    };
    assert!(matches!(
        validate_snapshotter_plugin(std::slice::from_ref(&plugin), "overlayfs"),
        Err(RuntimeError::Unavailable { message }) if message.contains("does not support idmapped snapshots")
    ));

    let mut capable = plugin;
    capable.capabilities.push("remap-ids".to_owned());
    assert!(validate_snapshotter_plugin(&[capable], "overlayfs").is_ok());
}
