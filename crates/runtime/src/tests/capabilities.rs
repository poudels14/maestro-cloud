use crate::{Capabilities, RuntimeCapability};

#[test]
fn capability_set_is_deduplicated_and_stably_ordered() {
    let capabilities = Capabilities::new([
        RuntimeCapability::TransferArtifact,
        RuntimeCapability::Exec,
        RuntimeCapability::Exec,
    ]);

    assert!(capabilities.supports(RuntimeCapability::Exec));
    assert!(!capabilities.supports(RuntimeCapability::Pause));
    assert_eq!(
        capabilities.iter().collect::<Vec<_>>(),
        vec![RuntimeCapability::Exec, RuntimeCapability::TransferArtifact]
    );
}
