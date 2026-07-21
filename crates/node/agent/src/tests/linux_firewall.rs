use std::os::unix::fs::PermissionsExt;

use kernel_api::{NodeFirewallSpec, NodeId};

use crate::{FirewallBackend, NftablesFirewallBackend};

#[tokio::test]
async fn nftables_backend_checks_then_applies_the_exact_script()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let binary = directory.path().join("nft-test");
    let calls = directory.path().join("calls");
    let inputs = directory.path().join("inputs");
    std::fs::write(
        &binary,
        format!(
            "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '{}'\ncat >> '{}'\n",
            calls.display(),
            inputs.display()
        ),
    )?;
    std::fs::set_permissions(&binary, std::fs::Permissions::from_mode(0o700))?;
    let backend = NftablesFirewallBackend::with_binary(binary);
    let script = "destroy table inet maestro_firewall\ntable inet maestro_firewall {}\n";

    backend
        .apply(&NodeFirewallSpec {
            node_id: NodeId::new("node-1")?,
            table_name: "maestro_firewall".to_string(),
            script: script.to_string(),
            digest: "digest-is-verified-by-the-agent".to_string(),
        })
        .await?;

    assert_eq!(std::fs::read_to_string(calls)?, "--check -f -\n-f -\n");
    assert_eq!(std::fs::read_to_string(inputs)?, script.repeat(2));
    Ok(())
}
