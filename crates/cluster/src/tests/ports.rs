use crate::{ClusterPorts, ClusterPortsError, DEFAULT_WIREGUARD_PORT};

#[test]
fn cluster_ports_include_the_wireguard_allocation() -> Result<(), Box<dyn std::error::Error>> {
    let ports = ClusterPorts::new(3_000, 23_79, 23_80, DEFAULT_WIREGUARD_PORT)?;
    let encoded = serde_json::to_value(ports)?;

    assert_eq!(
        encoded
            .get("formatVersion")
            .and_then(serde_json::Value::as_u64),
        Some(1)
    );
    assert_eq!(
        encoded.get("wireguard").and_then(serde_json::Value::as_u64),
        Some(u64::from(DEFAULT_WIREGUARD_PORT))
    );
    assert_eq!(ports.format_version(), ClusterPorts::FORMAT_VERSION);
    Ok(())
}

#[test]
fn cluster_ports_reject_zero_and_duplicates() {
    assert_eq!(
        ClusterPorts::new(0, 23_79, 23_80, DEFAULT_WIREGUARD_PORT),
        Err(ClusterPortsError::ZeroPort)
    );
    assert_eq!(
        ClusterPorts::new(3_000, 23_79, 23_80, 23_80),
        Err(ClusterPortsError::DuplicatePort)
    );
}

#[test]
fn cluster_ports_reject_a_conflicting_api_port() -> Result<(), Box<dyn std::error::Error>> {
    let ports = ClusterPorts::new(3_001, 23_79, 23_80, DEFAULT_WIREGUARD_PORT)?;

    assert_eq!(
        ports.validate_api_port(3_001),
        Err(ClusterPortsError::ApiPortConflict { port: 3_001 })
    );
    Ok(())
}
