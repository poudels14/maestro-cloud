use std::collections::BTreeSet;

use super::*;

#[test]
fn sysfs_reader_sums_owned_interfaces_and_rejects_unsafe_names()
-> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    for (interface, received, transmitted) in [("mh-one", 10, 20), ("mh-two", 30, 40)] {
        let statistics = directory.path().join(interface).join("statistics");
        std::fs::create_dir_all(&statistics)?;
        std::fs::write(statistics.join("rx_bytes"), format!("{received}\n"))?;
        std::fs::write(statistics.join("tx_bytes"), format!("{transmitted}\n"))?;
    }
    let interfaces = BTreeSet::from(["mh-one".to_owned(), "mh-two".to_owned()]);
    assert_eq!(
        read_interfaces(directory.path(), &interfaces)?,
        WorkloadNetworkStats {
            receive_bytes: 60,
            transmit_bytes: 40,
        }
    );
    assert!(read_interfaces(directory.path(), &BTreeSet::from(["../eth0".to_owned()])).is_err());
    assert!(
        HostNetworkStatsReader::new(
            Arc::new(runtime::FakeNetworkProvider::default()),
            PathBuf::from("sys/class/net"),
        )
        .is_err()
    );
    Ok(())
}

#[test]
fn sysfs_reader_bounds_and_validates_counter_text() -> Result<(), Box<dyn std::error::Error>> {
    let directory = tempfile::tempdir()?;
    let statistics = directory.path().join("mh-test").join("statistics");
    std::fs::create_dir_all(&statistics)?;
    std::fs::write(statistics.join("rx_bytes"), "not-a-counter")?;
    std::fs::write(statistics.join("tx_bytes"), "1")?;
    let interface = BTreeSet::from(["mh-test".to_owned()]);
    assert!(read_interfaces(directory.path(), &interface).is_err());
    std::fs::write(statistics.join("rx_bytes"), "1".repeat(65))?;
    assert!(read_interfaces(directory.path(), &interface).is_err());
    Ok(())
}
