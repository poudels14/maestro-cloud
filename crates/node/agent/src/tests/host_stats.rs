use std::os::unix::fs::symlink;

use crate::{HostStatsError, HostStatsReader, LinuxHostStatsReader};

#[tokio::test]
async fn linux_reader_parses_cumulative_cpu_memory_and_network_stats()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = HostStatsFixture::new()?;
    fixture.write("stat", "cpu  100 20 30 400 50 6 7 8 9 10\ncpu0 1 2 3 4\n")?;
    fixture.write(
        "meminfo",
        "MemTotal:       2048 kB\nMemFree:         256 kB\nMemAvailable:    512 kB\n",
    )?;
    fixture.write("net/dev", NETWORK_STATS)?;

    let stats = fixture.reader().read().await?;

    assert_eq!(stats.cpu.total_ticks, 621);
    assert_eq!(stats.cpu.idle_ticks, 450);
    assert_eq!(stats.memory.used_bytes, 1_572_864);
    assert_eq!(stats.memory.total_bytes, 2_097_152);
    assert_eq!(stats.network.receive_bytes, 1_100);
    assert_eq!(stats.network.transmit_bytes, 2_200);
    Ok(())
}

#[tokio::test]
async fn linux_reader_rejects_missing_malformed_and_oversized_stats()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = HostStatsFixture::new()?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::Io { .. })
    ));

    fixture.write("stat", "cpu nope\n")?;
    fixture.write("meminfo", "MemTotal: 1 kB\nMemAvailable: 1 kB\n")?;
    fixture.write("net/dev", NETWORK_STATS)?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::InvalidValue { file: "stat", .. })
    ));

    fixture.write("stat", &"x".repeat(1_048_577))?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::FileTooLarge { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn linux_reader_rejects_links_impossible_memory_and_counter_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = HostStatsFixture::new()?;
    fixture.write("stat", "cpu 1 1 1 1\n")?;
    fixture.write("meminfo", "MemTotal: 1 kB\nMemAvailable: 2 kB\n")?;
    fixture.write("net/dev", NETWORK_STATS)?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::InvalidValue {
            file: "meminfo",
            ..
        })
    ));

    fixture.write("meminfo", "MemTotal: 2 kB\nMemAvailable: 1 kB\n")?;
    fixture.write(
        "net/dev",
        &format!(
            "Inter-| Receive | Transmit\n face |bytes |bytes\neth0: {} 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\neth1: 1 0 0 0 0 0 0 0 1 0 0 0 0 0 0 0\n",
            u64::MAX
        ),
    )?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::CounterOverflow { file: "net/dev" })
    ));

    fixture.write("net/dev", NETWORK_STATS)?;
    std::fs::remove_file(fixture.directory.path().join("stat"))?;
    let outside = fixture.directory.path().join("outside");
    std::fs::write(&outside, "cpu 1 1 1 1\n")?;
    symlink(&outside, fixture.directory.path().join("stat"))?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostStatsError::UnsafePath { .. })
    ));
    Ok(())
}

struct HostStatsFixture {
    directory: tempfile::TempDir,
}

impl HostStatsFixture {
    fn new() -> std::io::Result<Self> {
        let directory = tempfile::tempdir()?;
        std::fs::create_dir(directory.path().join("net"))?;
        Ok(Self { directory })
    }

    fn write(&self, name: &str, contents: &str) -> std::io::Result<()> {
        std::fs::write(self.directory.path().join(name), contents)
    }

    fn reader(&self) -> LinuxHostStatsReader {
        LinuxHostStatsReader::from_proc_root(self.directory.path().to_path_buf())
    }
}

const NETWORK_STATS: &str = "\
Inter-|   Receive                                                |  Transmit
 face |bytes    packets errs drop fifo frame compressed multicast|bytes    packets errs drop fifo colls carrier compressed
    lo: 100 1 0 0 0 0 0 0 200 1 0 0 0 0 0 0
  eth0: 1000 2 0 0 0 0 0 0 2000 2 0 0 0 0 0 0
";
