use std::os::unix::fs::symlink;

use runtime::CgroupPath;

use crate::{CgroupStatsError, CgroupStatsReader, CgroupV2StatsReader};

#[tokio::test]
async fn cgroup_v2_reader_parses_cpu_memory_io_and_process_counters()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = CgroupFixture::new()?;
    fixture.write("cpu.stat", CPU_STAT)?;
    fixture.write("memory.current", "1048576\n")?;
    fixture.write("memory.max", "max\n")?;
    fixture.write("memory.events", MEMORY_EVENTS)?;
    fixture.write(
        "io.stat",
        "8:0 rbytes=100 wbytes=200 rios=3 wios=4 dbytes=5 dios=6\n\
         8:16 rbytes=10 wbytes=20 rios=1 wios=2 dbytes=3 dios=4 cost.usage=99\n",
    )?;
    fixture.write("pids.current", "7\n")?;
    fixture.write("pids.max", "128\n")?;

    let stats = CgroupV2StatsReader.read(&fixture.path()).await?;

    assert_eq!(stats.cpu.usage_usec, 1_000);
    assert_eq!(stats.cpu.user_usec, 700);
    assert_eq!(stats.cpu.system_usec, 300);
    assert_eq!(stats.cpu.periods, 20);
    assert_eq!(stats.cpu.throttled_periods, 2);
    assert_eq!(stats.cpu.throttled_usec, 50);
    assert_eq!(stats.memory.current_bytes, 1_048_576);
    assert_eq!(stats.memory.maximum_bytes, None);
    assert_eq!(stats.memory.events.maximum, 3);
    assert_eq!(stats.memory.events.out_of_memory_kills, 5);
    assert_eq!(stats.memory.events.out_of_memory_group_kills, 6);
    assert_eq!(stats.io.read_bytes, 110);
    assert_eq!(stats.io.write_bytes, 220);
    assert_eq!(stats.io.read_operations, 4);
    assert_eq!(stats.io.write_operations, 6);
    assert_eq!(stats.io.discarded_bytes, 8);
    assert_eq!(stats.io.discard_operations, 10);
    assert_eq!(stats.processes.current, 7);
    assert_eq!(stats.processes.maximum, Some(128));
    Ok(())
}

#[tokio::test]
async fn cgroup_v2_reader_accepts_empty_io_and_older_memory_event_shape()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = CgroupFixture::new()?;
    fixture.write("cpu.stat", CPU_STAT)?;
    fixture.write("memory.current", "1\n")?;
    fixture.write("memory.max", "2048\n")?;
    fixture.write("memory.events", "low 0\nhigh 0\nmax 0\noom 0\noom_kill 0\n")?;
    fixture.write("io.stat", "")?;
    fixture.write("pids.current", "1\n")?;
    fixture.write("pids.max", "max\n")?;

    let stats = CgroupV2StatsReader.read(&fixture.path()).await?;

    assert_eq!(stats.memory.maximum_bytes, Some(2048));
    assert_eq!(stats.memory.events.out_of_memory_group_kills, 0);
    assert_eq!(stats.io, Default::default());
    assert_eq!(stats.processes.maximum, None);
    Ok(())
}

#[tokio::test]
async fn cgroup_v2_reader_rejects_missing_malformed_oversized_and_linked_files()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = CgroupFixture::new()?;
    assert!(matches!(
        CgroupV2StatsReader.read(&fixture.path()).await,
        Err(CgroupStatsError::Io { .. })
    ));

    fixture.write("cpu.stat", "usage_usec nope\n")?;
    assert!(matches!(
        CgroupV2StatsReader.read(&fixture.path()).await,
        Err(CgroupStatsError::InvalidValue {
            file: "cpu.stat",
            ..
        })
    ));

    fixture.write("cpu.stat", &"x".repeat(65_537))?;
    assert!(matches!(
        CgroupV2StatsReader.read(&fixture.path()).await,
        Err(CgroupStatsError::FileTooLarge { .. })
    ));

    std::fs::remove_file(fixture.directory.path().join("cpu.stat"))?;
    let outside = fixture.directory.path().join("outside");
    std::fs::write(&outside, CPU_STAT)?;
    symlink(&outside, fixture.directory.path().join("cpu.stat"))?;
    assert!(matches!(
        CgroupV2StatsReader.read(&fixture.path()).await,
        Err(CgroupStatsError::UnsafePath { .. })
    ));
    Ok(())
}

#[tokio::test]
async fn cgroup_v2_reader_detects_per_device_counter_overflow()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = CgroupFixture::new()?;
    fixture.write("cpu.stat", CPU_STAT)?;
    fixture.write("memory.current", "1\n")?;
    fixture.write("memory.max", "max\n")?;
    fixture.write("memory.events", MEMORY_EVENTS)?;
    fixture.write(
        "io.stat",
        &format!("8:0 rbytes={}\n8:16 rbytes=1\n", u64::MAX),
    )?;
    fixture.write("pids.current", "1\n")?;
    fixture.write("pids.max", "max\n")?;

    assert!(matches!(
        CgroupV2StatsReader.read(&fixture.path()).await,
        Err(CgroupStatsError::CounterOverflow)
    ));
    Ok(())
}

struct CgroupFixture {
    directory: tempfile::TempDir,
}

impl CgroupFixture {
    fn new() -> std::io::Result<Self> {
        Ok(Self {
            directory: tempfile::tempdir()?,
        })
    }

    fn write(&self, name: &str, contents: &str) -> std::io::Result<()> {
        std::fs::write(self.directory.path().join(name), contents)
    }

    fn path(&self) -> CgroupPath {
        CgroupPath::new(self.directory.path().to_path_buf()).expect("absolute temporary path")
    }
}

const CPU_STAT: &str = "\
usage_usec 1000
user_usec 700
system_usec 300
nr_periods 20
nr_throttled 2
throttled_usec 50
";

const MEMORY_EVENTS: &str = "\
low 1
high 2
max 3
oom 4
oom_kill 5
oom_group_kill 6
";
