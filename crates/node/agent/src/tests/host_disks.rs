use std::os::unix::fs::symlink;

use crate::{HostDiskError, HostDiskReader, LinuxHostDiskReader};

#[tokio::test]
async fn linux_reader_reports_sorted_unique_storage_mounts_and_filters_virtual_filesystems()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = DiskFixture::new()?;
    let first = fixture.mount("first")?;
    let second = fixture.mount("second")?;
    fixture.write(&format!(
        "/dev/vdb {} ext4 rw 0 0\n\
         proc /proc proc rw 0 0\n\
         server:/volume {} nfs rw 0 0\n\
         /dev/vda {} xfs rw 0 0\n\
         /dev/vda {} xfs rw 0 0\n",
        second.display(),
        fixture.directory.path().display(),
        first.display(),
        first.display(),
    ))?;

    let report = fixture.reader().read().await?;

    assert!(report.failures.is_empty());
    assert_eq!(report.disks.len(), 2);
    let first_disk = report.disks.first().expect("first disk");
    let second_disk = report.disks.get(1).expect("second disk");
    assert_eq!(first_disk.name, "/dev/vda");
    assert_eq!(first_disk.mount_point, first.to_string_lossy());
    assert_eq!(first_disk.file_system, "xfs");
    assert!(first_disk.total_bytes > 0);
    assert!(first_disk.available_bytes <= first_disk.total_bytes);
    assert_eq!(second_disk.name, "/dev/vdb");
    assert_eq!(second_disk.mount_point, second.to_string_lossy());
    Ok(())
}

#[tokio::test]
async fn linux_reader_decodes_mount_escapes_and_isolates_disappearing_mounts()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = DiskFixture::new()?;
    let spaced = fixture.mount("disk with space")?;
    let escaped = spaced.to_string_lossy().replace(' ', "\\040");
    fixture.write(&format!(
        "/dev/disk\\040one {escaped} ext4 rw 0 0\n\
         /dev/missing /maestro-test-missing-mount ext4 rw 0 0\n"
    ))?;

    let report = fixture.reader().read().await?;

    assert_eq!(report.disks.len(), 1);
    let disk = report.disks.first().expect("decoded disk");
    assert_eq!(disk.name, "/dev/disk one");
    assert_eq!(disk.mount_point, spaced.to_string_lossy());
    assert_eq!(report.failures.len(), 1);
    let failure = report.failures.first().expect("missing mount failure");
    assert_eq!(failure.mount_point, "/maestro-test-missing-mount");
    Ok(())
}

#[tokio::test]
async fn linux_reader_rejects_missing_malformed_oversized_and_linked_mount_tables()
-> Result<(), Box<dyn std::error::Error>> {
    let fixture = DiskFixture::new()?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostDiskError::Io { .. })
    ));

    fixture.write("missing fields\n")?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostDiskError::InvalidMount { .. })
    ));

    fixture.write(&"x".repeat(1_048_577))?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostDiskError::FileTooLarge { .. })
    ));

    std::fs::remove_file(fixture.mounts_file())?;
    let outside = fixture.directory.path().join("outside");
    std::fs::write(&outside, "/dev/vda / ext4 rw 0 0\n")?;
    symlink(&outside, fixture.mounts_file())?;
    assert!(matches!(
        fixture.reader().read().await,
        Err(HostDiskError::UnsafePath { .. })
    ));
    Ok(())
}

struct DiskFixture {
    directory: tempfile::TempDir,
}

impl DiskFixture {
    fn new() -> std::io::Result<Self> {
        Ok(Self {
            directory: tempfile::tempdir()?,
        })
    }

    fn mount(&self, name: &str) -> std::io::Result<std::path::PathBuf> {
        let path = self.directory.path().join(name);
        std::fs::create_dir(&path)?;
        Ok(path)
    }

    fn write(&self, contents: &str) -> std::io::Result<()> {
        std::fs::write(self.mounts_file(), contents)
    }

    fn mounts_file(&self) -> std::path::PathBuf {
        self.directory.path().join("mounts")
    }

    fn reader(&self) -> LinuxHostDiskReader {
        LinuxHostDiskReader::from_mounts_file(self.mounts_file())
    }
}
