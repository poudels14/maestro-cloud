use std::io::Read;

use crate::archive::pack_context;

#[test]
fn context_archive_is_deterministic_and_honors_ignore_files()
-> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("Dockerfile"), b"FROM scratch\n")?;
    std::fs::write(source.path().join(".dockerignore"), b"ignored.txt\n")?;
    std::fs::write(source.path().join("ignored.txt"), b"ignored")?;
    std::fs::create_dir_all(source.path().join("src"))?;
    std::fs::write(source.path().join("src/main.rs"), b"fn main() {}\n")?;
    std::fs::create_dir_all(source.path().join(".git/objects"))?;
    std::fs::write(source.path().join(".git/HEAD"), b"ref: refs/heads/main\n")?;

    let first = pack_context(source.path())?;
    let second = pack_context(source.path())?;
    assert_eq!(first, second);
    assert!(first.starts_with(&[0x1f, 0x8b]));

    let mut archive = tar::Archive::new(flate2::read::GzDecoder::new(first.as_slice()));
    let mut entries = Vec::new();
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry.path()?.into_owned();
        let mut body = Vec::new();
        entry.read_to_end(&mut body)?;
        entries.push((path, body));
    }
    assert!(
        entries
            .iter()
            .any(|(path, _)| path == std::path::Path::new("Dockerfile"))
    );
    assert!(
        entries
            .iter()
            .any(|(path, _)| path == std::path::Path::new("src/main.rs"))
    );
    assert!(entries.iter().all(|(path, _)| !path.starts_with(".git")));
    assert!(
        entries
            .iter()
            .all(|(path, _)| path != std::path::Path::new("ignored.txt"))
    );
    Ok(())
}

#[cfg(unix)]
#[test]
fn context_archive_rejects_symbolic_links() -> Result<(), Box<dyn std::error::Error>> {
    let source = tempfile::tempdir()?;
    std::fs::write(source.path().join("outside"), b"outside")?;
    std::os::unix::fs::symlink("outside", source.path().join("linked"))?;
    let error = pack_context(source.path()).expect_err("symlink must fail closed");
    assert!(error.to_string().contains("regular file or directory"));
    Ok(())
}
