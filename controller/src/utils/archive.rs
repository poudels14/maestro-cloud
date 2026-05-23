use std::io::Read;
use std::path::Path;

use anyhow::{Result, anyhow};
use flate2::Compression;
use flate2::read::GzDecoder;
use flate2::write::GzEncoder;
use ignore::WalkBuilder;

pub fn pack_context(context_dir: &Path) -> Result<Vec<u8>> {
    let absolute = std::fs::canonicalize(context_dir).map_err(|err| {
        anyhow!(
            "failed to resolve context directory `{}`: {err}",
            context_dir.display()
        )
    })?;

    let mut walker = WalkBuilder::new(&absolute);
    walker
        .hidden(false)
        .git_ignore(true)
        .git_exclude(true)
        .git_global(true)
        .add_custom_ignore_filename(".gitignore")
        .add_custom_ignore_filename(".dockerignore");

    let encoder = GzEncoder::new(Vec::new(), Compression::default());
    let mut builder = tar::Builder::new(encoder);
    builder.follow_symlinks(false);

    for result in walker.build() {
        let entry = result.map_err(|err| anyhow!("walk error: {err}"))?;
        let path = entry.path();
        if path == absolute {
            continue;
        }
        let relative = path
            .strip_prefix(&absolute)
            .map_err(|err| anyhow!("strip_prefix failed: {err}"))?;
        if is_excluded_path(relative) {
            continue;
        }
        let file_type = entry
            .file_type()
            .ok_or_else(|| anyhow!("missing file type for `{}`", path.display()))?;
        if file_type.is_dir() {
            builder
                .append_dir(relative, path)
                .map_err(|err| anyhow!("failed to append dir `{}`: {err}", path.display()))?;
        } else if file_type.is_file() {
            let mut file = std::fs::File::open(path)
                .map_err(|err| anyhow!("failed to open `{}`: {err}", path.display()))?;
            builder
                .append_file(relative, &mut file)
                .map_err(|err| anyhow!("failed to append file `{}`: {err}", path.display()))?;
        } else if file_type.is_symlink() {
            let target = std::fs::read_link(path)
                .map_err(|err| anyhow!("failed to read symlink `{}`: {err}", path.display()))?;
            let mut header = tar::Header::new_gnu();
            header.set_entry_type(tar::EntryType::Symlink);
            header.set_size(0);
            header.set_mode(0o777);
            builder
                .append_link(&mut header, relative, &target)
                .map_err(|err| anyhow!("failed to append symlink `{}`: {err}", path.display()))?;
        }
    }

    let encoder = builder
        .into_inner()
        .map_err(|err| anyhow!("failed to finish tar: {err}"))?;
    let bytes = encoder
        .finish()
        .map_err(|err| anyhow!("failed to finish gzip: {err}"))?;
    Ok(bytes)
}

pub fn extract_context<R: Read>(reader: R, target_dir: &Path) -> Result<()> {
    std::fs::create_dir_all(target_dir).map_err(|err| {
        anyhow!(
            "failed to create target dir `{}`: {err}",
            target_dir.display()
        )
    })?;
    let decoder = GzDecoder::new(reader);
    let mut archive = tar::Archive::new(decoder);
    archive.set_preserve_permissions(true);
    archive.unpack(target_dir).map_err(|err| {
        anyhow!(
            "failed to unpack archive into `{}`: {err}",
            target_dir.display()
        )
    })?;
    Ok(())
}

pub fn extract_context_file(archive_path: &Path, target_dir: &Path) -> Result<()> {
    let file = std::fs::File::open(archive_path).map_err(|err| {
        anyhow!(
            "failed to open upload archive `{}`: {err}",
            archive_path.display()
        )
    })?;
    extract_context(std::io::BufReader::new(file), target_dir)
}

fn is_excluded_path(relative: &Path) -> bool {
    relative
        .components()
        .any(|comp| matches!(comp.as_os_str().to_str(), Some(".git")))
}

#[cfg(test)]
mod tests {
    use super::*;

    use std::fs;

    fn tmpdir(suffix: &str) -> std::path::PathBuf {
        let mut path = std::env::temp_dir();
        let unique = format!(
            "maestro-archive-test-{suffix}-{}",
            crate::utils::nanoid::unique_id(8)
        );
        path.push(unique);
        fs::create_dir_all(&path).expect("create tmp dir");
        path
    }

    #[test]
    fn roundtrip_preserves_files_and_excludes_git_dir() {
        let source = tmpdir("source");
        fs::write(source.join("app.rs"), b"fn main() {}").unwrap();
        fs::create_dir_all(source.join(".git/objects")).unwrap();
        fs::write(source.join(".git/HEAD"), b"ref: refs/heads/main").unwrap();
        fs::create_dir_all(source.join("nested")).unwrap();
        fs::write(source.join("nested/lib.rs"), b"pub fn lib() {}").unwrap();

        let bytes = pack_context(&source).expect("pack");
        assert!(!bytes.is_empty());

        let dest = tmpdir("dest");
        extract_context(bytes.as_slice(), &dest).expect("extract");

        assert_eq!(fs::read(dest.join("app.rs")).unwrap(), b"fn main() {}");
        assert_eq!(
            fs::read(dest.join("nested/lib.rs")).unwrap(),
            b"pub fn lib() {}",
        );
        assert!(!dest.join(".git").exists(), ".git must not be included");

        let _ = fs::remove_dir_all(&source);
        let _ = fs::remove_dir_all(&dest);
    }

    #[test]
    fn honors_gitignore_and_dockerignore() {
        let source = tmpdir("ignore");
        fs::write(source.join(".gitignore"), b"target\n").unwrap();
        fs::write(source.join(".dockerignore"), b"node_modules\n").unwrap();
        fs::create_dir_all(source.join("target")).unwrap();
        fs::write(source.join("target/output.bin"), b"binary").unwrap();
        fs::create_dir_all(source.join("node_modules/dep")).unwrap();
        fs::write(source.join("node_modules/dep/index.js"), b"module").unwrap();
        fs::write(source.join("keep.txt"), b"kept").unwrap();

        let bytes = pack_context(&source).expect("pack");
        let dest = tmpdir("ignore-dest");
        extract_context(bytes.as_slice(), &dest).expect("extract");

        assert!(dest.join("keep.txt").exists(), "non-ignored file kept");
        assert!(
            !dest.join("target/output.bin").exists(),
            ".gitignore honored"
        );
        assert!(
            !dest.join("node_modules/dep/index.js").exists(),
            ".dockerignore honored",
        );

        let _ = fs::remove_dir_all(&source);
        let _ = fs::remove_dir_all(&dest);
    }
}
