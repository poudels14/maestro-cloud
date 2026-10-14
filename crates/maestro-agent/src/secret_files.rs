use std::collections::{BTreeMap, BTreeSet};
use std::io::Write;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt};
use std::path::{Path, PathBuf};

use maestro_core::ReplicaId;

const FILE_EXTENSION: &str = "env";

pub fn write(
    directory: &Path,
    replica_id: &ReplicaId,
    items: &BTreeMap<String, String>,
) -> std::io::Result<PathBuf> {
    std::fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(directory)?;
    let path = file_path(directory, replica_id);
    remove(&path);
    let mut file = std::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(&path)?;
    file.write_all(render(items).as_bytes())?;
    Ok(path)
}

pub fn remove_for(directory: &Path, replica_id: &ReplicaId) {
    remove(&file_path(directory, replica_id));
}

pub fn sweep(directory: &Path, keep: &BTreeSet<ReplicaId>) {
    let stale = std::fs::read_dir(directory)
        .into_iter()
        .flatten()
        .flatten()
        .map(|entry| entry.path())
        .filter(|path| {
            path.extension()
                .is_some_and(|extension| extension == FILE_EXTENSION)
                && path
                    .file_stem()
                    .and_then(|stem| stem.to_str())
                    .is_none_or(|stem| !keep.contains(&ReplicaId::from(stem)))
        })
        .collect::<Vec<_>>();
    stale.iter().for_each(|path| remove(path));
}

fn render(items: &BTreeMap<String, String>) -> String {
    items
        .iter()
        .map(|(key, value)| {
            let escaped = value
                .replace('\\', "\\\\")
                .replace('"', "\\\"")
                .replace('\n', "\\n");
            format!("{key}=\"{escaped}\"\n")
        })
        .collect()
}

fn file_path(directory: &Path, replica_id: &ReplicaId) -> PathBuf {
    directory.join(format!("{replica_id}.{FILE_EXTENSION}"))
}

fn remove(path: &Path) {
    if let Ok(metadata) = std::fs::metadata(path) {
        let zeros = vec![0u8; metadata.len() as usize];
        let _ = std::fs::write(path, zeros);
        let _ = std::fs::remove_file(path);
    }
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn writes_quoted_lines_with_private_permissions() {
        let directory = tempfile::tempdir().unwrap();
        let secrets = directory.path().join("secrets");
        let items = BTreeMap::from([
            ("B".to_string(), "say \"hi\"\\n".to_string()),
            ("A".to_string(), "line1\nline2".to_string()),
        ]);
        let path = write(&secrets, &ReplicaId::from("d1-0"), &items).unwrap();
        assert_eq!(
            std::fs::read_to_string(&path).unwrap(),
            "A=\"line1\\nline2\"\nB=\"say \\\"hi\\\"\\\\n\"\n"
        );
        assert_eq!(
            std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
            0o600
        );
        assert_eq!(
            std::fs::metadata(&secrets).unwrap().permissions().mode() & 0o777,
            0o700
        );
    }

    #[test]
    fn sweeps_files_of_replicas_that_are_gone() {
        let directory = tempfile::tempdir().unwrap();
        let items = BTreeMap::from([("K".to_string(), "v".to_string())]);
        write(directory.path(), &ReplicaId::from("d1-0"), &items).unwrap();
        write(directory.path(), &ReplicaId::from("d1-1"), &items).unwrap();
        sweep(directory.path(), &BTreeSet::from([ReplicaId::from("d1-1")]));
        assert!(!directory.path().join("d1-0.env").exists());
        assert!(directory.path().join("d1-1.env").exists());
        remove_for(directory.path(), &ReplicaId::from("d1-1"));
        assert!(!directory.path().join("d1-1.env").exists());
    }
}
