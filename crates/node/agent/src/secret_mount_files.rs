use std::collections::{BTreeMap, BTreeSet};
use std::fmt::Write as _;
use std::fs::{self, File, OpenOptions};
use std::io::{Seek, SeekFrom, Write};
use std::os::unix::fs::{MetadataExt, OpenOptionsExt, PermissionsExt};
use std::path::{Component, Path, PathBuf};

use kernel_api::{EnvironmentName, SecretMountSpec, SecretValue, WorkloadId};
use nix::unistd::{Gid, Uid, chown};
use runtime::{MountAccess, MountSource, WorkloadMount, WorkloadUser};
use zeroize::{Zeroize, Zeroizing};

use crate::secret_mount::SecretMountError;

const DIRECTORY_MODE: u32 = 0o700;
const SECRET_MODE: u32 = 0o600;
const DOTENV_FILE: &str = "secrets.env";

pub(crate) fn materialize(
    root: &Path,
    workload_id: &WorkloadId,
    spec: &SecretMountSpec,
    owner: Option<WorkloadUser>,
) -> Result<WorkloadMount, SecretMountError> {
    let target = validate_target(spec.mount_path())?;
    ensure_private_directory(root)?;
    match spec {
        SecretMountSpec::Dotenv { items, .. } => {
            materialize_dotenv(root, workload_id, target, items, owner)
        }
        SecretMountSpec::Files { files, .. } => {
            materialize_file_set(root, workload_id, target, files, owner)
        }
    }
}

fn materialize_dotenv(
    root: &Path,
    workload_id: &WorkloadId,
    target: PathBuf,
    items: &BTreeMap<String, SecretValue>,
    owner: Option<WorkloadUser>,
) -> Result<WorkloadMount, SecretMountError> {
    let content = encode_dotenv(items)?;
    let directory = root.join(workload_id.as_str());
    ensure_private_directory(&directory)?;
    let secret_path = directory.join(DOTENV_FILE);
    cleanup_temporary(&dotenv_staging_path(&directory), &secret_path)?;
    if secret_path.exists() {
        ensure_expected_entries(
            &directory,
            &BTreeSet::from([DOTENV_FILE.to_owned()]),
            workload_id,
        )?;
        ensure_existing_content(&secret_path, content.as_bytes(), workload_id)?;
    } else {
        ensure_expected_entries(&directory, &BTreeSet::new(), workload_id)?;
        install_secret(&directory, &secret_path, content.as_bytes(), workload_id)?;
    }
    if let Some(owner) = owner {
        set_owner(&secret_path, owner)?;
        set_owner(&directory, owner)?;
    }
    Ok(read_only_mount(secret_path, target))
}

fn encode_dotenv(
    items: &BTreeMap<String, SecretValue>,
) -> Result<Zeroizing<String>, SecretMountError> {
    let mut content = Zeroizing::new(String::new());
    for (name, value) in items {
        if EnvironmentName::parse(name).is_err() {
            return Err(SecretMountError::InvalidKey { name: name.clone() });
        }
        let encoded = Zeroizing::new(serde_json::to_string(value.expose()).map_err(|error| {
            SecretMountError::Encode {
                message: error.to_string(),
            }
        })?);
        writeln!(&mut *content, "{name}={}", encoded.as_str()).map_err(|error| {
            SecretMountError::Encode {
                message: error.to_string(),
            }
        })?;
    }
    Ok(content)
}

fn materialize_file_set(
    root: &Path,
    workload_id: &WorkloadId,
    target: PathBuf,
    files: &BTreeMap<String, SecretValue>,
    owner: Option<WorkloadUser>,
) -> Result<WorkloadMount, SecretMountError> {
    validate_file_names(files.keys())?;
    let directory = root.join(workload_id.as_str());
    if directory.exists() {
        ensure_private_directory(&directory)?;
        ensure_existing_file_set(&directory, files, workload_id)?;
        if let Some(owner) = owner {
            set_file_set_owner(&directory, files, owner)?;
        }
        return Ok(read_only_mount(directory, target));
    }

    let temporary = root.join(format!(".{}.files.new", workload_id.as_str()));
    cleanup_directory(&temporary)?;
    fs::create_dir(&temporary)
        .map_err(|source| io_error("create file-set staging directory", &temporary, source))?;
    fs::set_permissions(&temporary, fs::Permissions::from_mode(DIRECTORY_MODE))
        .map_err(|source| io_error("protect file-set staging directory", &temporary, source))?;
    let install = install_file_set(&temporary, files).and_then(|()| {
        sync_directory(&temporary)?;
        match fs::rename(&temporary, &directory) {
            Ok(()) => {
                sync_directory(root)?;
                Ok(())
            }
            Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
                cleanup_directory(&temporary)?;
                ensure_private_directory(&directory)?;
                ensure_existing_file_set(&directory, files, workload_id)
            }
            Err(source) => Err(io_error(
                "install secret file-set directory",
                &directory,
                source,
            )),
        }
    });
    if let Err(error) = install {
        let _cleanup = cleanup_directory(&temporary);
        return Err(error);
    }
    if let Some(owner) = owner {
        set_file_set_owner(&directory, files, owner)?;
    }
    Ok(read_only_mount(directory, target))
}

fn set_file_set_owner(
    directory: &Path,
    files: &BTreeMap<String, SecretValue>,
    owner: WorkloadUser,
) -> Result<(), SecretMountError> {
    for name in files.keys() {
        set_owner(&directory.join(name), owner)?;
    }
    set_owner(directory, owner)
}

fn set_owner(path: &Path, owner: WorkloadUser) -> Result<(), SecretMountError> {
    chown(
        path,
        Some(Uid::from_raw(owner.user_id)),
        Some(Gid::from_raw(owner.group_id)),
    )
    .map_err(|source| {
        io_error(
            "set secret owner",
            path,
            std::io::Error::from_raw_os_error(source as i32),
        )
    })
}

fn install_file_set(
    directory: &Path,
    files: &BTreeMap<String, SecretValue>,
) -> Result<(), SecretMountError> {
    for (name, value) in files {
        let path = directory.join(name);
        let mut file = OpenOptions::new()
            .create_new(true)
            .write(true)
            .mode(SECRET_MODE)
            .open(&path)
            .map_err(|source| io_error("create secret file", &path, source))?;
        if let Err(source) = file
            .write_all(value.expose().as_bytes())
            .and_then(|()| file.sync_all())
        {
            drop(file);
            let _cleanup = zeroize_and_remove(&path);
            return Err(io_error("write secret file", &path, source));
        }
    }
    Ok(())
}

fn ensure_existing_file_set(
    directory: &Path,
    files: &BTreeMap<String, SecretValue>,
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    ensure_expected_entries(directory, &files.keys().cloned().collect(), workload_id)?;
    for (name, value) in files {
        ensure_existing_content(
            &directory.join(name),
            value.expose().as_bytes(),
            workload_id,
        )?;
    }
    Ok(())
}

fn ensure_expected_entries(
    directory: &Path,
    expected: &BTreeSet<String>,
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    let mut observed = BTreeSet::new();
    for entry in fs::read_dir(directory)
        .map_err(|source| io_error("list secret directory", directory, source))?
    {
        let entry = entry.map_err(|source| io_error("list secret directory", directory, source))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect secret entry", &path, source))?;
        if !file_type.is_file() || file_type.is_symlink() {
            return Err(SecretMountError::UnsafePath { path });
        }
        observed.insert(entry.file_name().to_string_lossy().into_owned());
    }
    if &observed != expected {
        Err(SecretMountError::ContentConflict {
            workload_id: workload_id.to_string(),
        })
    } else {
        Ok(())
    }
}

fn validate_target(value: &str) -> Result<PathBuf, SecretMountError> {
    let target = PathBuf::from(value);
    if !target.is_absolute()
        || target.parent().is_none()
        || target == Path::new("/")
        || target
            .components()
            .any(|component| component == Component::ParentDir)
    {
        Err(SecretMountError::InvalidTarget {
            target: value.to_owned(),
        })
    } else {
        Ok(target)
    }
}

fn validate_file_names<'a>(
    names: impl IntoIterator<Item = &'a String>,
) -> Result<(), SecretMountError> {
    for name in names {
        let mut components = Path::new(name).components();
        if name.is_empty()
            || !matches!(components.next(), Some(Component::Normal(_)))
            || components.next().is_some()
        {
            return Err(SecretMountError::InvalidFileName { name: name.clone() });
        }
    }
    Ok(())
}

fn read_only_mount(source: PathBuf, target: PathBuf) -> WorkloadMount {
    WorkloadMount {
        source: MountSource::HostPath(source),
        target,
        access: MountAccess::ReadOnly,
    }
}

fn ensure_private_directory(path: &Path) -> Result<(), SecretMountError> {
    fs::create_dir_all(path).map_err(|source| io_error("create directory", path, source))?;
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect directory", path, source))?;
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(SecretMountError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(DIRECTORY_MODE))
        .map_err(|source| io_error("protect directory", path, source))
}

fn install_secret(
    directory: &Path,
    secret_path: &Path,
    content: &[u8],
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    let temporary = dotenv_staging_path(directory);
    cleanup_temporary(&temporary, secret_path)?;
    let mut file = OpenOptions::new()
        .create_new(true)
        .write(true)
        .mode(SECRET_MODE)
        .open(&temporary)
        .map_err(|source| io_error("create secret", &temporary, source))?;
    if let Err(source) = file.write_all(content).and_then(|()| file.sync_all()) {
        drop(file);
        let _cleanup = zeroize_and_remove(&temporary);
        return Err(io_error("write secret", &temporary, source));
    }
    drop(file);
    match fs::hard_link(&temporary, secret_path) {
        Ok(()) => {
            fs::remove_file(&temporary)
                .map_err(|source| io_error("remove secret staging link", &temporary, source))?;
            sync_directory(directory)
        }
        Err(error) if error.kind() == std::io::ErrorKind::AlreadyExists => {
            cleanup_temporary(&temporary, secret_path)?;
            ensure_existing_content(secret_path, content, workload_id)
        }
        Err(source) => {
            let _cleanup = cleanup_temporary(&temporary, secret_path);
            Err(io_error("install secret", secret_path, source))
        }
    }
}

fn dotenv_staging_path(directory: &Path) -> PathBuf {
    directory.join(format!(".{DOTENV_FILE}.new"))
}

fn ensure_existing_content(
    path: &Path,
    expected: &[u8],
    workload_id: &WorkloadId,
) -> Result<(), SecretMountError> {
    let metadata =
        fs::symlink_metadata(path).map_err(|source| io_error("inspect secret", path, source))?;
    if !metadata.file_type().is_file() || metadata.file_type().is_symlink() {
        return Err(SecretMountError::UnsafePath {
            path: path.to_path_buf(),
        });
    }
    let mut existing = Zeroizing::new(
        fs::read(path).map_err(|source| io_error("read existing secret", path, source))?,
    );
    let matches = existing.as_slice() == expected;
    existing.zeroize();
    if !matches {
        return Err(SecretMountError::ContentConflict {
            workload_id: workload_id.to_string(),
        });
    }
    fs::set_permissions(path, fs::Permissions::from_mode(SECRET_MODE))
        .map_err(|source| io_error("protect secret", path, source))
}

pub(crate) fn cleanup_stale(
    root: &Path,
    active: &BTreeSet<String>,
) -> Result<usize, SecretMountError> {
    let entries = match fs::read_dir(root) {
        Ok(entries) => entries,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(0),
        Err(source) => return Err(io_error("list secret root", root, source)),
    };
    let mut cleaned = 0_usize;
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list secret root", root, source))?;
        let name = entry.file_name().to_string_lossy().into_owned();
        if entry
            .file_type()
            .map_err(|source| io_error("inspect secret entry", &entry.path(), source))?
            .is_dir()
            && WorkloadId::new(&name).is_ok()
            && !active.contains(&name)
        {
            cleanup_directory(&entry.path())?;
            cleaned = cleaned.saturating_add(1);
        }
    }
    Ok(cleaned)
}

pub(crate) fn cleanup_directory(directory: &Path) -> Result<(), SecretMountError> {
    let metadata = match fs::symlink_metadata(directory) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => {
            return Err(io_error(
                "inspect workload secret directory",
                directory,
                source,
            ));
        }
    };
    if !metadata.file_type().is_dir() || metadata.file_type().is_symlink() {
        return Err(SecretMountError::UnsafePath {
            path: directory.to_path_buf(),
        });
    }
    let entries = match fs::read_dir(directory) {
        Ok(entries) => entries,
        Err(source) => return Err(io_error("list workload secrets", directory, source)),
    };
    for entry in entries {
        let entry = entry.map_err(|source| io_error("list workload secrets", directory, source))?;
        let path = entry.path();
        let file_type = entry
            .file_type()
            .map_err(|source| io_error("inspect workload secret", &path, source))?;
        if file_type.is_file() {
            zeroize_and_remove(&path)?;
        } else if file_type.is_symlink() {
            fs::remove_file(&path)
                .map_err(|source| io_error("remove unsafe link", &path, source))?;
        }
    }
    fs::remove_dir(directory)
        .or_else(|error| {
            if error.kind() == std::io::ErrorKind::NotFound {
                Ok(())
            } else {
                Err(error)
            }
        })
        .map_err(|source| io_error("remove workload secret directory", directory, source))
}

fn cleanup_temporary(temporary: &Path, installed: &Path) -> Result<(), SecretMountError> {
    let temporary_metadata = match fs::symlink_metadata(temporary) {
        Ok(metadata) => metadata,
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(()),
        Err(source) => return Err(io_error("inspect secret staging file", temporary, source)),
    };
    let installed_metadata = fs::symlink_metadata(installed).ok();
    let aliases_installed = installed_metadata.is_some_and(|installed_metadata| {
        installed_metadata.dev() == temporary_metadata.dev()
            && installed_metadata.ino() == temporary_metadata.ino()
    });
    if aliases_installed {
        fs::remove_file(temporary)
            .map_err(|source| io_error("remove secret staging link", temporary, source))
    } else {
        zeroize_and_remove(temporary)
    }
}

fn zeroize_and_remove(path: &Path) -> Result<(), SecretMountError> {
    let length = fs::metadata(path)
        .map_err(|source| io_error("inspect secret for cleanup", path, source))?
        .len();
    let mut file = OpenOptions::new()
        .write(true)
        .open(path)
        .map_err(|source| io_error("open secret for cleanup", path, source))?;
    file.seek(SeekFrom::Start(0))
        .map_err(|source| io_error("seek secret for cleanup", path, source))?;
    let zeros = [0_u8; 4096];
    let mut remaining = length;
    while remaining > 0 {
        let count = usize::try_from(remaining.min(zeros.len() as u64)).unwrap_or(zeros.len());
        let chunk = zeros.get(..count).unwrap_or(&zeros);
        file.write_all(chunk)
            .map_err(|source| io_error("zero secret", path, source))?;
        remaining = remaining.saturating_sub(count as u64);
    }
    file.sync_all()
        .map_err(|source| io_error("sync zeroed secret", path, source))?;
    drop(file);
    fs::remove_file(path).map_err(|source| io_error("remove zeroed secret", path, source))
}

fn sync_directory(directory: &Path) -> Result<(), SecretMountError> {
    File::open(directory)
        .and_then(|directory| directory.sync_all())
        .map_err(|source| io_error("sync secret directory", directory, source))
}

fn io_error(operation: &'static str, path: &Path, source: std::io::Error) -> SecretMountError {
    SecretMountError::Io {
        operation,
        path: path.to_path_buf(),
        source,
    }
}
