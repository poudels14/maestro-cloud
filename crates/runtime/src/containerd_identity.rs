use std::ffi::CString;
use std::fs::File;
use std::io::Read as _;
use std::path::{Path, PathBuf};

use containerd::types::Mount;
use csv::{ByteRecord, ReaderBuilder, Trim};
use rustix::fs::{Mode, OFlags, ResolveFlags, open, openat2};
use rustix::mount::{MountFlags, UnmountFlags, mount, mount_remount, unmount};

use crate::{RuntimeError, WorkloadUser};

const MAX_IDENTITY_FILE_BYTES: u64 = 1024 * 1024;
const MAX_CONTAINER_ID: u32 = i32::MAX as u32;

#[derive(Debug, Clone, PartialEq, Eq)]
enum Identity {
    Id(u32),
    Name(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ImageUserSpec {
    Default,
    User(Identity),
    UserGroup(Identity, Identity),
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct PasswdEntry {
    name: Vec<u8>,
    user_id: u32,
    group_id: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
struct GroupEntry {
    name: Vec<u8>,
    group_id: u32,
}

/// Resolves an OCI image `User` value using the image root filesystem.
///
/// containerd requires numeric identities in the runtime specification. Named users and groups,
/// and a numeric user without an explicit group, therefore need the image's passwd/group files.
pub(crate) async fn resolve_image_user(
    value: String,
    mounts: Vec<Mount>,
) -> Result<WorkloadUser, RuntimeError> {
    let spec = ImageUserSpec::parse(&value)?;
    if let Some(user) = spec.without_rootfs() {
        return Ok(user);
    }
    tokio::task::spawn_blocking(move || {
        with_readonly_rootfs(&mounts, |root| spec.resolve(root, &value))
    })
    .await
    .map_err(|error| RuntimeError::Unavailable {
        message: format!("containerd image-user resolution task failed: {error}"),
    })?
}

#[cfg(test)]
pub(crate) fn resolve_image_user_in_rootfs(
    value: &str,
    root: &Path,
) -> Result<WorkloadUser, RuntimeError> {
    let spec = ImageUserSpec::parse(value)?;
    spec.without_rootfs()
        .map_or_else(|| spec.resolve(root, value), Ok)
}

#[cfg(test)]
pub(crate) fn readonly_overlay_options_for_test(
    options: &[String],
) -> Result<Vec<String>, RuntimeError> {
    readonly_overlay_options(options)
}

impl ImageUserSpec {
    fn parse(value: &str) -> Result<Self, RuntimeError> {
        if value.is_empty() {
            return Ok(Self::Default);
        }
        let mut components = value.split(':');
        let user = components.next().unwrap_or_default();
        let group = components.next();
        if user.is_empty() || group.is_some_and(str::is_empty) || components.next().is_some() {
            return Err(invalid_user(value, "expected `user` or `user:group`"));
        }
        let user = Identity::parse(user, value, "user")?;
        match group {
            Some(group) => Ok(Self::UserGroup(
                user,
                Identity::parse(group, value, "group")?,
            )),
            None => Ok(Self::User(user)),
        }
    }

    fn without_rootfs(&self) -> Option<WorkloadUser> {
        match self {
            Self::Default => Some(WorkloadUser {
                user_id: 0,
                group_id: 0,
            }),
            Self::UserGroup(Identity::Id(user_id), Identity::Id(group_id)) => Some(WorkloadUser {
                user_id: *user_id,
                group_id: *group_id,
            }),
            Self::User(_) | Self::UserGroup(_, _) => None,
        }
    }

    fn resolve(&self, root: &Path, value: &str) -> Result<WorkloadUser, RuntimeError> {
        match self {
            Self::Default => Ok(WorkloadUser {
                user_id: 0,
                group_id: 0,
            }),
            Self::User(Identity::Id(user_id)) => {
                let passwd = read_identity_file(root, "etc/passwd")?;
                let group_id = passwd
                    .as_deref()
                    .map(parse_passwd)
                    .transpose()?
                    .unwrap_or_default()
                    .into_iter()
                    .find(|entry| entry.user_id == *user_id)
                    .map_or(0, |entry| entry.group_id);
                Ok(WorkloadUser {
                    user_id: *user_id,
                    group_id,
                })
            }
            Self::User(Identity::Name(name)) => {
                let entry = named_user(root, name, value)?;
                Ok(WorkloadUser {
                    user_id: entry.user_id,
                    group_id: entry.group_id,
                })
            }
            Self::UserGroup(user, group) => Ok(WorkloadUser {
                user_id: match user {
                    Identity::Id(user_id) => *user_id,
                    Identity::Name(name) => named_user(root, name, value)?.user_id,
                },
                group_id: match group {
                    Identity::Id(group_id) => *group_id,
                    Identity::Name(name) => named_group(root, name, value)?.group_id,
                },
            }),
        }
    }
}

impl Identity {
    fn parse(component: &str, value: &str, kind: &str) -> Result<Self, RuntimeError> {
        match component.parse::<i64>() {
            Ok(id) if (0..=i64::from(MAX_CONTAINER_ID)).contains(&id) => Ok(Self::Id(id as u32)),
            Ok(_) => Err(invalid_user(
                value,
                &format!("{kind} ID must be between 0 and {MAX_CONTAINER_ID}"),
            )),
            Err(error) if error.kind() == &std::num::IntErrorKind::InvalidDigit => {
                Ok(Self::Name(component.to_owned()))
            }
            Err(_) => Err(invalid_user(value, &format!("invalid {kind} ID"))),
        }
    }
}

fn named_user(root: &Path, name: &str, value: &str) -> Result<PasswdEntry, RuntimeError> {
    let passwd = read_identity_file(root, "etc/passwd")?.ok_or_else(|| {
        invalid_user(
            value,
            &format!("named user `{name}` requires image `/etc/passwd`"),
        )
    })?;
    parse_passwd(&passwd)?
        .into_iter()
        .find(|entry| entry.name == name.as_bytes())
        .ok_or_else(|| {
            invalid_user(
                value,
                &format!("user `{name}` does not exist in image `/etc/passwd`"),
            )
        })
}

fn named_group(root: &Path, name: &str, value: &str) -> Result<GroupEntry, RuntimeError> {
    let group = read_identity_file(root, "etc/group")?.ok_or_else(|| {
        invalid_user(
            value,
            &format!("named group `{name}` requires image `/etc/group`"),
        )
    })?;
    parse_group(&group)?
        .into_iter()
        .find(|entry| entry.name == name.as_bytes())
        .ok_or_else(|| {
            invalid_user(
                value,
                &format!("group `{name}` does not exist in image `/etc/group`"),
            )
        })
}

fn parse_passwd(bytes: &[u8]) -> Result<Vec<PasswdEntry>, RuntimeError> {
    records(bytes, "passwd", 7)?
        .into_iter()
        .map(|record| {
            Ok(PasswdEntry {
                name: record
                    .get(0)
                    .ok_or_else(|| malformed_file("passwd", "missing user name"))?
                    .to_vec(),
                user_id: parse_file_id(&record, 2, "passwd", "UID")?,
                group_id: parse_file_id(&record, 3, "passwd", "GID")?,
            })
        })
        .collect()
}

fn parse_group(bytes: &[u8]) -> Result<Vec<GroupEntry>, RuntimeError> {
    records(bytes, "group", 4)?
        .into_iter()
        .map(|record| {
            Ok(GroupEntry {
                name: record
                    .get(0)
                    .ok_or_else(|| malformed_file("group", "missing group name"))?
                    .to_vec(),
                group_id: parse_file_id(&record, 2, "group", "GID")?,
            })
        })
        .collect()
}

fn records(bytes: &[u8], file: &str, fields: usize) -> Result<Vec<ByteRecord>, RuntimeError> {
    let mut reader = ReaderBuilder::new()
        .delimiter(b':')
        .has_headers(false)
        .flexible(true)
        .quoting(false)
        .comment(Some(b'#'))
        .trim(Trim::All)
        .from_reader(bytes);
    reader
        .byte_records()
        .map(|record| {
            let record = record.map_err(|error| malformed_file(file, error))?;
            if record.len() != fields {
                return Err(malformed_file(
                    file,
                    format!("expected {fields} fields, found {}", record.len()),
                ));
            }
            Ok(record)
        })
        .collect()
}

fn parse_file_id(
    record: &ByteRecord,
    field: usize,
    file: &str,
    kind: &str,
) -> Result<u32, RuntimeError> {
    let field = record
        .get(field)
        .ok_or_else(|| malformed_file(file, format!("missing {kind}")))?;
    let value = std::str::from_utf8(field)
        .map_err(|error| malformed_file(file, format!("{kind} is not UTF-8: {error}")))?;
    let id = value
        .parse::<u32>()
        .map_err(|error| malformed_file(file, format!("invalid {kind} `{value}`: {error}")))?;
    if id > MAX_CONTAINER_ID {
        return Err(malformed_file(
            file,
            format!("{kind} `{id}` exceeds {MAX_CONTAINER_ID}"),
        ));
    }
    Ok(id)
}

fn read_identity_file(root: &Path, relative: &str) -> Result<Option<Vec<u8>>, RuntimeError> {
    let root = open(
        root,
        OFlags::RDONLY | OFlags::DIRECTORY | OFlags::CLOEXEC,
        Mode::empty(),
    )
    .map_err(|error| rootfs_io("open", root, error))?;
    let file = match openat2(
        &root,
        relative,
        OFlags::RDONLY | OFlags::CLOEXEC | OFlags::NONBLOCK,
        Mode::empty(),
        ResolveFlags::IN_ROOT | ResolveFlags::NO_MAGICLINKS,
    ) {
        Ok(file) => file,
        Err(rustix::io::Errno::NOENT) => return Ok(None),
        Err(error) => return Err(rootfs_io("open", Path::new(relative), error)),
    };
    let mut file = File::from(file);
    let metadata = file
        .metadata()
        .map_err(|error| rootfs_io("inspect", Path::new(relative), error))?;
    if !metadata.is_file() {
        return Err(RuntimeError::InvalidSpec {
            message: format!("image `/{relative}` is not a regular file"),
        });
    }
    let mut bytes = Vec::new();
    file.by_ref()
        .take(MAX_IDENTITY_FILE_BYTES + 1)
        .read_to_end(&mut bytes)
        .map_err(|error| rootfs_io("read", Path::new(relative), error))?;
    if bytes.len() as u64 > MAX_IDENTITY_FILE_BYTES {
        return Err(RuntimeError::InvalidSpec {
            message: format!(
                "image `/{relative}` exceeds the {MAX_IDENTITY_FILE_BYTES}-byte identity-file limit"
            ),
        });
    }
    Ok(Some(bytes))
}

fn with_readonly_rootfs<Value>(
    mounts: &[Mount],
    operation: impl FnOnce(&Path) -> Result<Value, RuntimeError>,
) -> Result<Value, RuntimeError> {
    if mounts.is_empty() {
        return Err(RuntimeError::Unavailable {
            message: "containerd returned no mounts for image-user resolution".to_owned(),
        });
    }
    if mounts.iter().any(|mount| !mount.target.is_empty()) {
        return Err(RuntimeError::Unavailable {
            message: "containerd returned a submount for image-user resolution".to_owned(),
        });
    }
    let mut rootfs = MountedRootfs::new()?;
    let result = (|| {
        for containerd_mount in mounts {
            let plan = MountPlan::readonly(containerd_mount)?;
            plan.mount(rootfs.path())?;
            rootfs.record_mount();
            plan.finish(rootfs.path())?;
        }
        operation(rootfs.path())
    })();
    let cleanup = rootfs.cleanup();
    match (result, cleanup) {
        (Ok(value), Ok(())) => Ok(value),
        (Err(error), Ok(())) => Err(error),
        (Ok(_), Err(error)) => Err(error),
        (Err(error), Err(cleanup)) => Err(RuntimeError::Unavailable {
            message: format!("{error}; additionally, {cleanup}"),
        }),
    }
}

struct MountedRootfs {
    path: PathBuf,
    mounted: usize,
}

impl MountedRootfs {
    fn new() -> Result<Self, RuntimeError> {
        let path = tempfile::Builder::new()
            .prefix("maestro-containerd-rootfs-")
            .tempdir()
            .map_err(|error| mount_io("create temporary rootfs", error))?
            .keep();
        Ok(Self { path, mounted: 0 })
    }

    fn path(&self) -> &Path {
        &self.path
    }

    fn record_mount(&mut self) {
        self.mounted += 1;
    }

    fn cleanup(&mut self) -> Result<(), RuntimeError> {
        while self.mounted > 0 {
            unmount(&self.path, UnmountFlags::NOFOLLOW)
                .map_err(|error| mount_io("unmount temporary image rootfs", error))?;
            self.mounted -= 1;
        }
        std::fs::remove_dir(&self.path)
            .map_err(|error| mount_io("remove temporary image rootfs", error))
    }
}

impl Drop for MountedRootfs {
    fn drop(&mut self) {
        while self.mounted > 0 {
            if unmount(&self.path, UnmountFlags::DETACH | UnmountFlags::NOFOLLOW).is_err() {
                return;
            }
            self.mounted -= 1;
        }
        let _cleanup = std::fs::remove_dir(&self.path);
    }
}

struct MountPlan {
    source: String,
    filesystem: String,
    flags: MountFlags,
    data: CString,
    bind_readonly: bool,
}

impl MountPlan {
    fn readonly(value: &Mount) -> Result<Self, RuntimeError> {
        let options = if value.r#type == "overlay" {
            readonly_overlay_options(&value.options)?
        } else {
            value
                .options
                .iter()
                .filter(|option| {
                    !matches!(option.as_str(), "ro" | "rw")
                        && !option.starts_with("uidmap=")
                        && !option.starts_with("gidmap=")
                })
                .cloned()
                .chain(std::iter::once("ro".to_owned()))
                .collect()
        };
        let (flags, data) = mount_options(&options)?;
        let data = CString::new(data.join(",")).map_err(|_| RuntimeError::Unavailable {
            message: "containerd returned a mount option containing NUL".to_owned(),
        })?;
        Ok(Self {
            source: value.source.clone(),
            filesystem: value.r#type.clone(),
            flags,
            data,
            bind_readonly: flags.contains(MountFlags::BIND | MountFlags::RDONLY),
        })
    }

    fn mount(&self, target: &Path) -> Result<(), RuntimeError> {
        mount(
            self.source.as_str(),
            target,
            self.filesystem.as_str(),
            self.flags,
            (!self.data.as_bytes().is_empty()).then_some(self.data.as_c_str()),
        )
        .map_err(|error| mount_io("mount image rootfs", error))
    }

    fn finish(&self, target: &Path) -> Result<(), RuntimeError> {
        if !self.bind_readonly {
            return Ok(());
        }
        mount_remount(target, self.flags, "")
            .map_err(|error| mount_io("remount image rootfs read-only", error))
    }
}

fn readonly_overlay_options(options: &[String]) -> Result<Vec<String>, RuntimeError> {
    let mut output = Vec::with_capacity(options.len());
    let mut upper = None;
    for option in options {
        if let Some(path) = option.strip_prefix("upperdir=") {
            upper = Some(path);
        } else if !option.starts_with("workdir=")
            && !option.starts_with("uidmap=")
            && !option.starts_with("gidmap=")
            && !matches!(option.as_str(), "volatile" | "fsync=volatile")
        {
            output.push(option.clone());
        }
    }
    if let Some(upper) = upper {
        let lower = output
            .iter_mut()
            .find(|option| option.starts_with("lowerdir="))
            .ok_or_else(|| RuntimeError::Unavailable {
                message: "containerd overlay mount omitted `lowerdir`".to_owned(),
            })?;
        lower.insert_str("lowerdir=".len(), &format!("{upper}:"));
    }
    Ok(output)
}

fn mount_options(options: &[String]) -> Result<(MountFlags, Vec<String>), RuntimeError> {
    let mut flags = MountFlags::empty();
    let mut data = Vec::new();
    for option in options {
        match option.as_str() {
            "defaults" => {}
            "ro" => flags.insert(MountFlags::RDONLY),
            "rw" => flags.remove(MountFlags::RDONLY),
            "nosuid" => flags.insert(MountFlags::NOSUID),
            "suid" => flags.remove(MountFlags::NOSUID),
            "nodev" => flags.insert(MountFlags::NODEV),
            "dev" => flags.remove(MountFlags::NODEV),
            "noexec" => flags.insert(MountFlags::NOEXEC),
            "exec" => flags.remove(MountFlags::NOEXEC),
            "sync" => flags.insert(MountFlags::SYNCHRONOUS),
            "async" => flags.remove(MountFlags::SYNCHRONOUS),
            "dirsync" => flags.insert(MountFlags::DIRSYNC),
            "mand" => flags.insert(MountFlags::PERMIT_MANDATORY_FILE_LOCKING),
            "nomand" => flags.remove(MountFlags::PERMIT_MANDATORY_FILE_LOCKING),
            "noatime" => flags.insert(MountFlags::NOATIME),
            "atime" => flags.remove(MountFlags::NOATIME),
            "nodiratime" => flags.insert(MountFlags::NODIRATIME),
            "diratime" => flags.remove(MountFlags::NODIRATIME),
            "bind" => flags.insert(MountFlags::BIND),
            "rbind" => flags.insert(MountFlags::BIND | MountFlags::REC),
            "relatime" => flags.insert(MountFlags::RELATIME),
            "norelatime" => flags.remove(MountFlags::RELATIME),
            "strictatime" => flags.insert(MountFlags::STRICTATIME),
            "nostrictatime" => flags.remove(MountFlags::STRICTATIME),
            option if option.starts_with("X-containerd.") => {
                return Err(RuntimeError::Unavailable {
                    message: format!(
                        "containerd internal mount option `{option}` was not activated"
                    ),
                });
            }
            option if option.starts_with("uidmap=") || option.starts_with("gidmap=") => {}
            _ => data.push(option.clone()),
        }
    }
    Ok((flags, data))
}

fn invalid_user(value: &str, reason: &str) -> RuntimeError {
    RuntimeError::InvalidSpec {
        message: format!("invalid image user `{value}`: {reason}"),
    }
}

fn malformed_file(file: &str, error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::InvalidSpec {
        message: format!("image `/etc/{file}` is malformed: {error}"),
    }
}

fn rootfs_io(action: &str, path: &Path, error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!(
            "failed to {action} image rootfs identity file `{}`: {error}",
            path.display()
        ),
    }
}

fn mount_io(action: &str, error: impl std::fmt::Display) -> RuntimeError {
    RuntimeError::Unavailable {
        message: format!("failed to {action}: {error}"),
    }
}
