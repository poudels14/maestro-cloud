use std::io::Read;

use flate2::read::GzDecoder;

use crate::spec::RuntimeError;

const PASSWD_PATH: &str = "etc/passwd";
const GROUP_PATH: &str = "etc/group";
const PASSWD_WHITEOUT: &str = "etc/.wh.passwd";
const GROUP_WHITEOUT: &str = "etc/.wh.group";
const GZIP_MAGIC: [u8; 2] = [0x1f, 0x8b];

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ProcessUser {
    pub uid: u32,
    pub gid: u32,
    pub additional_gids: Vec<u32>,
}

#[derive(Debug, Default, PartialEq, Eq)]
pub struct AccountFiles {
    pub passwd: Option<String>,
    pub group: Option<String>,
}

pub fn numeric_user(user: Option<&str>) -> Option<ProcessUser> {
    let trimmed = user.unwrap_or_default().trim();
    let (uid_text, gid_text) = trimmed.split_once(':').unwrap_or((trimmed, ""));
    let uid = match uid_text {
        "" | "root" => Some(0),
        numeric => numeric.parse::<u32>().ok(),
    };
    let gid = match gid_text {
        "" => uid,
        "root" => Some(0),
        numeric => numeric.parse::<u32>().ok(),
    };
    uid.zip(gid).map(|(uid, gid)| ProcessUser {
        uid,
        gid,
        additional_gids: Vec::new(),
    })
}

pub fn resolve_user(user: &str, files: &AccountFiles) -> Result<ProcessUser, RuntimeError> {
    let unsupported = |reason: &str| RuntimeError::UnsupportedUser(format!("{user}: {reason}"));
    let (user_text, group_text) = user.trim().split_once(':').unwrap_or((user.trim(), ""));
    let passwd = files.passwd.as_deref().unwrap_or_default();
    let group = files.group.as_deref().unwrap_or_default();
    let account = passwd_entries(passwd).find(|entry| {
        entry.name == user_text || user_text.parse::<u32>().is_ok_and(|uid| uid == entry.uid)
    });
    let uid = match (user_text.parse::<u32>(), &account) {
        (Ok(uid), _) => Ok(uid),
        (Err(_), Some(entry)) => Ok(entry.uid),
        (Err(_), None) => Err(unsupported("no such user in the image's /etc/passwd")),
    }?;
    let gid = match (group_text, group_text.parse::<u32>()) {
        ("", _) => account.as_ref().map_or(uid, |entry| entry.gid),
        (_, Ok(gid)) => gid,
        (name, Err(_)) => group_entries(group)
            .find(|entry| entry.name == name)
            .map(|entry| entry.gid)
            .ok_or_else(|| unsupported("no such group in the image's /etc/group"))?,
    };
    let additional_gids = match &account {
        Some(entry) => group_entries(group)
            .filter(|group_entry| group_entry.members.contains(&entry.name))
            .map(|group_entry| group_entry.gid)
            .filter(|additional| *additional != gid)
            .collect(),
        None => Vec::new(),
    };
    Ok(ProcessUser {
        uid,
        gid,
        additional_gids,
    })
}

pub fn read_account_files(layer: &[u8], files: &mut AccountFiles) -> Result<(), RuntimeError> {
    let reader: Box<dyn Read> = match layer.starts_with(&GZIP_MAGIC) {
        true => Box::new(GzDecoder::new(layer)),
        false => Box::new(layer),
    };
    let mut archive = tar::Archive::new(reader);
    for entry in archive.entries()? {
        let mut entry = entry?;
        let path = entry
            .path()?
            .to_string_lossy()
            .trim_start_matches("./")
            .to_string();
        let mut contents = String::new();
        match path.as_str() {
            PASSWD_PATH if files.passwd.is_none() => {
                entry.read_to_string(&mut contents)?;
                files.passwd = Some(contents);
            }
            GROUP_PATH if files.group.is_none() => {
                entry.read_to_string(&mut contents)?;
                files.group = Some(contents);
            }
            PASSWD_WHITEOUT if files.passwd.is_none() => files.passwd = Some(String::new()),
            GROUP_WHITEOUT if files.group.is_none() => files.group = Some(String::new()),
            _ => {}
        }
    }
    Ok(())
}

struct PasswdEntry<'a> {
    name: &'a str,
    uid: u32,
    gid: u32,
}

struct GroupEntry<'a> {
    name: &'a str,
    gid: u32,
    members: Vec<&'a str>,
}

fn passwd_entries(passwd: &str) -> impl Iterator<Item = PasswdEntry<'_>> {
    passwd.lines().filter_map(|line| {
        let mut fields = line.split(':');
        let name = fields.next()?;
        let uid = fields.nth(1)?.parse().ok()?;
        let gid = fields.next()?.parse().ok()?;
        Some(PasswdEntry { name, uid, gid })
    })
}

fn group_entries(group: &str) -> impl Iterator<Item = GroupEntry<'_>> {
    group.lines().filter_map(|line| {
        let mut fields = line.split(':');
        let name = fields.next()?;
        let gid = fields.nth(1)?.parse().ok()?;
        let members = fields
            .next()
            .unwrap_or_default()
            .split(',')
            .map(str::trim)
            .filter(|member| !member.is_empty())
            .collect();
        Some(GroupEntry { name, gid, members })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    const PASSWD: &str = "root:x:0:0:root:/root:/bin/bash\nnode:x:1000:1000::/home/node:/bin/sh\n";
    const GROUP: &str = "root:x:0:\nnode:x:1000:\nwww-data:x:33:node,nginx\naudio:x:29:\n";

    fn files() -> AccountFiles {
        AccountFiles {
            passwd: Some(PASSWD.to_string()),
            group: Some(GROUP.to_string()),
        }
    }

    #[test]
    fn parses_numeric_users_without_the_image() {
        assert_eq!(numeric_user(None), Some(ProcessUser::default()));
        assert_eq!(
            numeric_user(Some("101")).map(|user| (user.uid, user.gid)),
            Some((101, 101))
        );
        assert_eq!(numeric_user(Some("101:root")).map(|user| user.gid), Some(0));
        assert_eq!(numeric_user(Some("node")), None);
    }

    #[test]
    fn resolves_named_users_and_groups() {
        assert_eq!(
            resolve_user("node", &files()).unwrap(),
            ProcessUser {
                uid: 1000,
                gid: 1000,
                additional_gids: vec![33],
            }
        );
        assert_eq!(resolve_user("node:audio", &files()).unwrap().gid, 29);
        assert_eq!(resolve_user("1000:www-data", &files()).unwrap().gid, 33);
        assert!(matches!(
            resolve_user("ghost", &files()),
            Err(RuntimeError::UnsupportedUser(_))
        ));
    }

    #[test]
    fn finds_account_files_in_gzipped_and_plain_layers() {
        let mut builder = tar::Builder::new(Vec::new());
        for (path, contents) in [("./etc/passwd", PASSWD), ("etc/group", GROUP)] {
            let mut header = tar::Header::new_gnu();
            header.set_size(contents.len() as u64);
            header.set_mode(0o644);
            header.set_cksum();
            builder
                .append_data(&mut header, path, contents.as_bytes())
                .unwrap();
        }
        let plain = builder.into_inner().unwrap();
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::fast());
        std::io::Write::write_all(&mut encoder, &plain).unwrap();
        let gzipped = encoder.finish().unwrap();
        for layer in [plain, gzipped] {
            let mut found = AccountFiles::default();
            read_account_files(&layer, &mut found).unwrap();
            assert_eq!(found, files());
        }
    }
}
