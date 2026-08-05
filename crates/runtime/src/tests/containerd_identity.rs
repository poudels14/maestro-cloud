use std::fs;

use crate::containerd_identity::{readonly_overlay_options_for_test, resolve_image_user_in_rootfs};
use crate::{RuntimeError, WorkloadUser};

fn rootfs() -> tempfile::TempDir {
    let root = tempfile::tempdir().unwrap();
    fs::create_dir(root.path().join("etc")).unwrap();
    fs::write(
        root.path().join("etc/passwd"),
        b"root:x:0:0:root:/root:/bin/sh\nbaton:x:1000:1001:Baton:/home/baton:/bin/sh\n",
    )
    .unwrap();
    fs::write(
        root.path().join("etc/group"),
        b"root:x:0:root\nbaton:x:1001:baton\noperators:x:2000:baton\n",
    )
    .unwrap();
    root
}

#[test]
fn resolves_named_image_user_from_image_passwd() {
    let root = rootfs();
    assert_eq!(
        resolve_image_user_in_rootfs("baton", root.path()).unwrap(),
        WorkloadUser {
            user_id: 1000,
            group_id: 1001,
        }
    );
    assert_eq!(
        resolve_image_user_in_rootfs("baton:operators", root.path()).unwrap(),
        WorkloadUser {
            user_id: 1000,
            group_id: 2000,
        }
    );
}

#[test]
fn numeric_image_user_uses_passwd_primary_group_or_root_group() {
    let root = rootfs();
    assert_eq!(
        resolve_image_user_in_rootfs("1000", root.path()).unwrap(),
        WorkloadUser {
            user_id: 1000,
            group_id: 1001,
        }
    );
    assert_eq!(
        resolve_image_user_in_rootfs("1234", root.path()).unwrap(),
        WorkloadUser {
            user_id: 1234,
            group_id: 0,
        }
    );
    assert_eq!(
        resolve_image_user_in_rootfs("1234:4321", root.path()).unwrap(),
        WorkloadUser {
            user_id: 1234,
            group_id: 4321,
        }
    );
}

#[test]
fn missing_named_image_identity_is_an_invalid_specification() {
    let root = rootfs();
    assert!(matches!(
        resolve_image_user_in_rootfs("missing", root.path()),
        Err(RuntimeError::InvalidSpec { message })
            if message.contains("does not exist in image `/etc/passwd`")
    ));
    assert!(matches!(
        resolve_image_user_in_rootfs("baton:missing", root.path()),
        Err(RuntimeError::InvalidSpec { message })
            if message.contains("does not exist in image `/etc/group`")
    ));
}

#[test]
fn rejects_out_of_range_numeric_image_identities() {
    let root = rootfs();
    for value in ["-1", "2147483648", "1000:-1"] {
        assert!(matches!(
            resolve_image_user_in_rootfs(value, root.path()),
            Err(RuntimeError::InvalidSpec { .. })
        ));
    }
}

#[test]
fn rejects_malformed_image_passwd() {
    let root = rootfs();
    fs::write(root.path().join("etc/passwd"), b"baton:x:not-a-uid:1001\n").unwrap();
    assert!(matches!(
        resolve_image_user_in_rootfs("baton", root.path()),
        Err(RuntimeError::InvalidSpec { message })
            if message.contains("image `/etc/passwd` is malformed")
    ));
}

#[test]
fn converts_idmapped_active_overlay_to_a_read_only_view() {
    let options = [
        "workdir=/snapshots/3/work".to_owned(),
        "upperdir=/snapshots/3/fs".to_owned(),
        "lowerdir=/snapshots/2/fs:/snapshots/1/fs".to_owned(),
        "uidmap=0:1048576:65536".to_owned(),
        "gidmap=0:1048576:65536".to_owned(),
        "volatile".to_owned(),
    ];
    assert_eq!(
        readonly_overlay_options_for_test(&options).unwrap(),
        vec!["lowerdir=/snapshots/3/fs:/snapshots/2/fs:/snapshots/1/fs"]
    );
}
