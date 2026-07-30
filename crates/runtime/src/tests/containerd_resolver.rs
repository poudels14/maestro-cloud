use std::net::{IpAddr, Ipv4Addr, Ipv6Addr};
use std::os::unix::fs::PermissionsExt;

use kernel_api::{ClusterId, WorkloadId};

use crate::containerd_resolver::prepare_resolver_file;

#[tokio::test]
async fn containerd_resolver_file_is_private_readable_and_replaceable() {
    let root = tempfile::tempdir().unwrap();
    let workload_id = WorkloadId::new("workload-1").unwrap();
    let path = prepare_resolver_file(
        root.path(),
        &workload_id,
        IpAddr::V4(Ipv4Addr::new(10, 42, 0, 1)),
        &ClusterId::new("cluster-1").unwrap(),
    )
    .await
    .unwrap();

    assert_eq!(
        std::fs::read_to_string(&path).unwrap(),
        "search cluster-1.maestro.internal\nnameserver 10.42.0.1\n"
    );
    assert_eq!(
        std::fs::metadata(&path).unwrap().permissions().mode() & 0o777,
        0o644
    );
    assert_eq!(
        std::fs::metadata(path.parent().unwrap())
            .unwrap()
            .permissions()
            .mode()
            & 0o777,
        0o700
    );

    prepare_resolver_file(
        root.path(),
        &workload_id,
        IpAddr::V6(Ipv6Addr::LOCALHOST),
        &ClusterId::new("cluster-2").unwrap(),
    )
    .await
    .unwrap();
    assert_eq!(
        std::fs::read_to_string(path).unwrap(),
        "search cluster-2.maestro.internal\nnameserver ::1\n"
    );
}

#[tokio::test]
async fn containerd_resolver_rejects_a_symlinked_workload_directory() {
    let root = tempfile::tempdir().unwrap();
    let outside = tempfile::tempdir().unwrap();
    let workloads = root.path().join("workloads");
    std::fs::create_dir(&workloads).unwrap();
    std::os::unix::fs::symlink(outside.path(), workloads.join("workload-1")).unwrap();

    assert!(
        prepare_resolver_file(
            root.path(),
            &WorkloadId::new("workload-1").unwrap(),
            IpAddr::V4(Ipv4Addr::LOCALHOST),
            &ClusterId::new("cluster-1").unwrap(),
        )
        .await
        .is_err()
    );
    assert!(!outside.path().join("resolv.conf").exists());
}
