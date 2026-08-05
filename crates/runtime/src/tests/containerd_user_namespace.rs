use std::collections::BTreeSet;

use kernel_api::WorkloadId;

use crate::RuntimeError;
use crate::containerd_user_namespace::ContainerdUserNamespaceAllocator;

#[test]
fn allocations_are_stable_unique_and_reusable() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let first = WorkloadId::new("workload-a")?;
    let second = WorkloadId::new("workload-b")?;
    let allocator = ContainerdUserNamespaceAllocator::new(root.path())?;

    let first_namespace = allocator.allocate(&first)?;
    assert_eq!(allocator.allocate(&first)?, first_namespace);
    let second_namespace = allocator.allocate(&second)?;
    assert_ne!(first_namespace.uid.host_id, second_namespace.uid.host_id);

    drop(allocator);
    let allocator = ContainerdUserNamespaceAllocator::new(root.path())?;
    assert_eq!(allocator.allocate(&first)?, first_namespace);
    assert_eq!(allocator.cleanup(&BTreeSet::from([second.clone()]))?, 1);

    let third = WorkloadId::new("workload-c")?;
    assert_eq!(allocator.allocate(&third)?, first_namespace);
    Ok(())
}

#[test]
fn duplicate_persisted_slots_are_rejected() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let allocator = ContainerdUserNamespaceAllocator::new(root.path())?;
    let first = WorkloadId::new("workload-a")?;
    let second = WorkloadId::new("workload-b")?;
    allocator.allocate(&first)?;
    let allocation_root = root.path().join("user-namespaces");
    std::fs::copy(
        allocation_root.join(format!("{first}.json")),
        allocation_root.join(format!("{second}.json")),
    )?;
    drop(allocator);

    assert!(matches!(
        ContainerdUserNamespaceAllocator::new(root.path()),
        Err(RuntimeError::Rejected { message }) if message.contains("allocated more than once")
    ));
    Ok(())
}

#[test]
fn concurrent_allocators_cannot_reuse_a_slot() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let first = std::sync::Arc::new(ContainerdUserNamespaceAllocator::new(root.path())?);
    let second = std::sync::Arc::new(ContainerdUserNamespaceAllocator::new(root.path())?);
    let first_id = WorkloadId::new("workload-a")?;
    let second_id = WorkloadId::new("workload-b")?;
    let first_task = {
        let allocator = first.clone();
        std::thread::spawn(move || allocator.allocate(&first_id))
    };
    let second_task = {
        let allocator = second.clone();
        std::thread::spawn(move || allocator.allocate(&second_id))
    };
    let first_namespace = first_task
        .join()
        .map_err(|_| std::io::Error::other("first allocator thread panicked"))??;
    let second_namespace = second_task
        .join()
        .map_err(|_| std::io::Error::other("second allocator thread panicked"))??;

    assert_ne!(first_namespace.uid.host_id, second_namespace.uid.host_id);
    Ok(())
}
