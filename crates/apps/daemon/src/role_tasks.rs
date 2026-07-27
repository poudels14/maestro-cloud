use std::time::Duration;

use futures_util::stream::{FuturesUnordered, StreamExt};
use kernel_store::Clock;
use tokio::task::JoinHandle;

use crate::RoleError;

pub(crate) async fn wait_for_role_task(
    tasks: &mut Vec<JoinHandle<Result<(), RoleError>>>,
) -> RoleError {
    let mut pending = FuturesUnordered::new();
    for (index, task) in tasks.iter_mut().enumerate() {
        pending.push(async move { (index, task.await) });
    }
    let result = pending.next().await;
    drop(pending);
    match result {
        Some((index, result)) => {
            tasks.swap_remove(index);
            task_failure(result)
        }
        None => RoleError::new("role runtime has no active worker"),
    }
}

pub(crate) async fn shutdown_role_tasks(
    tasks: &mut Vec<JoinHandle<Result<(), RoleError>>>,
    clock: &dyn Clock,
    grace: Duration,
) -> Vec<String> {
    let deadline = clock.now().saturating_add(grace);
    let mut pending = FuturesUnordered::new();
    let mut abort_handles = Vec::with_capacity(tasks.len());
    for task in tasks.drain(..) {
        abort_handles.push(task.abort_handle());
        pending.push(task);
    }
    let timeout = clock.sleep_until(deadline);
    tokio::pin!(timeout);
    let mut failures = Vec::new();
    while !pending.is_empty() {
        tokio::select! {
            result = pending.next() => {
                if let Some(result) = result {
                    record_result(result, &mut failures);
                }
            }
            () = &mut timeout => {
                let remaining = pending.len();
                for handle in &abort_handles {
                    handle.abort();
                }
                while pending.next().await.is_some() {}
                failures.push(format!(
                    "role task shutdown exceeded {grace:?}; aborted {remaining} remaining task(s)"
                ));
                break;
            }
        }
    }
    failures
}

fn record_result(
    result: Result<Result<(), RoleError>, tokio::task::JoinError>,
    failures: &mut Vec<String>,
) {
    match result {
        Ok(Ok(())) => {}
        Ok(Err(error)) => failures.push(error.to_string()),
        Err(error) => failures.push(format!("role task failed: {error}")),
    }
}

fn task_failure(result: Result<Result<(), RoleError>, tokio::task::JoinError>) -> RoleError {
    match result {
        Ok(Ok(())) => RoleError::new("role worker exited unexpectedly"),
        Ok(Err(error)) => error,
        Err(error) => RoleError::new(format!("role task failed: {error}")),
    }
}
