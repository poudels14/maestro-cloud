use std::sync::Arc;

use kernel_controller::{
    ControllerError, FencedStore, LeaderElector, LeaderIdentity, LeadershipLease,
};
use kernel_store::{Clock, Store, StoreError, StoreKey};
use tokio::sync::watch;

use crate::RoleError;
use crate::control_plane::{DaemonRoleSettings, LeaderWorkload, role_error};

#[allow(clippy::too_many_arguments)]
pub(crate) async fn run_leadership(
    store: Arc<dyn Store>,
    leader_key: StoreKey,
    elector: Arc<dyn LeaderElector>,
    identity: LeaderIdentity,
    mut lease: Option<Box<dyn LeadershipLease>>,
    workload: Option<Arc<dyn LeaderWorkload>>,
    clock: Arc<dyn Clock>,
    settings: DaemonRoleSettings,
    mut shutdown: watch::Receiver<bool>,
) -> Result<(), RoleError> {
    loop {
        if shutdown_requested(&shutdown) {
            return resign(lease).await;
        }
        if let Some(active) = lease.take() {
            lease = run_term(
                store.clone(),
                leader_key.clone(),
                active,
                workload.clone(),
                clock.clone(),
                settings,
                &mut shutdown,
            )
            .await?;
            if shutdown_requested(&shutdown) {
                return Ok(());
            }
        } else {
            let retry_at = clock.now().saturating_add(settings.campaign_retry_interval);
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return Ok(());
                    }
                }
                () = clock.sleep_until(retry_at) => {
                    match elector
                        .campaign(identity.clone(), settings.leadership_ttl)
                        .await
                    {
                        Ok(next_lease) => lease = next_lease,
                        Err(error) if retryable_campaign_error(&error) => {
                            tracing::warn!(
                                error = %error,
                                "controller leadership campaign temporarily failed"
                            );
                        }
                        Err(error) => {
                            return Err(role_error(
                                "retry controller leadership campaign",
                                error,
                            ));
                        }
                    }
                }
            }
        }
    }
}

fn retryable_campaign_error(error: &ControllerError) -> bool {
    matches!(
        error,
        ControllerError::Store(StoreError::Unavailable { .. })
    )
}

fn shutdown_requested(shutdown: &watch::Receiver<bool>) -> bool {
    *shutdown.borrow() || shutdown.has_changed().is_err()
}

async fn run_term(
    store: Arc<dyn Store>,
    leader_key: StoreKey,
    lease: Box<dyn LeadershipLease>,
    workload: Option<Arc<dyn LeaderWorkload>>,
    clock: Arc<dyn Clock>,
    settings: DaemonRoleSettings,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Box<dyn LeadershipLease>>, RoleError> {
    let Some(workload) = workload else {
        return keep_lease(lease, clock, settings, shutdown).await;
    };
    let fenced_store = Arc::new(FencedStore::new(store, leader_key, lease.token().clone()));
    let (stop, stop_receiver) = watch::channel(false);
    let worker = workload.run(fenced_store, stop_receiver);
    tokio::pin!(worker);

    loop {
        let keepalive_at = clock
            .now()
            .saturating_add(settings.leadership_keepalive_interval);
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    let _ = stop.send(true);
                    let worker_result = worker.await;
                    let resign_result = resign(Some(lease)).await;
                    worker_result?;
                    resign_result?;
                    return Ok(None);
                }
            }
            result = &mut worker => {
                let resign_result = resign(Some(lease)).await;
                result?;
                resign_result?;
                return Err(RoleError::new(
                    "leader workload stopped while its leadership fence remained active",
                ));
            }
            () = clock.sleep_until(keepalive_at) => {
                if lease.keep_alive().await.is_err() {
                    let _ = stop.send(true);
                    worker.await?;
                    return Ok(None);
                }
            }
        }
    }
}

async fn keep_lease(
    lease: Box<dyn LeadershipLease>,
    clock: Arc<dyn Clock>,
    settings: DaemonRoleSettings,
    shutdown: &mut watch::Receiver<bool>,
) -> Result<Option<Box<dyn LeadershipLease>>, RoleError> {
    loop {
        let keepalive_at = clock
            .now()
            .saturating_add(settings.leadership_keepalive_interval);
        tokio::select! {
            changed = shutdown.changed() => {
                if changed.is_err() || *shutdown.borrow() {
                    resign(Some(lease)).await?;
                    return Ok(None);
                }
            }
            () = clock.sleep_until(keepalive_at) => {
                if lease.keep_alive().await.is_err() {
                    return Ok(None);
                }
            }
        }
    }
}

async fn resign(lease: Option<Box<dyn LeadershipLease>>) -> Result<(), RoleError> {
    if let Some(lease) = lease {
        lease
            .resign()
            .await
            .map_err(|error| role_error("resign controller leadership", error))?;
    }
    Ok(())
}
