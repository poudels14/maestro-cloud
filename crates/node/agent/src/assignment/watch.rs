use std::time::Duration;

use kernel_store::{StoreError, WatchStart};
use runtime::{EventCursor, EventRequest};
use tokio::sync::watch;

use super::AssignmentAgent;
use crate::assignment_error::AssignmentAgentError;
use crate::assignment_types::monotonic_deadline;

const RUNTIME_STREAM_RECONNECT_DELAY: Duration = Duration::from_secs(1);

impl AssignmentAgent {
    /// Runs watch-driven reconciliation with periodic full resync until shutdown.
    pub async fn run(
        &self,
        mut shutdown: watch::Receiver<bool>,
    ) -> Result<(), AssignmentAgentError> {
        let mut resync_at = self
            .monotonic_clock
            .now()
            .saturating_add(self.settings.resync_interval);
        let mut runtime_cursor: Option<EventCursor> = None;
        let mut runtime_events = None;
        let mut runtime_reconnect_at = self.monotonic_clock.now();
        loop {
            if *shutdown.borrow() {
                #[cfg(unix)]
                self.node_api.shutdown_all().await?;
                return Ok(());
            }
            let reconcile_deadline = self
                .monotonic_clock
                .now()
                .saturating_add(self.settings.reconcile_timeout);
            let reconcile = tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                result = self.reconcile_with_cursor() => Some(result),
                () = self.monotonic_clock.sleep_until(reconcile_deadline) => None,
            };
            let Some(reconcile) = reconcile else {
                continue;
            };
            let (report, cursor) = match reconcile {
                Ok(reconciled) => reconciled,
                Err(error) if error.retryable() => {
                    if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error),
            };
            let retry_at = report.requeue_at.map(|deadline| {
                monotonic_deadline(
                    self.monotonic_clock.as_ref(),
                    self.status_clock.as_ref(),
                    deadline,
                )
            });
            let mut events = match self
                .store
                .watch(self.keyspace.resources(), WatchStart::After(cursor))
            {
                Ok(events) => events,
                Err(error) if retryable_watch_error(&error) => {
                    if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                        #[cfg(unix)]
                        self.node_api.shutdown_all().await?;
                        return Ok(());
                    }
                    continue;
                }
                Err(error) => return Err(error.into()),
            };
            if runtime_events.is_none() && self.monotonic_clock.now() >= runtime_reconnect_at {
                runtime_events = match self
                    .runtime
                    .events(EventRequest {
                        cluster_id: self.settings.cluster_id.clone(),
                        node_id: self.settings.node_id.clone(),
                        after: runtime_cursor.clone(),
                    })
                    .await
                {
                    Ok(events) => Some(events),
                    Err(_) => {
                        runtime_reconnect_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
                        None
                    }
                };
            }
            loop {
                tokio::select! {
                    changed = shutdown.changed() => {
                        if changed.is_err() || *shutdown.borrow() {
                            #[cfg(unix)]
                            self.node_api.shutdown_all().await?;
                            return Ok(());
                        }
                    }
                    event = events.next() => {
                        match event {
                            Ok(_) => break,
                            Err(error) if retryable_watch_error(&error) => {
                                if self.wait_for_retry_or_shutdown(&mut shutdown).await {
                                    #[cfg(unix)]
                                    self.node_api.shutdown_all().await?;
                                    return Ok(());
                                }
                                break;
                            }
                            Err(error) => return Err(error.into()),
                        }
                    }
                    event = async {
                        match runtime_events.as_mut() {
                            Some(events) => events.next().await,
                            None => std::future::pending().await,
                        }
                    }, if runtime_events.is_some() && retry_at.is_none() => {
                        match event {
                            Ok(Some(event)) => {
                                runtime_cursor = Some(event.cursor);
                                break;
                            }
                            Ok(None) | Err(_) => {
                                runtime_events = None;
                                runtime_reconnect_at = self
                                    .monotonic_clock
                                    .now()
                                    .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
                            }
                        }
                    }
                    () = self.monotonic_clock.sleep_until(runtime_reconnect_at), if runtime_events.is_none() => {
                        break;
                    }
                    () = async {
                        match retry_at {
                            Some(retry_at) => self.monotonic_clock.sleep_until(retry_at).await,
                            None => std::future::pending().await,
                        }
                    }, if retry_at.is_some() => {
                        break;
                    }
                    () = self.monotonic_clock.sleep_until(resync_at) => {
                        resync_at = self
                            .monotonic_clock
                            .now()
                            .saturating_add(self.settings.resync_interval);
                        break;
                    }
                }
            }
        }
    }

    async fn wait_for_retry_or_shutdown(&self, shutdown: &mut watch::Receiver<bool>) -> bool {
        let retry_at = self
            .monotonic_clock
            .now()
            .saturating_add(RUNTIME_STREAM_RECONNECT_DELAY);
        loop {
            tokio::select! {
                changed = shutdown.changed() => {
                    if changed.is_err() || *shutdown.borrow() {
                        return true;
                    }
                }
                () = self.monotonic_clock.sleep_until(retry_at) => return false,
            }
        }
    }
}

fn retryable_watch_error(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::CursorExpired { .. }
            | StoreError::SessionExpired { .. }
            | StoreError::Unavailable { .. }
    )
}
