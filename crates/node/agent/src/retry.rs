use std::time::Duration;

use kernel_store::{Clock, StoreError};
use tokio::sync::watch;

const STORE_RETRY_DELAY: Duration = Duration::from_secs(1);

pub(crate) fn retryable_store_error(error: &StoreError) -> bool {
    matches!(
        error,
        StoreError::CursorExpired { .. }
            | StoreError::SessionExpired { .. }
            | StoreError::Unavailable { .. }
    )
}

pub(crate) async fn wait_for_store_retry_or_shutdown(
    clock: &dyn Clock,
    shutdown: &mut watch::Receiver<bool>,
) -> bool {
    let retry_at = clock.now().saturating_add(STORE_RETRY_DELAY);
    tokio::select! {
        changed = shutdown.changed() => changed.is_err() || *shutdown.borrow(),
        () = clock.sleep_until(retry_at) => false,
    }
}
