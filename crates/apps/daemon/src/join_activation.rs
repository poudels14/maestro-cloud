use std::time::Duration;

use cluster::{MemberState, StoreJoinTicket, StoreProvider, StoreProviderError};
use kernel_store::Clock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct JoinActivationSettings {
    retry_interval: Duration,
    timeout: Duration,
}

impl JoinActivationSettings {
    pub(crate) fn new(retry_interval: Duration, timeout: Duration) -> Self {
        Self {
            retry_interval,
            timeout,
        }
    }
}

impl Default for JoinActivationSettings {
    fn default() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(30))
    }
}

pub(crate) async fn activate_joined_member(
    provider: &dyn StoreProvider,
    ticket: &StoreJoinTicket,
    clock: &dyn Clock,
    settings: JoinActivationSettings,
) -> Result<(), StoreProviderError> {
    let deadline = clock.now().saturating_add(settings.timeout);
    loop {
        match provider.activate_member(ticket).await {
            Ok(activation) if activation.state == MemberState::Active => return Ok(()),
            Ok(_) | Err(StoreProviderError::MemberNotReady { .. }) if clock.now() < deadline => {
                clock
                    .sleep_until(clock.now().saturating_add(settings.retry_interval))
                    .await;
            }
            Ok(activation) => {
                return Err(StoreProviderError::MemberNotReady {
                    node_id: activation.node_id,
                });
            }
            Err(error) => return Err(error),
        }
    }
}
