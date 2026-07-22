use std::collections::VecDeque;
use std::sync::Mutex;
use std::time::Duration;

use async_trait::async_trait;
use cluster::{
    MemberActivation, MemberState, StoreJoinTicket, StoreMember, StoreProvider, StoreProviderError,
    StoreRecovery, StoreRecoveryPermit, StoreRuntime, StoreStartMode,
};
use kernel_api::NodeId;
use kernel_store::{Clock, MonotonicTime};

use crate::join_activation::{JoinActivationSettings, activate_joined_member};

struct ActivationProvider {
    outcomes: Mutex<VecDeque<Result<MemberActivation, StoreProviderError>>>,
}

#[async_trait]
impl StoreProvider for ActivationProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        Err(unused())
    }

    async fn stage_member(
        &self,
        _member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Err(unused())
    }

    async fn activate_member(
        &self,
        _ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        self.outcomes
            .lock()
            .map_err(|_| unused())?
            .pop_front()
            .unwrap_or_else(|| Err(unused()))
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Err(unused())
    }

    async fn recover(
        &self,
        _permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        Err(unused())
    }
}

struct AdvancingClock {
    now: Mutex<MonotonicTime>,
}

#[async_trait]
impl Clock for AdvancingClock {
    fn now(&self) -> MonotonicTime {
        self.now
            .lock()
            .map_or_else(|_| MonotonicTime::default(), |now| *now)
    }

    async fn sleep_until(&self, deadline: MonotonicTime) {
        if let Ok(mut now) = self.now.lock() {
            *now = deadline;
        }
    }
}

#[tokio::test]
async fn joined_member_retries_readiness_until_active() -> Result<(), Box<dyn std::error::Error>> {
    let node_id = NodeId::new("node-2")?;
    let provider = ActivationProvider {
        outcomes: Mutex::new(VecDeque::from([
            Err(StoreProviderError::MemberNotReady {
                node_id: node_id.clone(),
            }),
            Ok(MemberActivation {
                node_id: node_id.clone(),
                state: MemberState::Staged,
            }),
            Ok(MemberActivation {
                node_id: node_id.clone(),
                state: MemberState::Active,
            }),
        ])),
    };
    let clock = AdvancingClock {
        now: Mutex::new(MonotonicTime::default()),
    };
    activate_joined_member(
        &provider,
        &StoreJoinTicket::from_provider_data(node_id, b"ticket"),
        &clock,
        JoinActivationSettings::new(Duration::from_secs(1), Duration::from_secs(3)),
    )
    .await?;
    assert!(
        provider
            .outcomes
            .lock()
            .map_err(|_| "outcomes lock poisoned")?
            .is_empty()
    );
    Ok(())
}

#[tokio::test]
async fn joined_member_stops_at_the_activation_deadline() -> Result<(), Box<dyn std::error::Error>>
{
    let node_id = NodeId::new("node-2")?;
    let provider = ActivationProvider {
        outcomes: Mutex::new(VecDeque::from([
            Err(StoreProviderError::MemberNotReady {
                node_id: node_id.clone(),
            }),
            Err(StoreProviderError::MemberNotReady {
                node_id: node_id.clone(),
            }),
        ])),
    };
    let clock = AdvancingClock {
        now: Mutex::new(MonotonicTime::default()),
    };
    let result = activate_joined_member(
        &provider,
        &StoreJoinTicket::from_provider_data(node_id, b"ticket"),
        &clock,
        JoinActivationSettings::new(Duration::from_secs(1), Duration::from_secs(1)),
    )
    .await;
    let Err(error) = result else {
        return Err("activation unexpectedly succeeded".into());
    };
    assert!(matches!(error, StoreProviderError::MemberNotReady { .. }));
    Ok(())
}

fn unused() -> StoreProviderError {
    StoreProviderError::InvalidConfiguration {
        reason: "unused test operation".to_string(),
    }
}
