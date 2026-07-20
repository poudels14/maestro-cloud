use std::collections::BTreeSet;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use async_trait::async_trait;
use kernel_api::NodeId;
use kernel_store::{InMemoryStore, Store, TokioClock};

use crate::{
    MemberActivation, MemberState, StoreJoinTicket, StoreMember, StoreProvider,
    StoreProviderConfig, StoreProviderError, StoreRecovery, StoreRecoveryPermit,
    StoreRecoveryReport, StoreRuntime, StoreShutdown, StoreStartMode,
};

use super::fixtures::{provider_config, valid_config};

struct FakeProvider {
    config: StoreProviderConfig,
    running: Arc<AtomicBool>,
}

struct FakeRuntime {
    store: Arc<InMemoryStore>,
    running: Arc<AtomicBool>,
}

#[async_trait]
impl StoreRuntime for FakeRuntime {
    fn store(&self) -> Arc<dyn Store> {
        self.store.clone()
    }

    async fn shutdown(self: Box<Self>, _request: StoreShutdown) -> Result<(), StoreProviderError> {
        self.running.store(false, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl StoreProvider for FakeProvider {
    async fn start(
        &self,
        _mode: StoreStartMode,
    ) -> Result<Box<dyn StoreRuntime>, StoreProviderError> {
        self.running.store(true, Ordering::SeqCst);
        Ok(Box::new(FakeRuntime {
            store: Arc::new(InMemoryStore::new(Arc::new(TokioClock::new()))),
            running: self.running.clone(),
        }))
    }

    async fn stage_member(
        &self,
        member: StoreMember,
    ) -> Result<(StoreJoinTicket, MemberActivation), StoreProviderError> {
        Ok((
            StoreJoinTicket::from_provider_data(member.node_id.clone(), b"fake-membership"),
            MemberActivation {
                node_id: member.node_id,
                state: MemberState::Staged,
            },
        ))
    }

    async fn activate_member(
        &self,
        ticket: &StoreJoinTicket,
    ) -> Result<MemberActivation, StoreProviderError> {
        assert_eq!(ticket.provider_data()?, b"fake-membership");
        Ok(MemberActivation {
            node_id: ticket.node_id().clone(),
            state: MemberState::Active,
        })
    }

    async fn remove_member(&self, _node_id: &NodeId) -> Result<(), StoreProviderError> {
        Ok(())
    }

    async fn recover(
        &self,
        permit: StoreRecoveryPermit,
    ) -> Result<StoreRecovery, StoreProviderError> {
        let runtime = self.start(StoreStartMode::Restart).await?;
        let members_to_rejoin = permit
            .expected_members()
            .iter()
            .filter(|node_id| *node_id != permit.retained_node())
            .cloned()
            .collect();
        Ok(StoreRecovery {
            runtime,
            report: StoreRecoveryReport {
                retained_node: permit.retained_node().clone(),
                members_to_rejoin,
            },
        })
    }
}

#[tokio::test]
async fn provider_contract_owns_lifecycle_and_membership() -> Result<(), Box<dyn std::error::Error>>
{
    let config = provider_config(std::path::PathBuf::from("/var/lib/maestro/store"), "node-1")?;
    let running = Arc::new(AtomicBool::new(false));
    let provider = FakeProvider {
        config,
        running: running.clone(),
    };
    assert_eq!(provider.config.known_members().len(), 3);

    let runtime = provider.start(StoreStartMode::Bootstrap).await?;
    assert!(running.load(Ordering::SeqCst));
    let member = provider
        .config
        .known_members()
        .get(&NodeId::new("node-2")?)
        .ok_or("missing fixture node")?
        .clone();
    let (ticket, staged) = provider.stage_member(member).await?;
    assert_eq!(staged.state, MemberState::Staged);
    assert_eq!(
        provider.activate_member(&ticket).await?.state,
        MemberState::Active
    );
    runtime.shutdown(StoreShutdown::Immediate).await?;
    assert!(!running.load(Ordering::SeqCst));
    Ok(())
}

#[test]
fn recovery_permit_requires_a_retained_multi_member_identity()
-> Result<(), Box<dyn std::error::Error>> {
    let config = valid_config()?;
    let retained = NodeId::new("node-1")?;
    let singleton = BTreeSet::from([retained.clone()]);
    assert!(StoreRecoveryPermit::new(config.cluster_id, retained, singleton, 1_000).is_err());
    Ok(())
}
