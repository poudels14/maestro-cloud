use std::future::Future;

use maestro_store::{Consensus, ConsensusError, Mutation, StateMachine};

pub trait StoreWriter: Send + Sync + 'static {
    fn write(
        &self,
        mutations: Vec<Mutation>,
    ) -> impl Future<Output = Result<u64, ConsensusError>> + Send;

    fn state(&self) -> &StateMachine;
}

impl StoreWriter for Consensus {
    async fn write(&self, mutations: Vec<Mutation>) -> Result<u64, ConsensusError> {
        Consensus::write(self, mutations).await
    }

    fn state(&self) -> &StateMachine {
        Consensus::state(self)
    }
}
