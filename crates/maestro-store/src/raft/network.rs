use std::fmt;

use openraft::BasicNode;
use openraft::error::{InstallSnapshotError, RPCError, RaftError, Unreachable};
use openraft::network::{RPCOption, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};

use super::TypeConfig;

type RpcResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>>;

pub(crate) struct LocalOnlyNetwork;

#[derive(Debug)]
struct NoTransport {
    target: u64,
}

impl RaftNetworkFactory<TypeConfig> for LocalOnlyNetwork {
    type Network = LocalOnlyClient;

    async fn new_client(&mut self, target: u64, _node: &BasicNode) -> Self::Network {
        LocalOnlyClient { target }
    }
}

pub(crate) struct LocalOnlyClient {
    target: u64,
}

impl RaftNetwork<TypeConfig> for LocalOnlyClient {
    async fn append_entries(
        &mut self,
        _request: AppendEntriesRequest<TypeConfig>,
        _option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<u64>> {
        Err(unreachable(self.target))
    }

    async fn install_snapshot(
        &mut self,
        _request: InstallSnapshotRequest<TypeConfig>,
        _option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        Err(unreachable(self.target))
    }

    async fn vote(
        &mut self,
        _request: VoteRequest<u64>,
        _option: RPCOption,
    ) -> RpcResult<VoteResponse<u64>> {
        Err(unreachable(self.target))
    }
}

impl fmt::Display for NoTransport {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            formatter,
            "no raft transport configured to reach node {}",
            self.target
        )
    }
}

impl std::error::Error for NoTransport {}

fn unreachable<E: std::error::Error>(target: u64) -> RPCError<u64, BasicNode, RaftError<u64, E>> {
    RPCError::Unreachable(Unreachable::new(&NoTransport { target }))
}
