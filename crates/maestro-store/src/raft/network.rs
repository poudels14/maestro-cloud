use std::collections::HashMap;
use std::error::Error;
use std::future::Future;

use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_client::RaftClient;
use openraft::BasicNode;
use openraft::error::{
    InstallSnapshotError, NetworkError, RPCError, RaftError, RemoteError, Timeout, Unreachable,
};
use openraft::network::{RPCOption, RPCTypes, RaftNetwork, RaftNetworkFactory};
use openraft::raft::{
    AppendEntriesRequest, AppendEntriesResponse, InstallSnapshotRequest, InstallSnapshotResponse,
    VoteRequest, VoteResponse,
};
use serde::Serialize;
use serde::de::DeserializeOwned;
use thiserror::Error;
use tonic::transport::{Channel, ClientTlsConfig, Endpoint};
use tonic::{Code, Status};

use super::{MAX_MESSAGE_BYTES, TypeConfig};

type RpcResult<T, E = openraft::error::Infallible> =
    Result<T, RPCError<u64, BasicNode, RaftError<u64, E>>>;

pub(crate) struct GrpcNetwork {
    node_id: u64,
    tls: Option<ClientTlsConfig>,
    channels: HashMap<u64, (String, Channel)>,
}

pub(crate) struct GrpcClient {
    node_id: u64,
    target: u64,
    node: BasicNode,
    channel: Result<Channel, TransportError>,
}

#[derive(Debug, Error)]
enum TransportError {
    #[error("no raft transport credentials configured to reach node {0}")]
    NoCredentials(u64),
    #[error("invalid raft endpoint for node {target}: {reason}")]
    Endpoint { target: u64, reason: String },
}

impl GrpcNetwork {
    pub(crate) fn new(node_id: u64, tls: Option<ClientTlsConfig>) -> Self {
        Self {
            node_id,
            tls,
            channels: HashMap::new(),
        }
    }

    fn connect(&mut self, target: u64, address: &str) -> Result<Channel, TransportError> {
        let channel = match self.tls.clone() {
            Some(tls) => Endpoint::from_shared(format!("https://{address}"))
                .and_then(|endpoint| endpoint.tls_config(tls))
                .map(|endpoint| endpoint.connect_lazy())
                .map_err(|error| TransportError::Endpoint {
                    target,
                    reason: error.to_string(),
                }),
            None => Err(TransportError::NoCredentials(target)),
        };
        if let Ok(channel) = &channel {
            self.channels
                .insert(target, (address.to_string(), channel.clone()));
        }
        channel
    }
}

impl RaftNetworkFactory<TypeConfig> for GrpcNetwork {
    type Network = GrpcClient;

    async fn new_client(&mut self, target: u64, node: &BasicNode) -> Self::Network {
        let channel = match self.channels.get(&target) {
            Some((address, channel)) if *address == node.addr => Ok(channel.clone()),
            _ => self.connect(target, &node.addr),
        };
        GrpcClient {
            node_id: self.node_id,
            target,
            node: node.clone(),
            channel,
        }
    }
}

impl RaftNetwork<TypeConfig> for GrpcClient {
    async fn append_entries(
        &mut self,
        request: AppendEntriesRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<AppendEntriesResponse<u64>> {
        self.call(
            RPCTypes::AppendEntries,
            &request,
            option,
            |mut client, envelope| async move { client.append_entries(envelope).await },
        )
        .await
    }

    async fn install_snapshot(
        &mut self,
        request: InstallSnapshotRequest<TypeConfig>,
        option: RPCOption,
    ) -> RpcResult<InstallSnapshotResponse<u64>, InstallSnapshotError> {
        self.call(
            RPCTypes::InstallSnapshot,
            &request,
            option,
            |mut client, envelope| async move { client.install_snapshot(envelope).await },
        )
        .await
    }

    async fn vote(
        &mut self,
        request: VoteRequest<u64>,
        option: RPCOption,
    ) -> RpcResult<VoteResponse<u64>> {
        self.call(
            RPCTypes::Vote,
            &request,
            option,
            |mut client, envelope| async move { client.vote(envelope).await },
        )
        .await
    }
}

impl GrpcClient {
    async fn call<Payload, Reply, RemoteFailure, Rpc, Pending>(
        &self,
        action: RPCTypes,
        request: &Payload,
        option: RPCOption,
        rpc: Rpc,
    ) -> RpcResult<Reply, RemoteFailure>
    where
        Payload: Serialize,
        Reply: DeserializeOwned,
        RemoteFailure: Error,
        RaftError<u64, RemoteFailure>: DeserializeOwned,
        Rpc: FnOnce(RaftClient<Channel>, Envelope) -> Pending,
        Pending: Future<Output = Result<tonic::Response<Envelope>, Status>>,
    {
        let channel = self
            .channel
            .as_ref()
            .map_err(|error| RPCError::Unreachable(Unreachable::new(error)))?
            .clone();
        let json = serde_json::to_vec(request)
            .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        let client = RaftClient::new(channel)
            .max_decoding_message_size(MAX_MESSAGE_BYTES)
            .max_encoding_message_size(MAX_MESSAGE_BYTES);
        let reply = tokio::time::timeout(option.hard_ttl(), rpc(client, Envelope { json }))
            .await
            .map_err(|_| {
                RPCError::Timeout(Timeout {
                    action,
                    id: self.node_id,
                    target: self.target,
                    timeout: option.hard_ttl(),
                })
            })?
            .map_err(|status| status_error(&status))?;
        let outcome: Result<Reply, RaftError<u64, RemoteFailure>> =
            serde_json::from_slice(&reply.into_inner().json)
                .map_err(|error| RPCError::Network(NetworkError::new(&error)))?;
        outcome.map_err(|error| {
            RPCError::RemoteError(RemoteError::new_with_node(
                self.target,
                self.node.clone(),
                error,
            ))
        })
    }
}

fn status_error<RemoteFailure: Error>(
    status: &Status,
) -> RPCError<u64, BasicNode, RaftError<u64, RemoteFailure>> {
    match status.code() {
        Code::Unavailable => RPCError::Unreachable(Unreachable::new(status)),
        _ => RPCError::Network(NetworkError::new(status)),
    }
}
