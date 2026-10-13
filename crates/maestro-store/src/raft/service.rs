use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_server::Raft as RaftRpc;
use openraft::Raft;
use serde::Serialize;
use serde::de::DeserializeOwned;
use tonic::{Request, Response, Status};

use super::TypeConfig;

pub struct RaftService {
    raft: Raft<TypeConfig>,
}

impl RaftService {
    pub(crate) fn new(raft: Raft<TypeConfig>) -> Self {
        Self { raft }
    }
}

#[tonic::async_trait]
impl RaftRpc for RaftService {
    async fn append_entries(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let rpc = authenticated_payload(request)?;
        reply(&self.raft.append_entries(rpc).await)
    }

    async fn vote(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let rpc = authenticated_payload(request)?;
        reply(&self.raft.vote(rpc).await)
    }

    async fn install_snapshot(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let rpc = authenticated_payload(request)?;
        reply(&self.raft.install_snapshot(rpc).await)
    }
}

fn authenticated_payload<Payload: DeserializeOwned>(
    request: Request<Envelope>,
) -> Result<Payload, Status> {
    match request.peer_certs().is_some_and(|certs| !certs.is_empty()) {
        true => serde_json::from_slice(&request.into_inner().json)
            .map_err(|error| Status::invalid_argument(format!("malformed raft payload: {error}"))),
        false => Err(Status::unauthenticated(
            "raft peers must present a cluster client certificate",
        )),
    }
}

fn reply<Outcome: Serialize>(outcome: &Outcome) -> Result<Response<Envelope>, Status> {
    serde_json::to_vec(outcome)
        .map(|json| Response::new(Envelope { json }))
        .map_err(|error| Status::internal(format!("failed to encode raft reply: {error}")))
}
