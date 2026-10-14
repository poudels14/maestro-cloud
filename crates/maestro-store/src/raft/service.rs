use maestro_proto::pb::Envelope;
use maestro_proto::pb::raft_server::Raft as RaftRpc;
use maestro_proto::{CertRole, peer_identity};
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

    fn authenticated_payload<Payload: DeserializeOwned>(
        &self,
        request: Request<Envelope>,
    ) -> Result<Payload, Status> {
        let identity = request
            .peer_certs()
            .and_then(|certificates| peer_identity(&certificates))
            .ok_or_else(|| {
                Status::unauthenticated("raft peers must present a cluster client certificate")
            })?;
        let metrics = self.raft.metrics().borrow().clone();
        let membership = metrics.membership_config.membership();
        let members = membership
            .nodes()
            .map(|(node_id, _)| *node_id)
            .collect::<Vec<_>>();
        let allowed = match members.is_empty() {
            true => identity.role == Some(CertRole::Voter),
            false => members.contains(&identity.node_id),
        };
        match allowed {
            true => serde_json::from_slice(&request.into_inner().json).map_err(|error| {
                Status::invalid_argument(format!("malformed raft payload: {error}"))
            }),
            false => Err(Status::permission_denied(format!(
                "node {} is not a raft member",
                identity.node_id
            ))),
        }
    }
}

#[tonic::async_trait]
impl RaftRpc for RaftService {
    async fn append_entries(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let rpc = self.authenticated_payload(request)?;
        reply(&self.raft.append_entries(rpc).await)
    }

    async fn vote(&self, request: Request<Envelope>) -> Result<Response<Envelope>, Status> {
        let rpc = self.authenticated_payload(request)?;
        reply(&self.raft.vote(rpc).await)
    }

    async fn install_snapshot(
        &self,
        request: Request<Envelope>,
    ) -> Result<Response<Envelope>, Status> {
        let rpc = self.authenticated_payload(request)?;
        reply(&self.raft.install_snapshot(rpc).await)
    }
}

fn reply<Outcome: Serialize>(outcome: &Outcome) -> Result<Response<Envelope>, Status> {
    serde_json::to_vec(outcome)
        .map(|json| Response::new(Envelope { json }))
        .map_err(|error| Status::internal(format!("failed to encode raft reply: {error}")))
}
