use std::net::{IpAddr, Ipv4Addr, SocketAddr};

use axum::extract::rejection::JsonRejection;
use axum::extract::{ConnectInfo, DefaultBodyLimit, State};
use axum::routing::post;
use axum::{Json, Router};
use cluster::{
    AdmissionCoordinator, AdmissionCoordinatorError, AdmissionError, CaDiscoveryRequest,
    CaDiscoveryResponse, CertificateValidity, EncryptedJoinResponse, SignedJoinRequest,
};
use time::{Duration, OffsetDateTime};

use crate::{ApiError, AppState, mutation};

const MAXIMUM_ADMISSION_REQUEST_BYTES: usize = 64 * 1_024;
const NODE_CERTIFICATE_VALIDITY_DAYS: i64 = 825;

pub(super) fn public_router() -> Router<AppState> {
    Router::new()
        .route("/api/cluster/ca", post(discover_ca))
        .route("/api/cluster/join", post(join))
        .layer(DefaultBodyLimit::max(MAXIMUM_ADMISSION_REQUEST_BYTES))
}

async fn discover_ca(
    State(state): State<AppState>,
    payload: Result<Json<CaDiscoveryRequest>, JsonRejection>,
) -> Result<Json<CaDiscoveryResponse>, ApiError> {
    let coordinator = coordinator(&state)?;
    let request = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "CA discovery"))?
        .0;
    coordinator
        .discover(&request)
        .map(Json)
        .map_err(coordinator_error)
}

async fn join(
    State(state): State<AppState>,
    ConnectInfo(source): ConnectInfo<SocketAddr>,
    payload: Result<Json<SignedJoinRequest>, JsonRejection>,
) -> Result<Json<EncryptedJoinResponse>, ApiError> {
    let coordinator = coordinator(&state)?;
    let signed = payload
        .map_err(|rejection| mutation::json_rejection(rejection, "cluster join"))?
        .0;
    let now_unix_ms = state.timestamp_clock.now().0;
    let validity = certificate_validity(now_unix_ms)?;
    coordinator
        .admit(
            &signed.request,
            &signed.signature,
            ipv4_source(source.ip())?,
            now_unix_ms,
            validity,
        )
        .await
        .map(Json)
        .map_err(coordinator_error)
}

fn coordinator(state: &AppState) -> Result<&AdmissionCoordinator, ApiError> {
    state.admission_coordinator.as_deref().ok_or_else(|| {
        ApiError::service_unavailable("cluster admission is unavailable on this node")
    })
}

fn ipv4_source(address: IpAddr) -> Result<Ipv4Addr, ApiError> {
    match address {
        IpAddr::V4(address) => Ok(address),
        IpAddr::V6(address) => address.to_ipv4_mapped().ok_or_else(|| {
            ApiError::forbidden("cluster join requires a declared IPv4 control address")
        }),
    }
}

fn certificate_validity(now_unix_ms: i64) -> Result<CertificateValidity, ApiError> {
    let now = OffsetDateTime::from_unix_timestamp_nanos(i128::from(now_unix_ms) * 1_000_000)
        .map_err(|_| ApiError::internal("current time is outside certificate limits"))?;
    let not_before = now
        .checked_sub(Duration::minutes(5))
        .ok_or_else(|| ApiError::internal("certificate activation time is out of range"))?;
    let not_after = now
        .checked_add(Duration::days(NODE_CERTIFICATE_VALIDITY_DAYS))
        .ok_or_else(|| ApiError::internal("certificate expiration time is out of range"))?;
    CertificateValidity::new(not_before, not_after)
        .map_err(|_| ApiError::internal("certificate validity could not be constructed"))
}

fn coordinator_error(error: AdmissionCoordinatorError) -> ApiError {
    match error {
        AdmissionCoordinatorError::MasterCannotJoin | AdmissionCoordinatorError::Protocol(_) => {
            ApiError::bad_request(error.to_string())
        }
        AdmissionCoordinatorError::Admission(AdmissionError::Protocol(_)) => {
            ApiError::forbidden("join request authentication failed")
        }
        AdmissionCoordinatorError::Admission(AdmissionError::SourceAddressMismatch { .. })
        | AdmissionCoordinatorError::Admission(AdmissionError::SourceOutsideControlNetworks {
            ..
        })
        | AdmissionCoordinatorError::JoinKeyConflict { .. } => {
            ApiError::forbidden(error.to_string())
        }
        AdmissionCoordinatorError::Admission(_) => ApiError::bad_request(error.to_string()),
        AdmissionCoordinatorError::AdmissionConflict { .. }
        | AdmissionCoordinatorError::NodeRemovalInProgress { .. }
        | AdmissionCoordinatorError::NodeRemoved { .. } => {
            ApiError::conflict("joinConflict", error.to_string())
        }
        AdmissionCoordinatorError::InvalidTopology(_)
        | AdmissionCoordinatorError::ConcurrentAdmission { .. }
        | AdmissionCoordinatorError::Provider(_)
        | AdmissionCoordinatorError::Store(_) => {
            ApiError::service_unavailable("cluster admission is temporarily unavailable")
        }
        AdmissionCoordinatorError::Certificate(_) | AdmissionCoordinatorError::Serialization(_) => {
            ApiError::internal("cluster admission failed to produce a valid grant")
        }
    }
}
