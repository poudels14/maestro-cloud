use axum::Json;
use axum::Router;
use axum::extract::{Path, State};
use axum::routing::get;
use kernel_api::{ArtifactTemplate, BuiltinKind, SecretValue, Service, ServiceId};

use crate::{ApiError, AppState, resource};

pub(super) fn router() -> Router<AppState> {
    Router::new()
        .route("/api/services", get(list_services))
        .route("/api/services/{service_id}", get(get_service))
}

async fn list_services(State(state): State<AppState>) -> Result<Json<Vec<Service>>, ApiError> {
    let services = resource::list(&state, BuiltinKind::Service)
        .await?
        .into_iter()
        .map(mask_service)
        .collect();
    Ok(Json(services))
}

async fn get_service(
    State(state): State<AppState>,
    Path(service_id): Path<String>,
) -> Result<Json<Service>, ApiError> {
    let service_id =
        ServiceId::new(service_id).map_err(|error| ApiError::bad_request(error.to_string()))?;
    Ok(Json(mask_service(
        resource::get(&state, BuiltinKind::Service, service_id).await?,
    )))
}

fn mask_service(mut service: Service) -> Service {
    if let ArtifactTemplate::Build { template } = &mut service.spec.artifact {
        mask_values(template.secrets.values_mut());
    }
    if let Some(secrets) = &mut service.spec.secrets {
        mask_values(secrets.items.values_mut());
    }
    service
}

fn mask_values<'a>(values: impl Iterator<Item = &'a mut SecretValue>) {
    for value in values {
        *value = SecretValue::new(value.masked().as_str());
    }
}
