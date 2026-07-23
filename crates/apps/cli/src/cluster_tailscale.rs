use std::io::Write;

use cluster::TailscaleAuthKeyRecord;
use kernel_api::{
    RequestId, SecretValue, TailscaleAuthKeyRotationRequest, TailscaleAuthKeyRotationResponse,
    TailscaleAuthKeyStatus,
};

use crate::CliError;
use crate::api_client::ApiClient;
use crate::config_source::ConfigSourceReader;

pub(crate) async fn rotate_auth_key(
    client: &impl TailscaleApi,
    source: &str,
    request_id: RequestId,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let auth_key = reader.read(source).await?;
    let auth_key = SecretValue::new(auth_key.trim());
    TailscaleAuthKeyRecord::new(auth_key.clone())
        .map_err(|error| CliError::invalid_input(error.to_string()))?;
    let status = client.auth_key_status().await?;
    let response = client
        .rotate_auth_key(
            &request_id,
            TailscaleAuthKeyRotationRequest {
                expected_revision: status.override_revision,
                auth_key,
            },
        )
        .await?;
    if response.request_id != request_id {
        return Err(CliError::invalid_api_response(
            "Tailscale auth-key rotation receipt does not match the submitted request",
        ));
    }
    writeln!(
        output,
        "[maestro]: Tailscale auth-key rotation `{}` accepted",
        response.request_id
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}

pub(crate) trait TailscaleApi {
    async fn auth_key_status(&self) -> Result<TailscaleAuthKeyStatus, CliError>;

    async fn rotate_auth_key(
        &self,
        request_id: &RequestId,
        request: TailscaleAuthKeyRotationRequest,
    ) -> Result<TailscaleAuthKeyRotationResponse, CliError>;
}

impl TailscaleApi for ApiClient {
    async fn auth_key_status(&self) -> Result<TailscaleAuthKeyStatus, CliError> {
        self.get("/api/cluster/tailscale/auth-key").await
    }

    async fn rotate_auth_key(
        &self,
        request_id: &RequestId,
        request: TailscaleAuthKeyRotationRequest,
    ) -> Result<TailscaleAuthKeyRotationResponse, CliError> {
        self.put("/api/cluster/tailscale/auth-key", request_id, &request)
            .await
    }
}
