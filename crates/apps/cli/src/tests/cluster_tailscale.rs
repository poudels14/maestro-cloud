use std::sync::Mutex;

use kernel_api::{
    RequestId, ResourceRevision, TailscaleAuthKeyRotationRequest, TailscaleAuthKeyRotationResponse,
    TailscaleAuthKeyStatus,
};

use crate::CliError;
use crate::cluster_tailscale::{TailscaleApi, rotate_auth_key};
use crate::config_source::ConfigSourceReader;

struct RecordingApi {
    revision: Option<ResourceRevision>,
    writes: Mutex<Vec<(RequestId, TailscaleAuthKeyRotationRequest)>>,
}

impl TailscaleApi for RecordingApi {
    async fn auth_key_status(&self) -> Result<TailscaleAuthKeyStatus, CliError> {
        Ok(TailscaleAuthKeyStatus {
            override_revision: self.revision,
        })
    }

    async fn rotate_auth_key(
        &self,
        request_id: &RequestId,
        request: TailscaleAuthKeyRotationRequest,
    ) -> Result<TailscaleAuthKeyRotationResponse, CliError> {
        self.writes
            .lock()
            .map_err(|_| CliError::invalid_input("recording API lock poisoned"))?
            .push((request_id.clone(), request));
        Ok(TailscaleAuthKeyRotationResponse {
            request_id: request_id.clone(),
        })
    }
}

struct SecretReader(&'static str);

impl ConfigSourceReader for SecretReader {
    async fn read(&self, _source: &str) -> Result<String, CliError> {
        Ok(self.0.to_owned())
    }
}

#[tokio::test]
async fn rotation_reads_a_secret_source_and_never_prints_the_key()
-> Result<(), Box<dyn std::error::Error>> {
    let api = RecordingApi {
        revision: Some(ResourceRevision(42)),
        writes: Mutex::new(Vec::new()),
    };
    let request_id = RequestId::new("tailscale-rotation-1")?;
    let mut output = Vec::new();
    rotate_auth_key(
        &api,
        "aws-secret://tailscale-key",
        request_id.clone(),
        &mut output,
        &SecretReader("  tskey-auth-new-reusable-secret  \n"),
    )
    .await?;

    let writes = api
        .writes
        .lock()
        .map_err(|_| "recording API lock poisoned")?;
    let (observed_id, request) = writes.first().ok_or("rotation was not submitted")?;
    assert_eq!(observed_id, &request_id);
    assert_eq!(request.expected_revision, Some(ResourceRevision(42)));
    assert_eq!(request.auth_key.expose(), "tskey-auth-new-reusable-secret");
    let output = String::from_utf8(output)?;
    assert!(output.contains("tailscale-rotation-1"));
    assert!(!output.contains("tskey-auth"));
    Ok(())
}

#[tokio::test]
async fn rotation_rejects_a_weak_key_before_contacting_the_api()
-> Result<(), Box<dyn std::error::Error>> {
    let api = RecordingApi {
        revision: None,
        writes: Mutex::new(Vec::new()),
    };
    let result = rotate_auth_key(
        &api,
        "file:///run/secrets/tailscale-key",
        RequestId::new("tailscale-rotation-weak")?,
        &mut Vec::new(),
        &SecretReader("too-short"),
    )
    .await;
    assert!(matches!(result, Err(CliError::InvalidInput { .. })));
    assert!(
        api.writes
            .lock()
            .map_err(|_| "recording API lock poisoned")?
            .is_empty()
    );
    Ok(())
}
