use crate::system_resources::ensure_user_resource_id;
use kernel_api::TAILSCALE_GATEWAY_SERVICE_ID;

#[test]
fn reserves_only_the_built_in_system_namespace() -> Result<(), crate::ApiError> {
    assert!(ensure_user_resource_id("Service", TAILSCALE_GATEWAY_SERVICE_ID).is_err());
    ensure_user_resource_id("Service", "maestro-api")?;
    Ok(())
}
