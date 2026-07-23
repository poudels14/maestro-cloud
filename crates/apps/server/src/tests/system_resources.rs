use crate::system_resources::ensure_user_resource_id;

#[test]
fn reserves_only_the_built_in_system_namespace() -> Result<(), crate::ApiError> {
    assert!(ensure_user_resource_id("Service", "maestro-system-tailscale-gateway").is_err());
    ensure_user_resource_id("Service", "maestro-api")?;
    Ok(())
}
