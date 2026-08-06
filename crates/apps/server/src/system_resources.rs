use crate::ApiError;

pub(crate) fn ensure_user_resource_id(kind: &str, resource_id: &str) -> Result<(), ApiError> {
    if kernel_api::is_system_resource_id(resource_id) {
        Err(ApiError::conflict(
            "systemResourceReserved",
            format!("{kind} identity `{resource_id}` is reserved for a built-in system resource"),
        ))
    } else {
        Ok(())
    }
}
