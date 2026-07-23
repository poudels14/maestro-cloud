use crate::ApiError;

const RESERVED_PREFIX: &str = "maestro-system-";

pub(crate) fn ensure_user_resource_id(kind: &str, resource_id: &str) -> Result<(), ApiError> {
    if resource_id.starts_with(RESERVED_PREFIX) {
        Err(ApiError::conflict(
            "systemResourceReserved",
            format!("{kind} identity `{resource_id}` is reserved for a built-in system resource"),
        ))
    } else {
        Ok(())
    }
}
