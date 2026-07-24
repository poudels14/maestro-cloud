/// Preview settings, resource, serialization, and store failures.
#[derive(Debug, thiserror::Error)]
pub enum PreviewError {
    /// A static or derived resource identifier was invalid.
    #[error(transparent)]
    InvalidIdentifier(#[from] kernel_api::InvalidIdentifier),
    /// Static preview configuration was invalid.
    #[error("invalid preview settings: {message}")]
    InvalidSettings { message: String },
    /// A base or preview resource could not produce safe derived state.
    #[error("invalid preview definition: {message}")]
    InvalidDefinition { message: String },
    /// A relevant stored resource could not be decoded.
    #[error("malformed {kind} resource at `{key}`: {message}")]
    MalformedResource {
        kind: &'static str,
        key: String,
        message: String,
    },
    /// One typed identity occurred more than once in a snapshot.
    #[error("{kind} `{resource_id}` occurs more than once in one snapshot")]
    DuplicateResource {
        kind: &'static str,
        resource_id: String,
    },
    /// A desired resource could not be serialized for a fenced transaction.
    #[error("failed to serialize preview resource: {message}")]
    Serialize { message: String },
    /// The leadership fence or backing store rejected an operation.
    #[error(transparent)]
    Controller(#[from] kernel_controller::ControllerError),
}
