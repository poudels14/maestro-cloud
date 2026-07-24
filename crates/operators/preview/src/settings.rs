use crate::error::PreviewError;

/// Static derivation settings shared by all previews in one cluster.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PreviewSettings {
    pub(super) preview_domain: String,
}

impl PreviewSettings {
    /// Validates a preview DNS suffix.
    pub fn new(preview_domain: impl Into<String>) -> Result<Self, PreviewError> {
        let preview_domain = preview_domain.into();
        let normalized = preview_domain.trim().trim_end_matches('.');
        if normalized.is_empty()
            || normalized.split('.').any(|label| {
                label.is_empty()
                    || label.starts_with('-')
                    || label.ends_with('-')
                    || !label
                        .chars()
                        .all(|character| character.is_ascii_alphanumeric() || character == '-')
            })
        {
            return Err(PreviewError::InvalidSettings {
                message: format!("preview domain `{preview_domain}` is not a DNS suffix"),
            });
        }
        Ok(Self {
            preview_domain: normalized.to_ascii_lowercase(),
        })
    }
}
