use std::path::{Path, PathBuf};

use url::Url;

/// A parsed external value source supported by Maestro configuration.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum ExternalValueSource {
    AwsSecret { secret_id: String },
    File { path: PathBuf },
}

impl ExternalValueSource {
    pub fn parse(source: &str) -> Result<Self, InvalidExternalValueSource> {
        let (scheme, suffix) = source
            .split_once(':')
            .ok_or(InvalidExternalValueSource::InvalidUrl)?;
        match scheme {
            "aws-secret" => {
                let secret_id = suffix
                    .strip_prefix("//")
                    .filter(|secret_id| {
                        !secret_id.is_empty()
                            && !secret_id.chars().any(char::is_whitespace)
                            && !secret_id.chars().any(char::is_control)
                    })
                    .ok_or(InvalidExternalValueSource::InvalidAwsSecret)?;
                Ok(Self::AwsSecret {
                    secret_id: secret_id.to_owned(),
                })
            }
            "file" => {
                let url = Url::parse(source).map_err(|_| InvalidExternalValueSource::InvalidUrl)?;
                file_url_path(&url)
                    .map(|path| Self::File { path })
                    .ok_or(InvalidExternalValueSource::InvalidFile)
            }
            _ => {
                Url::parse(source).map_err(|_| InvalidExternalValueSource::InvalidUrl)?;
                Err(InvalidExternalValueSource::UnsupportedScheme)
            }
        }
    }

    /// Parses only recognized source schemes, leaving ordinary literal values untouched.
    pub fn parse_supported(source: &str) -> Result<Option<Self>, InvalidExternalValueSource> {
        match source.split_once(':').map(|(scheme, _)| scheme) {
            Some("aws-secret" | "file") => Self::parse(source).map(Some),
            _ => Ok(None),
        }
    }

    #[must_use]
    pub fn aws_secret_id(&self) -> Option<&str> {
        match self {
            Self::AwsSecret { secret_id } => Some(secret_id),
            Self::File { .. } => None,
        }
    }

    #[must_use]
    pub fn file_path(&self) -> Option<&Path> {
        match self {
            Self::File { path } => Some(path),
            Self::AwsSecret { .. } => None,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum InvalidExternalValueSource {
    #[error("source is not a valid URL")]
    InvalidUrl,
    #[error("source uses an unsupported scheme")]
    UnsupportedScheme,
    #[error("aws-secret source must name a secret without whitespace")]
    InvalidAwsSecret,
    #[error("file source is not a supported local file URL")]
    InvalidFile,
}

fn file_url_path(url: &Url) -> Option<PathBuf> {
    if !url.username().is_empty()
        || url.password().is_some()
        || url.port().is_some()
        || url.query().is_some()
        || url.fragment().is_some()
    {
        return None;
    }
    match url.host_str() {
        None | Some("") | Some("localhost") => url.to_file_path().ok(),
        Some(host) if url.path().is_empty() || url.path() == "/" => Some(PathBuf::from(host)),
        Some(host) => {
            let path_url = Url::parse(&format!("file://{}", url.path())).ok()?;
            let suffix = path_url.to_file_path().ok()?;
            let suffix = suffix.strip_prefix(Path::new("/")).ok()?;
            Some(PathBuf::from(host).join(suffix))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::path::Path;

    use super::ExternalValueSource;

    #[test]
    fn parses_supported_external_sources() -> Result<(), Box<dyn std::error::Error>> {
        let aws = ExternalValueSource::parse("aws-secret://maestro/service")?;
        assert_eq!(aws.aws_secret_id(), Some("maestro/service"));
        let arn = ExternalValueSource::parse(
            "aws-secret://arn:aws:secretsmanager:us-west-2:123456789012:secret:maestro",
        )?;
        assert_eq!(
            arn.aws_secret_id(),
            Some("arn:aws:secretsmanager:us-west-2:123456789012:secret:maestro")
        );

        let absolute = ExternalValueSource::parse("file:///etc/maestro/config.json");
        assert!(absolute.is_ok(), "absolute file URL failed: {absolute:?}");
        let absolute = absolute?;
        assert_eq!(
            absolute.file_path(),
            Some(Path::new("/etc/maestro/config.json"))
        );

        let relative = ExternalValueSource::parse("file://config/service.json");
        assert!(relative.is_ok(), "relative file URL failed: {relative:?}");
        let relative = relative?;
        assert_eq!(relative.file_path(), Some(Path::new("config/service.json")));
        Ok(())
    }

    #[test]
    fn supported_parser_preserves_literals() -> Result<(), Box<dyn std::error::Error>> {
        assert_eq!(ExternalValueSource::parse_supported("literal-token")?, None);
        assert!(ExternalValueSource::parse_supported("aws-secret://").is_err());
        Ok(())
    }
}
