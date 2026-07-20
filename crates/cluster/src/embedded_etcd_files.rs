use std::io::Write;
use std::path::Path;

use crate::{StoreProviderConfig, StoreProviderError, embedded_etcd_plan::EtcdSecurityPaths};

pub(crate) fn materialize_security(
    config: &StoreProviderConfig,
) -> Result<EtcdSecurityPaths, StoreProviderError> {
    let directory = config.data_directory().join("tls");
    create_private_directory(&directory, "store TLS")?;
    let paths = EtcdSecurityPaths {
        certificate_authority: directory.join("ca.pem"),
        certificate: directory.join("identity.pem"),
        private_key: directory.join("identity-key.pem"),
    };
    write_or_validate(
        &paths.certificate_authority,
        config.security().trust_root_pem.as_bytes(),
        FileSensitivity::Public,
    )?;
    write_or_validate(
        &paths.certificate,
        config.security().identity.certificate_pem.as_bytes(),
        FileSensitivity::Public,
    )?;
    write_or_validate(
        &paths.private_key,
        config
            .security()
            .identity
            .private_key_pem
            .expose()
            .as_bytes(),
        FileSensitivity::Private,
    )?;
    Ok(paths)
}

pub(crate) fn create_private_directory(
    path: &Path,
    purpose: &str,
) -> Result<(), StoreProviderError> {
    std::fs::create_dir_all(path).map_err(|error| StoreProviderError::Lifecycle {
        reason: format!(
            "failed to create {purpose} directory `{}`: {error}",
            path.display()
        ),
    })?;
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700)).map_err(
            |error| StoreProviderError::Lifecycle {
                reason: format!(
                    "failed to restrict {purpose} directory `{}`: {error}",
                    path.display()
                ),
            },
        )?;
    }
    Ok(())
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum FileSensitivity {
    Public,
    Private,
}

fn write_or_validate(
    path: &Path,
    expected: &[u8],
    sensitivity: FileSensitivity,
) -> Result<(), StoreProviderError> {
    match std::fs::read(path) {
        Ok(existing) if existing == expected => validate_permissions(path, sensitivity),
        Ok(_) => Err(StoreProviderError::MembershipConflict {
            reason: format!(
                "persisted TLS file `{}` differs from node identity",
                path.display()
            ),
        }),
        Err(error) if error.kind() == std::io::ErrorKind::NotFound => {
            let mut options = std::fs::OpenOptions::new();
            options.create_new(true).write(true);
            #[cfg(unix)]
            if sensitivity == FileSensitivity::Private {
                use std::os::unix::fs::OpenOptionsExt;
                options.mode(0o600);
            }
            let mut file = options
                .open(path)
                .map_err(|error| StoreProviderError::Lifecycle {
                    reason: format!("failed to create TLS file `{}`: {error}", path.display()),
                })?;
            file.write_all(expected)
                .and_then(|()| file.sync_all())
                .map_err(|error| StoreProviderError::Lifecycle {
                    reason: format!("failed to persist TLS file `{}`: {error}", path.display()),
                })?;
            validate_permissions(path, sensitivity)
        }
        Err(error) => Err(StoreProviderError::Lifecycle {
            reason: format!("failed to read TLS file `{}`: {error}", path.display()),
        }),
    }
}

fn validate_permissions(
    path: &Path,
    sensitivity: FileSensitivity,
) -> Result<(), StoreProviderError> {
    #[cfg(unix)]
    if sensitivity == FileSensitivity::Private {
        use std::os::unix::fs::PermissionsExt;
        let mode = std::fs::metadata(path)
            .map_err(|error| StoreProviderError::Lifecycle {
                reason: format!("failed to inspect TLS key `{}`: {error}", path.display()),
            })?
            .permissions()
            .mode()
            & 0o777;
        if mode & 0o077 != 0 {
            return Err(StoreProviderError::MembershipConflict {
                reason: format!(
                    "TLS key `{}` has insecure permissions {mode:#o}",
                    path.display()
                ),
            });
        }
    }
    Ok(())
}
