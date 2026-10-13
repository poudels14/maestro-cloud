use std::io::ErrorKind;
use std::path::{Path, PathBuf};

use base64::Engine;
use base64::engine::general_purpose::STANDARD;
use tokio::io::AsyncWriteExt;
use x25519_dalek::{PublicKey, StaticSecret};

use crate::error::AgentError;

const PRIVATE_KEY_FILE: &str = "wireguard.key";
const KEY_LENGTH: usize = 32;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeKey {
    pub private_key_path: PathBuf,
    pub public_key: String,
}

pub async fn load_or_generate_key(directory: &Path) -> Result<NodeKey, AgentError> {
    let private_key_path = directory.join(PRIVATE_KEY_FILE);
    let private_key = match tokio::fs::read_to_string(&private_key_path).await {
        Ok(contents) => contents.trim().to_string(),
        Err(error) if error.kind() == ErrorKind::NotFound => {
            let generated = STANDARD.encode(StaticSecret::random().to_bytes());
            write_private_key(directory, &private_key_path, &generated).await?;
            generated
        }
        Err(error) => Err(key_error(&private_key_path, error))?,
    };
    let public_key = public_key(&private_key)?;
    Ok(NodeKey {
        private_key_path,
        public_key,
    })
}

pub fn public_key(private_key: &str) -> Result<String, AgentError> {
    let bytes: [u8; KEY_LENGTH] = STANDARD
        .decode(private_key.trim())
        .ok()
        .and_then(|decoded| decoded.try_into().ok())
        .ok_or_else(|| AgentError::Host("invalid WireGuard private key".to_string()))?;
    let secret = StaticSecret::from(bytes);
    Ok(STANDARD.encode(PublicKey::from(&secret).as_bytes()))
}

async fn write_private_key(
    directory: &Path,
    path: &Path,
    private_key: &str,
) -> Result<(), AgentError> {
    tokio::fs::create_dir_all(directory)
        .await
        .map_err(|error| key_error(directory, error))?;
    let mut file = tokio::fs::OpenOptions::new()
        .write(true)
        .create_new(true)
        .mode(0o600)
        .open(path)
        .await
        .map_err(|error| key_error(path, error))?;
    file.write_all(format!("{private_key}\n").as_bytes())
        .await
        .map_err(|error| key_error(path, error))?;
    file.sync_all()
        .await
        .map_err(|error| key_error(path, error))
}

fn key_error(path: &Path, error: std::io::Error) -> AgentError {
    AgentError::Host(format!("WireGuard key file {}: {error}", path.display()))
}

#[cfg(test)]
mod tests {
    use std::os::unix::fs::PermissionsExt;

    use super::*;

    #[test]
    fn derives_public_key_from_rfc7748_vector() {
        let private_key = STANDARD.encode(
            hex::decode("77076d0a7318a57d3c16c17251b26645df4c2f87ebc0992ab177fba51db92c2a")
                .unwrap(),
        );
        let expected = STANDARD.encode(
            hex::decode("8520f0098930a754748b7ddcb43ef75a0dbf3a0d26381af4eba4a98eaa9b4e6a")
                .unwrap(),
        );
        assert_eq!(public_key(&private_key).unwrap(), expected);
    }

    #[test]
    fn rejects_malformed_private_key() {
        assert!(public_key("not-base64!").is_err());
        assert!(public_key(&STANDARD.encode([1u8; 16])).is_err());
    }

    #[tokio::test]
    async fn generates_key_once_with_private_permissions() {
        let directory = tempfile::tempdir().unwrap();
        let key_directory = directory.path().join("keys");
        let generated = load_or_generate_key(&key_directory).await.unwrap();
        let mode = std::fs::metadata(&generated.private_key_path)
            .unwrap()
            .permissions()
            .mode();
        assert_eq!(mode & 0o777, 0o600);
        let stored = std::fs::read_to_string(&generated.private_key_path).unwrap();
        assert_eq!(public_key(&stored).unwrap(), generated.public_key);
        let reloaded = load_or_generate_key(&key_directory).await.unwrap();
        assert_eq!(reloaded, generated);
    }
}
