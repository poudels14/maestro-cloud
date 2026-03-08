pub mod file;

use async_trait::async_trait;

use crate::supervisor::model::NamedServiceConfig;

#[async_trait]
pub trait ServiceConfigSource {
    async fn next_snapshot(&mut self) -> Result<Vec<NamedServiceConfig>, String>;
}
