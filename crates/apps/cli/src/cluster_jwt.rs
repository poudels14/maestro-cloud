use std::io::Write;
use std::path::Path;

use crate::CliError;
use crate::config::load_cluster;
use crate::config_source::ConfigSourceReader;
use crate::launch_document::DaemonLaunchDocument;
use crate::private_document::{read_private, replace_private};

pub(crate) async fn rotate_jwt_key(
    config_source: &str,
    launch_path: &Path,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let loaded = load_cluster(config_source, reader).await?;
    let encoded = read_private(launch_path, "daemon launch document")?;
    let mut launch = serde_json::from_slice::<DaemonLaunchDocument>(&encoded)
        .map_err(|source| CliError::json("failed to decode daemon launch document", source))?;
    launch.validate()?;
    let changed =
        launch.replace_jwt_secret_key(&loaded.cluster, &loaded.node_id, loaded.jwt_secret_key)?;
    if changed {
        replace_private(launch_path, &launch, "daemon launch document")?;
    }
    let action = if changed { "updated" } else { "verified" };
    writeln!(
        output,
        "[maestro]: {action} JWT secret key for node `{}`",
        launch.node_id()
    )
    .map_err(|source| CliError::io("failed to write command output", source))
}
