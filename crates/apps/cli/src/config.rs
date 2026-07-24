use std::io::Write;
use std::path::{Path, PathBuf};

use clap::ValueEnum;

use crate::CliError;
use crate::cluster_config::decode_cluster;
use crate::config_source::{ConfigSourceReader, load_merged};
use crate::service_config::decode_services;

const CLUSTER_CONFIG_PATH: &str = "maestro.jsonc";
const SERVICES_CONFIG_PATH: &str = "maestro.services.jsonc";

#[derive(Debug, Clone, Copy, PartialEq, Eq, ValueEnum)]
pub(crate) enum ConfigKind {
    Cluster,
    Services,
}

impl ConfigKind {
    pub(crate) fn parse(value: &str) -> Result<Self, CliError> {
        match value.trim().to_ascii_lowercase().as_str() {
            "cluster" => Ok(Self::Cluster),
            "services" => Ok(Self::Services),
            _ => Err(CliError::invalid_input(
                "config kind must be `cluster` or `services`",
            )),
        }
    }

    fn default_path(self) -> PathBuf {
        match self {
            Self::Cluster => PathBuf::from(CLUSTER_CONFIG_PATH),
            Self::Services => PathBuf::from(SERVICES_CONFIG_PATH),
        }
    }

    fn template(self) -> String {
        match self {
            Self::Cluster => cluster_template(),
            Self::Services => SERVICES_TEMPLATE.to_string(),
        }
    }
}

pub(crate) fn init(
    kind: ConfigKind,
    destination: Option<&Path>,
    output: &mut dyn Write,
) -> Result<PathBuf, CliError> {
    let path = destination
        .map(Path::to_path_buf)
        .unwrap_or_else(|| kind.default_path());
    let parent = path
        .parent()
        .filter(|parent| !parent.as_os_str().is_empty())
        .unwrap_or_else(|| Path::new("."));
    let mut temporary = tempfile::NamedTempFile::new_in(parent).map_err(|source| {
        CliError::io(
            format!(
                "failed to create temporary config in `{}`",
                parent.display()
            ),
            source,
        )
    })?;
    temporary
        .write_all(kind.template().as_bytes())
        .map_err(|source| {
            CliError::io(
                format!("failed to write config `{}`", path.display()),
                source,
            )
        })?;
    temporary.as_file().sync_all().map_err(|source| {
        CliError::io(
            format!("failed to sync config `{}`", path.display()),
            source,
        )
    })?;
    temporary.persist_noclobber(&path).map_err(|error| {
        let source = error.error;
        let action = if source.kind() == std::io::ErrorKind::AlreadyExists {
            format!("refusing to overwrite existing config `{}`", path.display())
        } else {
            format!("failed to install config `{}`", path.display())
        };
        CliError::io(action, source)
    })?;
    writeln!(output, "[maestro]: created {}", path.display())
        .map_err(|source| CliError::io("failed to write command output", source))?;
    Ok(path)
}

pub(crate) async fn validate(
    source: &str,
    output: &mut dyn Write,
    reader: &impl ConfigSourceReader,
) -> Result<(), CliError> {
    let merged = load_merged(source, reader).await?;
    let object = merged.as_object().ok_or_else(|| {
        CliError::invalid_input(format!(
            "config `{source}` must contain a JSON object at the top level"
        ))
    })?;
    let has_cluster = object.contains_key("cluster");
    let has_services = object.contains_key("services");
    match (has_cluster, has_services) {
        (true, false) => {
            let loaded = decode_cluster(source, merged, reader).await?;
            writeln!(
                output,
                "[maestro]: {source} is a valid cluster config for `{}` on node `{}`",
                loaded.cluster.name, loaded.node_id
            )
            .map_err(output_error)?;
            write_ignored(&loaded.ignored_fields, output)
        }
        (false, true) => {
            let loaded = decode_services(source, merged, reader).await?;
            writeln!(
                output,
                "[maestro]: {source} is a valid services config ({} services)",
                loaded.services.len()
            )
            .map_err(output_error)?;
            write_ignored(&loaded.ignored_fields, output)
        }
        (true, true) => Err(CliError::invalid_input(format!(
            "config `{source}` is ambiguous: both `cluster` and `services` are present"
        ))),
        (false, false) => Err(CliError::invalid_input(format!(
            "unrecognized config `{source}`: expected a top-level `cluster` or `services` field"
        ))),
    }
}

pub(crate) async fn load_cluster(
    source: &str,
    reader: &impl ConfigSourceReader,
) -> Result<crate::cluster_config::LoadedClusterConfig, CliError> {
    decode_cluster(source, load_merged(source, reader).await?, reader).await
}

fn write_ignored(fields: &[String], output: &mut dyn Write) -> Result<(), CliError> {
    if fields.is_empty() {
        writeln!(output, "[maestro]: ignored fields: none").map_err(output_error)?;
    } else {
        writeln!(output, "[maestro]: warning: ignored fields:").map_err(output_error)?;
        for field in fields {
            writeln!(output, "  - {field}").map_err(output_error)?;
        }
    }
    Ok(())
}

fn cluster_template() -> String {
    let join_secret = format!(
        "{}{}",
        uuid::Uuid::new_v4().simple(),
        uuid::Uuid::new_v4().simple()
    );
    CLUSTER_TEMPLATE.replace("__JOIN_SECRET__", &join_secret)
}

fn output_error(source: std::io::Error) -> CliError {
    CliError::io("failed to write command output", source)
}

const CLUSTER_TEMPLATE: &str = r#"{
  // "$extends": "file://shared-cluster.jsonc",
  "cluster": {
    "name": "my-cluster",
    "cluster-cidr": "10.42.0.0/16",
    "nodes": {
      "node-1": {
        "hostname": "node-1.internal",
        "endpoint": "10.20.0.11",
        "subnet": "10.42.1.0/24",
        "role": "master"
      }
    },
    "control-allow-cidrs": ["10.20.0.0/24"],
    "join-secret": "__JOIN_SECRET__"
  },
  "node": "node-1"
}
"#;

const SERVICES_TEMPLATE: &str = r#"{
  // "$extends": "file://shared-services.jsonc",
  "services": {
    "service-1": {
      "name": "Service 1",
      "image": "traefik/whoami:latest",
      "deploy": {
        "exposePorts": [80],
        "replicas": 1,
        "exec": false
      }
    }
  }
}
"#;
