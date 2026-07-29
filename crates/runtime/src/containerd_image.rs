use containerd::services::v1::{GetImageRequest, ReadContentRequest};
use containerd::tonic::{Code, Status, transport::Channel};
use containerd::types::Descriptor;
use kernel_api::{CommandSpec, WorkloadId};
use oci_spec::image::{
    Config as OciConfig, Descriptor as OciDescriptor, ImageConfiguration, ImageIndex,
    ImageManifest, Os,
};
use serde::Deserialize;
use sha2::{Digest, Sha256};

use crate::RuntimeError;
use crate::containerd_support::namespaced;

const MAX_METADATA_BYTES: usize = 8 * 1024 * 1024;
const OCI_INDEX: &str = "application/vnd.oci.image.index.v1+json";
const DOCKER_INDEX: &str = "application/vnd.docker.distribution.manifest.list.v2+json";

pub(crate) struct ContainerdImage {
    pub(crate) configuration: ImageDefaults,
    pub(crate) snapshot_parent: String,
}

#[derive(Debug, Clone, Default)]
pub(crate) struct ImageDefaults {
    pub(crate) environment: Vec<String>,
    pub(crate) entrypoint: Vec<String>,
    pub(crate) command: Vec<String>,
    pub(crate) working_directory: Option<String>,
    pub(crate) user: String,
}

impl ImageDefaults {
    pub(crate) fn from_oci(configuration: &ImageConfiguration) -> Self {
        let config = configuration.config().as_ref();
        Self {
            environment: optional_values(config.map(OciConfig::env)),
            entrypoint: optional_values(config.map(OciConfig::entrypoint)),
            command: optional_values(config.map(OciConfig::cmd)),
            working_directory: config
                .and_then(|config| config.working_dir().as_ref())
                .filter(|directory| !directory.is_empty())
                .cloned(),
            user: config
                .and_then(|config| config.user().as_ref())
                .cloned()
                .unwrap_or_default(),
        }
    }

    pub(crate) fn command(&self) -> Result<CommandSpec, RuntimeError> {
        let mut arguments = self.entrypoint.clone();
        arguments.extend(self.command.clone());
        let mut arguments = arguments.into_iter();
        let executable = arguments.next().ok_or_else(|| RuntimeError::InvalidSpec {
            message: "containerd image has no default command".to_owned(),
        })?;
        Ok(CommandSpec {
            executable,
            arguments: arguments.collect(),
        })
    }
}

fn optional_values(values: Option<&Option<Vec<String>>>) -> Vec<String> {
    values.and_then(Option::as_ref).cloned().unwrap_or_default()
}

pub(crate) async fn load_image(
    channel: Channel,
    namespace: &str,
    reference: &str,
    workload_id: &WorkloadId,
) -> Result<ContainerdImage, RuntimeError> {
    let image = containerd::services::v1::images_client::ImagesClient::new(channel.clone())
        .get(namespaced(
            GetImageRequest {
                name: reference.to_owned(),
            },
            namespace,
        )?)
        .await
        .map_err(|error| image_error(error, reference, workload_id))?
        .into_inner()
        .image
        .ok_or_else(|| RuntimeError::Unavailable {
            message: format!("containerd omitted image metadata for `{reference}`"),
        })?;
    let target = image.target.ok_or_else(|| RuntimeError::Rejected {
        message: format!("containerd image `{reference}` has no target descriptor"),
    })?;
    let manifest = resolve_manifest(channel.clone(), namespace, target).await?;
    let config_descriptor = containerd_descriptor(manifest.config())?;
    let configuration =
        read_json::<ImageConfiguration>(channel, namespace, &config_descriptor).await?;
    Ok(ContainerdImage {
        snapshot_parent: chain_id(configuration.rootfs().diff_ids())?,
        configuration: ImageDefaults::from_oci(&configuration),
    })
}

async fn resolve_manifest(
    channel: Channel,
    namespace: &str,
    target: Descriptor,
) -> Result<ImageManifest, RuntimeError> {
    let descriptor = if target.media_type == OCI_INDEX || target.media_type == DOCKER_INDEX {
        let index = read_json::<ImageIndex>(channel.clone(), namespace, &target).await?;
        let descriptor = index
            .manifests()
            .iter()
            .find(|descriptor| matches_host(descriptor))
            .ok_or_else(|| RuntimeError::Rejected {
                message: "containerd image index has no manifest for this Linux architecture"
                    .to_owned(),
            })?;
        containerd_descriptor(descriptor)?
    } else {
        target
    };
    read_json(channel, namespace, &descriptor).await
}

async fn read_json<Value>(
    channel: Channel,
    namespace: &str,
    descriptor: &Descriptor,
) -> Result<Value, RuntimeError>
where
    Value: for<'de> Deserialize<'de>,
{
    if descriptor.size < 0
        || usize::try_from(descriptor.size)
            .ok()
            .is_none_or(|size| size > MAX_METADATA_BYTES)
    {
        return Err(RuntimeError::Rejected {
            message: format!(
                "containerd metadata blob `{}` exceeds the bounded decode size",
                descriptor.digest
            ),
        });
    }
    let response = containerd::services::v1::content_client::ContentClient::new(channel)
        .read(namespaced(
            ReadContentRequest {
                digest: descriptor.digest.clone(),
                offset: 0,
                size: descriptor.size,
            },
            namespace,
        )?)
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!(
                "failed to read containerd content `{}`: {error}",
                descriptor.digest
            ),
        })?;
    let mut stream = response.into_inner();
    let mut bytes = Vec::with_capacity(usize::try_from(descriptor.size).unwrap_or(0));
    while let Some(chunk) = stream
        .message()
        .await
        .map_err(|error| RuntimeError::Unavailable {
            message: format!(
                "containerd content stream `{}` failed: {error}",
                descriptor.digest
            ),
        })?
    {
        let next_size = bytes.len().saturating_add(chunk.data.len());
        if next_size > MAX_METADATA_BYTES {
            return Err(RuntimeError::Rejected {
                message: format!(
                    "containerd metadata blob `{}` exceeded the bounded decode size",
                    descriptor.digest
                ),
            });
        }
        bytes.extend(chunk.data);
    }
    serde_json::from_slice(&bytes).map_err(|error| RuntimeError::Rejected {
        message: format!(
            "containerd content `{}` is invalid OCI JSON: {error}",
            descriptor.digest
        ),
    })
}

pub(crate) fn chain_id(diff_ids: &[String]) -> Result<String, RuntimeError> {
    let mut diff_ids = diff_ids.iter();
    let mut chain = diff_ids
        .next()
        .filter(|digest| digest.starts_with("sha256:"))
        .cloned()
        .ok_or_else(|| RuntimeError::Rejected {
            message: "containerd image rootfs has no valid diff IDs".to_owned(),
        })?;
    for diff_id in diff_ids {
        if !diff_id.starts_with("sha256:") {
            return Err(RuntimeError::Rejected {
                message: format!("containerd image has invalid diff ID `{diff_id}`"),
            });
        }
        chain = format!(
            "sha256:{}",
            hex::encode(Sha256::digest(format!("{chain} {diff_id}")))
        );
    }
    Ok(chain)
}

fn image_error(error: Status, reference: &str, workload_id: &WorkloadId) -> RuntimeError {
    if error.code() == Code::NotFound {
        RuntimeError::Rejected {
            message: format!(
                "containerd image `{reference}` is not present; pull it through ArtifactStore"
            ),
        }
    } else {
        crate::containerd_support::runtime_status(error, workload_id)
    }
}

fn matches_host(descriptor: &OciDescriptor) -> bool {
    descriptor.platform().as_ref().is_some_and(|platform| {
        platform.os() == &Os::Linux && platform.architecture().to_string() == host_architecture()
    })
}

fn containerd_descriptor(descriptor: &OciDescriptor) -> Result<Descriptor, RuntimeError> {
    Ok(Descriptor {
        media_type: descriptor.media_type().to_string(),
        digest: descriptor.digest().to_string(),
        size: i64::try_from(descriptor.size()).map_err(|_| RuntimeError::Rejected {
            message: format!(
                "containerd metadata blob `{}` exceeds the supported descriptor size",
                descriptor.digest()
            ),
        })?,
        annotations: descriptor.annotations().clone().unwrap_or_default(),
    })
}

fn host_architecture() -> &'static str {
    match std::env::consts::ARCH {
        "x86_64" => "amd64",
        "aarch64" => "arm64",
        architecture => architecture,
    }
}
