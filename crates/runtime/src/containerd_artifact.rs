use std::collections::HashMap;

use async_trait::async_trait;
use containerd::services::v1::leases_client::LeasesClient;
use containerd::services::v1::transfer_client::TransferClient;
use containerd::services::v1::{
    CreateImageRequest, CreateRequest as CreateLeaseRequest, DeleteImageRequest,
    DeleteRequest as DeleteLeaseRequest, GetImageRequest, Image, ListImagesRequest,
    TransferOptions, TransferRequest, UpdateImageRequest,
};
use containerd::tonic::Code;
use containerd::tonic::transport::Channel;
use containerd::types::transfer::{
    ImageExportStream, ImageImportStream, ImageStore, OciRegistry, RegistryResolver,
    UnpackConfiguration,
};
use kernel_api::Timestamp;
use prost_types::Any;
use time::OffsetDateTime;
use time::format_description::well_known::Rfc3339;
use uuid::Uuid;

use crate::containerd::ContainerdRuntime;
use crate::containerd_artifact_stream::{
    ContainerdArtifactStream, TransferTask, open_stream, serve_registry_auth, upload_stream,
};
use crate::containerd_artifact_support::{
    MANAGED_ARTIFACT_LABEL, MANAGED_ARTIFACT_VALUE, artifact_request, host_platform, image_digest,
    is_not_found, namespaced_artifact, operation_error, prune_candidates, registry_reference,
    removed_digests, select_image,
};
use crate::containerd_build::run_build;
use crate::containerd_image::resolve_host_manifest_descriptor;
use crate::{
    ArtifactBuildOutputSink, ArtifactBuildRequest, ArtifactByteStream, ArtifactDigest,
    ArtifactPrunePolicy, ArtifactPruneReport, ArtifactReference, ArtifactStore, ArtifactStoreError,
    DiscardArtifactBuildOutput, RegistryCredential, RegistryCredentialProvider, RuntimeError,
};

const LEASE_EXPIRATION_LABEL: &str = "containerd.io/gc.expire";
const TRANSFER_LEASE_MILLIS: i64 = 60 * 60 * 1_000;

#[async_trait]
impl ArtifactStore for ContainerdRuntime {
    async fn build(
        &self,
        request: &ArtifactBuildRequest,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, &DiscardArtifactBuildOutput)
            .await
    }

    async fn build_with_output(
        &self,
        request: &ArtifactBuildRequest,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        self.build_artifact(request, output).await
    }

    async fn pull(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let platform = host_platform();
        let registry_reference = registry_reference(reference.as_str())?;
        let auth = registry_auth(reference, self.registry_credentials.as_ref()).await?;
        let source = containerd::to_any(&OciRegistry {
            reference: registry_reference,
            resolver: auth.as_ref().map(RegistryAuth::resolver),
        });
        let destination = containerd::to_any(&ImageStore {
            name: reference.as_str().to_owned(),
            labels: managed_labels(),
            platforms: vec![platform.clone()],
            unpacks: vec![UnpackConfiguration {
                platform: Some(platform),
                snapshotter: self.settings.snapshotter.clone(),
            }],
            ..Default::default()
        });
        registry_transfer(
            self.channel.clone(),
            self.settings.namespace.clone(),
            RegistryTransfer {
                source,
                destination,
                operation: "pull",
                reference: Some(reference.as_str().to_owned()),
                lease_id: None,
                auth,
            },
        )
        .await?;
        let image = self
            .host_platform_image(self.image(reference.as_str()).await?, reference.as_str())
            .await?;
        self.ensure_digest_alias(&image, reference.as_str()).await
    }

    async fn push(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        let image = select_image(&self.images().await?, digest)?;
        let registry_reference = registry_reference(destination.as_str())?;
        let auth = registry_auth(destination, self.registry_credentials.as_ref()).await?;
        registry_transfer(
            self.channel.clone(),
            self.settings.namespace.clone(),
            RegistryTransfer {
                source: containerd::to_any(&ImageStore {
                    name: image.name,
                    ..Default::default()
                }),
                destination: containerd::to_any(&OciRegistry {
                    reference: registry_reference,
                    resolver: auth.as_ref().map(RegistryAuth::resolver),
                }),
                operation: "push",
                reference: Some(destination.as_str().to_owned()),
                lease_id: None,
                auth,
            },
        )
        .await
    }

    async fn resolve_digest(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        match self.image(reference.as_str()).await {
            Ok(image) => {
                let image = self.host_platform_image(image, reference.as_str()).await?;
                self.ensure_digest_alias(&image, reference.as_str()).await
            }
            Err(ArtifactStoreError::NotFound { .. }) => self.pull(reference).await,
            Err(error) => Err(error),
        }
    }

    async fn ensure_local(
        &self,
        reference: &ArtifactReference,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        match self.image(reference.as_str()).await {
            Ok(image) => {
                let image = self.host_platform_image(image, reference.as_str()).await?;
                self.ensure_digest_alias(&image, reference.as_str()).await
            }
            Err(ArtifactStoreError::NotFound { .. }) => {
                let digest = ArtifactDigest::new(reference.as_str().to_owned())?;
                match select_image(&self.images().await?, &digest) {
                    Ok(image) => {
                        let image = self.host_platform_image(image, reference.as_str()).await?;
                        self.ensure_digest_alias(&image, reference.as_str()).await
                    }
                    Err(ArtifactStoreError::NotFound { .. }) => self.pull(reference).await,
                    Err(error) => Err(error),
                }
            }
            Err(error) => Err(error),
        }
    }

    async fn contains(&self, digest: &ArtifactDigest) -> Result<bool, ArtifactStoreError> {
        match select_image(&self.images().await?, digest) {
            Ok(_) => Ok(true),
            Err(ArtifactStoreError::NotFound { .. }) => Ok(false),
            Err(error) => Err(error),
        }
    }

    async fn export(
        &self,
        digest: &ArtifactDigest,
    ) -> Result<Box<dyn ArtifactByteStream>, ArtifactStoreError> {
        let image = select_image(&self.images().await?, digest)?;
        let stream_id = next_transfer_id("export");
        let lease_id = next_transfer_id("lease");
        create_lease(
            self.channel.clone(),
            &self.settings.namespace,
            &lease_id,
            self.clock.timestamp(),
        )
        .await?;
        let duplex = match open_stream(
            self.channel.clone(),
            &self.settings.namespace,
            &stream_id,
            Some(&lease_id),
        )
        .await
        {
            Ok(duplex) => duplex,
            Err(error) => {
                let _ignored =
                    delete_lease(self.channel.clone(), &self.settings.namespace, &lease_id).await;
                return Err(error);
            }
        };
        let transfer = spawn_transfer(
            self.channel.clone(),
            self.settings.namespace.clone(),
            containerd::to_any(&ImageStore {
                name: image.name,
                platforms: vec![host_platform()],
                ..Default::default()
            }),
            containerd::to_any(&ImageExportStream {
                stream: stream_id,
                media_type: String::new(),
                platforms: vec![host_platform()],
                all_platforms: false,
                skip_compatibility_manifest: false,
                skip_non_distributable: false,
            }),
            "export",
            Some(digest.as_str().to_owned()),
            lease_id,
        );
        Ok(Box::new(ContainerdArtifactStream::new(duplex, transfer)))
    }

    async fn import(
        &self,
        source: Box<dyn ArtifactByteStream>,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let stream_id = next_transfer_id("import");
        let image_name = format!("maestro.local/artifacts/{stream_id}:latest");
        let lease_id = next_transfer_id("lease");
        create_lease(
            self.channel.clone(),
            &self.settings.namespace,
            &lease_id,
            self.clock.timestamp(),
        )
        .await?;
        let duplex = match open_stream(
            self.channel.clone(),
            &self.settings.namespace,
            &stream_id,
            Some(&lease_id),
        )
        .await
        {
            Ok(duplex) => duplex,
            Err(error) => {
                let _ignored =
                    delete_lease(self.channel.clone(), &self.settings.namespace, &lease_id).await;
                return Err(error);
            }
        };
        let platform = host_platform();
        let transfer = spawn_transfer(
            self.channel.clone(),
            self.settings.namespace.clone(),
            containerd::to_any(&ImageImportStream {
                stream: stream_id,
                media_type: String::new(),
                force_compress: false,
            }),
            containerd::to_any(&ImageStore {
                name: image_name.clone(),
                labels: managed_labels(),
                platforms: vec![platform.clone()],
                unpacks: vec![UnpackConfiguration {
                    platform: Some(platform),
                    snapshotter: self.settings.snapshotter.clone(),
                }],
                ..Default::default()
            }),
            "import",
            Some(image_name.clone()),
            lease_id,
        );
        upload_stream(source, duplex, transfer).await?;
        let image = self
            .host_platform_image(self.image(&image_name).await?, &image_name)
            .await?;
        self.ensure_digest_alias(&image, &image_name).await
    }

    async fn prune(
        &self,
        policy: &ArtifactPrunePolicy,
    ) -> Result<ArtifactPruneReport, ArtifactStoreError> {
        let ArtifactPrunePolicy::Preserve(preserved) = policy;
        let before = self.images().await?;
        let candidates = prune_candidates(&before, preserved);
        for candidate in &candidates {
            let result =
                containerd::services::v1::images_client::ImagesClient::new(self.channel.clone())
                    .delete(namespaced_artifact(
                        DeleteImageRequest {
                            name: candidate.name.clone(),
                            sync: true,
                            target: candidate.target.clone(),
                        },
                        &self.settings.namespace,
                    )?)
                    .await;
            if let Err(error) = result
                && !is_not_found(&error)
            {
                return Err(operation_error("prune image", Some(&candidate.name), error));
            }
        }
        let remaining = self.images().await?;
        Ok(ArtifactPruneReport {
            removed: removed_digests(&candidates, &remaining)?,
        })
    }
}

impl ContainerdRuntime {
    async fn build_artifact(
        &self,
        request: &ArtifactBuildRequest,
        output: &dyn ArtifactBuildOutputSink,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let artifact =
            run_build(request, &self.settings, self.build_runner.clone(), output).await?;
        let digest = self.import(artifact.into_stream().await?).await?;
        for tag in &request.tags {
            self.tag_digest(&digest, tag).await?;
        }
        Ok(digest)
    }

    async fn image(&self, reference: &str) -> Result<Image, ArtifactStoreError> {
        containerd::services::v1::images_client::ImagesClient::new(self.channel.clone())
            .get(namespaced_artifact(
                GetImageRequest {
                    name: reference.to_owned(),
                },
                &self.settings.namespace,
            )?)
            .await
            .map_err(|error| operation_error("get image", Some(reference), error))?
            .into_inner()
            .image
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: format!("containerd omitted image metadata for `{reference}`"),
            })
    }

    async fn images(&self) -> Result<Vec<Image>, ArtifactStoreError> {
        Ok(
            containerd::services::v1::images_client::ImagesClient::new(self.channel.clone())
                .list(namespaced_artifact(
                    ListImagesRequest {
                        filters: Vec::new(),
                    },
                    &self.settings.namespace,
                )?)
                .await
                .map_err(|error| operation_error("list images", None, error))?
                .into_inner()
                .images,
        )
    }

    async fn host_platform_image(
        &self,
        mut image: Image,
        reference: &str,
    ) -> Result<Image, ArtifactStoreError> {
        let target = image
            .target
            .take()
            .ok_or_else(|| ArtifactStoreError::Unavailable {
                message: format!("containerd image `{reference}` omitted its target descriptor"),
            })?;
        let target_digest = target.digest.clone();
        let host_target = resolve_host_manifest_descriptor(
            self.channel.clone(),
            &self.settings.namespace,
            target,
        )
        .await
        .map_err(artifact_metadata_error)?;
        if host_target.digest == target_digest {
            image.target = Some(host_target);
            return Ok(image);
        }
        image.target = Some(host_target);
        containerd::services::v1::images_client::ImagesClient::new(self.channel.clone())
            .update(namespaced_artifact(
                UpdateImageRequest {
                    image: Some(image.clone()),
                    update_mask: Some(prost_types::FieldMask {
                        paths: vec!["target".to_owned()],
                    }),
                    source_date_epoch: None,
                },
                &self.settings.namespace,
            )?)
            .await
            .map_err(|error| operation_error("normalize image platform", Some(reference), error))?;
        Ok(image)
    }

    async fn ensure_digest_alias(
        &self,
        image: &Image,
        reference: &str,
    ) -> Result<ArtifactDigest, ArtifactStoreError> {
        let digest = image_digest(image, reference)?;
        let alias = Image {
            name: digest.as_str().to_owned(),
            labels: managed_labels(),
            target: image.target.clone(),
            created_at: None,
            updated_at: None,
        };
        let result =
            containerd::services::v1::images_client::ImagesClient::new(self.channel.clone())
                .create(namespaced_artifact(
                    CreateImageRequest {
                        image: Some(alias),
                        source_date_epoch: None,
                    },
                    &self.settings.namespace,
                )?)
                .await;
        match result {
            Ok(_) => Ok(digest),
            Err(error) if error.code() == Code::AlreadyExists => {
                let existing = self.image(digest.as_str()).await?;
                if image_digest(&existing, digest.as_str())? == digest {
                    Ok(digest)
                } else {
                    Err(ArtifactStoreError::Rejected {
                        message: format!(
                            "containerd digest alias `{}` points at different content",
                            digest.as_str()
                        ),
                    })
                }
            }
            Err(error) => Err(operation_error(
                "create immutable image reference",
                Some(digest.as_str()),
                error,
            )),
        }
    }

    async fn tag_digest(
        &self,
        digest: &ArtifactDigest,
        destination: &ArtifactReference,
    ) -> Result<(), ArtifactStoreError> {
        let source = select_image(&self.images().await?, digest)?;
        let image = Image {
            name: destination.as_str().to_owned(),
            labels: managed_labels(),
            target: source.target,
            created_at: None,
            updated_at: None,
        };
        let mut client =
            containerd::services::v1::images_client::ImagesClient::new(self.channel.clone());
        let result = client
            .clone()
            .create(namespaced_artifact(
                CreateImageRequest {
                    image: Some(image.clone()),
                    source_date_epoch: None,
                },
                &self.settings.namespace,
            )?)
            .await;
        match result {
            Ok(_) => Ok(()),
            Err(error) if error.code() == Code::AlreadyExists => client
                .update(namespaced_artifact(
                    UpdateImageRequest {
                        image: Some(image),
                        update_mask: Some(prost_types::FieldMask {
                            paths: vec!["target".to_owned(), "labels".to_owned()],
                        }),
                        source_date_epoch: None,
                    },
                    &self.settings.namespace,
                )?)
                .await
                .map(|_| ())
                .map_err(|error| {
                    operation_error("update image tag", Some(destination.as_str()), error)
                }),
            Err(error) => Err(operation_error(
                "create image tag",
                Some(destination.as_str()),
                error,
            )),
        }
    }
}

fn artifact_metadata_error(error: RuntimeError) -> ArtifactStoreError {
    match error {
        RuntimeError::Unavailable { message } | RuntimeError::Stream { message } => {
            ArtifactStoreError::Unavailable { message }
        }
        RuntimeError::Rejected { message } | RuntimeError::InvalidSpec { message } => {
            ArtifactStoreError::Rejected { message }
        }
        error => ArtifactStoreError::Rejected {
            message: error.to_string(),
        },
    }
}

fn managed_labels() -> HashMap<String, String> {
    HashMap::from([(
        MANAGED_ARTIFACT_LABEL.to_owned(),
        MANAGED_ARTIFACT_VALUE.to_owned(),
    )])
}

pub(crate) fn next_transfer_id(operation: &str) -> String {
    format!("maestro-{operation}-{}", Uuid::new_v4().simple())
}

fn spawn_transfer(
    channel: Channel,
    namespace: String,
    source: Any,
    destination: Any,
    operation: &'static str,
    reference: Option<String>,
    lease_id: String,
) -> TransferTask {
    TransferTask::new(async move {
        let result = transfer(
            channel.clone(),
            namespace.clone(),
            source,
            destination,
            operation,
            reference,
            Some(lease_id.clone()),
        )
        .await;
        let cleanup = delete_lease(channel, &namespace, &lease_id).await;
        match result {
            Err(error) => Err(error),
            Ok(()) => cleanup,
        }
    })
}

async fn transfer(
    channel: Channel,
    namespace: String,
    source: Any,
    destination: Any,
    operation: &str,
    reference: Option<String>,
    lease_id: Option<String>,
) -> Result<(), ArtifactStoreError> {
    TransferClient::new(channel)
        .transfer(artifact_request(
            TransferRequest {
                source: Some(source),
                destination: Some(destination),
                options: Some(TransferOptions::default()),
            },
            &namespace,
            lease_id.as_deref(),
        )?)
        .await
        .map_err(|error| operation_error(operation, reference.as_deref(), error))?;
    Ok(())
}

#[derive(Clone)]
struct RegistryAuth {
    stream_id: String,
    host: String,
    credential: RegistryCredential,
}

struct RegistryTransfer {
    source: Any,
    destination: Any,
    operation: &'static str,
    reference: Option<String>,
    lease_id: Option<String>,
    auth: Option<RegistryAuth>,
}

impl RegistryAuth {
    fn resolver(&self) -> RegistryResolver {
        RegistryResolver {
            auth_stream: self.stream_id.clone(),
            ..Default::default()
        }
    }
}

async fn registry_auth(
    reference: &ArtifactReference,
    credentials: &dyn RegistryCredentialProvider,
) -> Result<Option<RegistryAuth>, ArtifactStoreError> {
    let parsed = reference.parsed()?;
    let host = parsed.registry();
    Ok(credentials
        .credential(host)
        .await?
        .map(|credential| RegistryAuth {
            stream_id: next_transfer_id("registry-auth"),
            host: host.to_owned(),
            credential,
        }))
}

async fn registry_transfer(
    channel: Channel,
    namespace: String,
    request: RegistryTransfer,
) -> Result<(), ArtifactStoreError> {
    let RegistryTransfer {
        source,
        destination,
        operation,
        reference,
        lease_id,
        auth,
    } = request;
    let Some(auth) = auth else {
        return transfer(
            channel,
            namespace,
            source,
            destination,
            operation,
            reference,
            lease_id,
        )
        .await;
    };
    let duplex = open_stream(
        channel.clone(),
        &namespace,
        &auth.stream_id,
        lease_id.as_deref(),
    )
    .await?;
    let transfer = TransferTask::new(transfer(
        channel,
        namespace,
        source,
        destination,
        operation,
        reference,
        lease_id,
    ));
    serve_registry_auth(duplex, transfer, &auth.host, &auth.credential).await
}

async fn create_lease(
    channel: Channel,
    namespace: &str,
    lease_id: &str,
    now: Timestamp,
) -> Result<(), ArtifactStoreError> {
    let expires = lease_expiration(now)?;
    LeasesClient::new(channel)
        .create(namespaced_artifact(
            CreateLeaseRequest {
                id: lease_id.to_owned(),
                labels: HashMap::from([(LEASE_EXPIRATION_LABEL.to_owned(), expires)]),
            },
            namespace,
        )?)
        .await
        .map_err(|error| operation_error("create transfer lease", Some(lease_id), error))?;
    Ok(())
}

pub(crate) fn lease_expiration(now: Timestamp) -> Result<String, ArtifactStoreError> {
    let expires_at_ms = now.0.checked_add(TRANSFER_LEASE_MILLIS).ok_or_else(|| {
        ArtifactStoreError::Unavailable {
            message: "containerd lease expiration exceeded timestamp bounds".to_owned(),
        }
    })?;
    OffsetDateTime::from_unix_timestamp_nanos(i128::from(expires_at_ms) * 1_000_000)
        .map_err(|error| ArtifactStoreError::Unavailable {
            message: format!("construct containerd lease expiration: {error}"),
        })?
        .format(&Rfc3339)
        .map_err(|error| ArtifactStoreError::Unavailable {
            message: format!("format containerd lease expiration: {error}"),
        })
}

async fn delete_lease(
    channel: Channel,
    namespace: &str,
    lease_id: &str,
) -> Result<(), ArtifactStoreError> {
    let result = LeasesClient::new(channel)
        .delete(namespaced_artifact(
            DeleteLeaseRequest {
                id: lease_id.to_owned(),
                sync: true,
            },
            namespace,
        )?)
        .await;
    match result {
        Ok(_) => Ok(()),
        Err(error) if is_not_found(&error) => Ok(()),
        Err(error) => Err(operation_error(
            "delete transfer lease",
            Some(lease_id),
            error,
        )),
    }
}
