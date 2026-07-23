use std::io::{BufRead, Write};
use std::path::PathBuf;

use clap::Subcommand;

use crate::CliError;
use crate::api_client::{ApiClient, request_id};
use crate::contexts::ContextStore;
use crate::{rollout, services, up};

#[derive(Debug, Subcommand)]
pub(crate) enum ServiceCommand {
    /// List services from the active Maestro API context.
    Ls,
    /// Preview or apply services from maestro.services.jsonc.
    Rollout {
        /// Local, file://, or aws-secret:// services config source.
        #[arg(long, default_value = "maestro.services.jsonc")]
        config: String,
        /// Persist the previewed desired state.
        #[arg(long)]
        apply: bool,
        /// Allow this rollout to begin even when the service is frozen.
        #[arg(long)]
        force: bool,
        /// Limit the rollout to one or more service IDs.
        #[arg(long = "service")]
        services: Vec<String>,
        /// Apply without an interactive confirmation.
        #[arg(short = 'y', long)]
        yes: bool,
        /// Stable key for a single-service apply retry.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Package a local context and deploy one configured build service.
    Up {
        /// Service identity; may be omitted when the config contains exactly one service.
        service_id: Option<String>,
        /// Local, file://, or aws-secret:// services config source.
        #[arg(long, default_value = "maestro.services.jsonc")]
        config: String,
        /// Local build context directory.
        #[arg(long, default_value = ".")]
        context: PathBuf,
        /// Stable key to reuse after an ambiguous rollout failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// List retained deployment history for one service.
    Deployments {
        /// Owning service identity.
        service_id: String,
    },
    /// Trigger a new deployment generation for a service.
    Redeploy {
        /// Service identity.
        service_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Restart workload instances without replacing their deployment.
    Restart {
        /// Owning service identity.
        service_id: String,
        /// Deployment identity.
        deployment_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Cancel a queued or building deployment.
    Cancel {
        /// Owning service identity.
        service_id: String,
        /// Deployment identity.
        deployment_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Drain and remove a deployment.
    Remove {
        /// Owning service identity.
        service_id: String,
        /// Deployment identity.
        deployment_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Pause automatic rollout activity for a service.
    Freeze {
        /// Service identity.
        service_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Resume automatic rollout activity for a service.
    Unfreeze {
        /// Service identity.
        service_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Set or clear a temporary replica override.
    Replicas {
        #[command(subcommand)]
        command: ReplicaCommand,
    },
    /// Request cascading deletion of a service.
    Delete {
        /// Service identity.
        service_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

#[derive(Debug, Subcommand)]
pub(crate) enum ReplicaCommand {
    /// Set a temporary replica count.
    Set {
        /// Service identity.
        service_id: String,
        /// Temporary replica count, including zero.
        replicas: u32,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
    /// Return to the replica count in desired service state.
    Clear {
        /// Service identity.
        service_id: String,
        /// Stable key to reuse after an ambiguous transport failure.
        #[arg(long)]
        idempotency_key: Option<String>,
    },
}

pub(crate) async fn run(
    command: ServiceCommand,
    input: &mut dyn BufRead,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    let contexts = ContextStore::from_environment()?;
    let client = ApiClient::new(contexts.active()?)?;
    match command {
        ServiceCommand::Ls => services::list(&client, output).await,
        ServiceCommand::Rollout {
            config,
            apply,
            force,
            services,
            yes,
            idempotency_key,
        } => {
            rollout::run(
                &client,
                rollout::RolloutOptions {
                    config_source: &config,
                    filters: &services,
                    mode: if apply {
                        rollout::RolloutMode::Apply
                    } else {
                        rollout::RolloutMode::Preview
                    },
                    frozen_service_policy: if force {
                        rollout::FrozenServicePolicy::BypassFreeze
                    } else {
                        rollout::FrozenServicePolicy::HonorFreeze
                    },
                    confirmation: if yes {
                        rollout::ConfirmationMode::AssumeYes
                    } else {
                        rollout::ConfirmationMode::Prompt
                    },
                    idempotency_key,
                },
                input,
                output,
                &crate::config_source::SystemConfigSourceReader,
            )
            .await
        }
        ServiceCommand::Up {
            service_id,
            config,
            context,
            idempotency_key,
        } => {
            up::run(
                &client,
                &config,
                service_id,
                &context,
                request_id(idempotency_key)?,
                output,
                &crate::config_source::SystemConfigSourceReader,
            )
            .await
        }
        ServiceCommand::Deployments { service_id } => {
            crate::deployments::list(&client, service_id, output).await
        }
        ServiceCommand::Redeploy {
            service_id,
            idempotency_key,
        } => {
            service_action(
                &client,
                service_id,
                idempotency_key,
                services::ServiceLifecycleAction::Redeploy,
                output,
            )
            .await
        }
        ServiceCommand::Restart {
            service_id,
            deployment_id,
            idempotency_key,
        } => {
            deployment_action(
                &client,
                service_id,
                deployment_id,
                idempotency_key,
                services::DeploymentLifecycleAction::Restart,
                output,
            )
            .await
        }
        ServiceCommand::Cancel {
            service_id,
            deployment_id,
            idempotency_key,
        } => {
            deployment_action(
                &client,
                service_id,
                deployment_id,
                idempotency_key,
                services::DeploymentLifecycleAction::Cancel,
                output,
            )
            .await
        }
        ServiceCommand::Remove {
            service_id,
            deployment_id,
            idempotency_key,
        } => {
            deployment_action(
                &client,
                service_id,
                deployment_id,
                idempotency_key,
                services::DeploymentLifecycleAction::Remove,
                output,
            )
            .await
        }
        ServiceCommand::Freeze {
            service_id,
            idempotency_key,
        } => {
            service_action(
                &client,
                service_id,
                idempotency_key,
                services::ServiceLifecycleAction::Freeze,
                output,
            )
            .await
        }
        ServiceCommand::Unfreeze {
            service_id,
            idempotency_key,
        } => {
            service_action(
                &client,
                service_id,
                idempotency_key,
                services::ServiceLifecycleAction::Unfreeze,
                output,
            )
            .await
        }
        ServiceCommand::Replicas { command } => match command {
            ReplicaCommand::Set {
                service_id,
                replicas,
                idempotency_key,
            } => {
                services::set_replicas(
                    &client,
                    service_id,
                    request_id(idempotency_key)?,
                    services::ReplicaOverride::Set(replicas),
                    output,
                )
                .await
            }
            ReplicaCommand::Clear {
                service_id,
                idempotency_key,
            } => {
                services::set_replicas(
                    &client,
                    service_id,
                    request_id(idempotency_key)?,
                    services::ReplicaOverride::Clear,
                    output,
                )
                .await
            }
        },
        ServiceCommand::Delete {
            service_id,
            idempotency_key,
        } => {
            service_action(
                &client,
                service_id,
                idempotency_key,
                services::ServiceLifecycleAction::Delete,
                output,
            )
            .await
        }
    }
}

async fn service_action(
    client: &ApiClient,
    service_id: String,
    idempotency_key: Option<String>,
    action: services::ServiceLifecycleAction,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    services::service_lifecycle(
        client,
        service_id,
        request_id(idempotency_key)?,
        action,
        output,
    )
    .await
}

async fn deployment_action(
    client: &ApiClient,
    service_id: String,
    deployment_id: String,
    idempotency_key: Option<String>,
    action: services::DeploymentLifecycleAction,
    output: &mut dyn Write,
) -> Result<(), CliError> {
    services::deployment_lifecycle(
        client,
        service_id,
        deployment_id,
        request_id(idempotency_key)?,
        action,
        output,
    )
    .await
}
