use std::collections::{BTreeMap, BTreeSet};

use http::Uri;
use kernel_api::{
    BuiltinResource, Generation, Object, ObjectMeta, ResourceRevision, WebhookCategory,
    WebhookEvent, WebhookFormat, WebhookId, WebhookSpec, WebhookStatus,
};

use crate::LegacyEntry;
use crate::legacy_convert::annotations;
use crate::legacy_crypto::{LegacyCryptoError, LegacyDecryptor};
use crate::legacy_schema::{LegacySlackCategory, LegacySlackWebhook};

const SLACK_WEBHOOKS_KEY: &str = "/maetro/cluster/config/webhooks/slack";
const MAXIMUM_NAME_BYTES: usize = 256;
const MAXIMUM_ENDPOINT_BYTES: usize = 2_048;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct LegacyWebhookCatalog {
    webhooks: Vec<(WebhookId, LegacySlackWebhook)>,
    pub(crate) unclaimed: Vec<LegacyEntry>,
}

impl LegacyWebhookCatalog {
    pub(crate) fn decode(
        entries: &[LegacyEntry],
        master_secret: &str,
    ) -> Result<Self, LegacyWebhookError> {
        let decryptor = LegacyDecryptor::new(master_secret)?;
        let mut webhooks = Vec::new();
        let mut unclaimed = Vec::new();
        let mut ids = BTreeSet::new();

        for entry in entries {
            if entry.key() != SLACK_WEBHOOKS_KEY {
                unclaimed.push(entry.clone());
                continue;
            }
            let decoded = decryptor.decode_json::<Vec<LegacySlackWebhook>>(entry)?;
            for webhook in decoded {
                let id = WebhookId::new(&webhook.id).map_err(|error| {
                    invalid(entry.key(), format!("webhook id is invalid: {error}"))
                })?;
                if !ids.insert(id.clone()) {
                    return Err(invalid(
                        entry.key(),
                        format!("webhook id `{id}` occurs more than once"),
                    ));
                }
                validate(entry.key(), &webhook)?;
                webhooks.push((id, webhook));
            }
        }
        webhooks.sort_by(|left, right| left.0.cmp(&right.0));
        Ok(Self {
            webhooks,
            unclaimed,
        })
    }

    pub(crate) fn convert(&self) -> Vec<BuiltinResource> {
        self.webhooks
            .iter()
            .map(|(id, legacy)| convert_webhook(id, legacy))
            .collect()
    }
}

fn validate(key: &str, webhook: &LegacySlackWebhook) -> Result<(), LegacyWebhookError> {
    let name = webhook.name.trim();
    if name.is_empty() || name.len() > MAXIMUM_NAME_BYTES {
        return Err(invalid(
            key,
            format!("webhook `{}` has an empty or oversized name", webhook.id),
        ));
    }
    let endpoint = webhook.url.expose();
    if endpoint.len() > MAXIMUM_ENDPOINT_BYTES {
        return Err(invalid(
            key,
            format!("webhook `{}` endpoint is too long", webhook.id),
        ));
    }
    let endpoint = endpoint.parse::<Uri>().map_err(|error| {
        invalid(
            key,
            format!("webhook `{}` endpoint is invalid: {error}", webhook.id),
        )
    })?;
    if endpoint.scheme_str() != Some("https") || endpoint.authority().is_none() {
        return Err(invalid(
            key,
            format!(
                "webhook `{}` endpoint must be an absolute https URL",
                webhook.id
            ),
        ));
    }
    if endpoint
        .authority()
        .is_some_and(|authority| authority.as_str().contains('@'))
    {
        return Err(invalid(
            key,
            format!("webhook `{}` endpoint contains credentials", webhook.id),
        ));
    }
    let categories = webhook.categories.iter().copied().collect::<BTreeSet<_>>();
    if categories.is_empty() || categories.len() != webhook.categories.len() {
        return Err(invalid(
            key,
            format!(
                "webhook `{}` categories are empty or contain duplicates",
                webhook.id
            ),
        ));
    }
    Ok(())
}

fn convert_webhook(id: &WebhookId, legacy: &LegacySlackWebhook) -> BuiltinResource {
    BuiltinResource::Webhook(Object {
        meta: ObjectMeta {
            id: id.clone(),
            labels: BTreeMap::new(),
            annotations: annotations([(
                "migration.maestro.dev/legacy-slack-webhook",
                "true".to_owned(),
            )]),
            revision: ResourceRevision(0),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: WebhookSpec {
            name: legacy.name.trim().to_owned(),
            endpoint: legacy.url.clone(),
            events: vec![
                WebhookEvent::DeploymentTransition,
                WebhookEvent::NodeAvailability,
            ],
            categories: legacy
                .categories
                .iter()
                .copied()
                .map(convert_category)
                .collect(),
            enabled: legacy.enabled,
            format: WebhookFormat::Slack,
            signing_secret: None,
        },
        status: WebhookStatus {
            last_success_at: None,
            consecutive_failures: 0,
            retry_at: None,
            observed_generation: None,
            observations: Vec::new(),
            conditions: Vec::new(),
        },
    })
}

const fn convert_category(category: LegacySlackCategory) -> WebhookCategory {
    match category {
        LegacySlackCategory::Info => WebhookCategory::Info,
        LegacySlackCategory::Error => WebhookCategory::Error,
    }
}

fn invalid(key: impl Into<String>, message: impl Into<String>) -> LegacyWebhookError {
    LegacyWebhookError::InvalidState {
        key: key.into(),
        message: message.into(),
    }
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum LegacyWebhookError {
    #[error(transparent)]
    Crypto(#[from] LegacyCryptoError),
    #[error("legacy webhook state at `{key}` is invalid: {message}")]
    InvalidState { key: String, message: String },
}
