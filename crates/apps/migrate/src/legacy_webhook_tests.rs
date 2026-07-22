use kernel_api::{
    AnnotationKey, BuiltinKind, Webhook, WebhookCategory, WebhookEvent, WebhookFormat,
};
use serde_json::json;

use crate::legacy_crypto::encrypt_for_test;
use crate::{LegacyEntry, LegacyPlanError, LegacySnapshot, plan_legacy_snapshot};

type TestResult = Result<(), Box<dyn std::error::Error>>;

const MASTER_SECRET: &str = "correct horse battery staple";
const SLACK_KEY: &str = "/maetro/cluster/config/webhooks/slack";

#[test]
fn cutover_plan_authenticates_and_converts_slack_webhooks() -> TestResult {
    let snapshot = LegacySnapshot::new(vec![encrypted(
        json!([
            {
                "id": "wh_operations",
                "name": " Operations ",
                "url": "https://hooks.slack.test/services/operations-token",
                "categories": ["info", "error"]
            },
            {
                "id": "wh_errors",
                "name": "Failures",
                "url": "https://hooks.slack.test/services/errors-token",
                "categories": ["error"],
                "enabled": false
            }
        ]),
        MASTER_SECRET,
    )?])?;

    let plan = plan_legacy_snapshot(&snapshot, MASTER_SECRET)?;
    let operations = decode_webhook(&plan, "wh_operations")?;
    let errors = decode_webhook(&plan, "wh_errors")?;

    assert_eq!(operations.spec.name, "Operations");
    assert_eq!(operations.spec.format, WebhookFormat::Slack);
    assert_eq!(
        operations.spec.endpoint.expose(),
        "https://hooks.slack.test/services/operations-token"
    );
    assert_eq!(
        operations.spec.events,
        [
            WebhookEvent::DeploymentTransition,
            WebhookEvent::NodeAvailability
        ]
    );
    assert_eq!(
        operations.spec.categories,
        [WebhookCategory::Info, WebhookCategory::Error]
    );
    assert!(operations.spec.enabled);
    assert!(operations.spec.signing_secret.is_none());
    assert!(operations.status.observed_generation.is_none());
    assert_eq!(
        operations
            .meta
            .annotations
            .get(&AnnotationKey(
                "migration.maestro.dev/legacy-slack-webhook".to_owned()
            ))
            .map(String::as_str),
        Some("true")
    );
    assert!(!errors.spec.enabled);
    assert_eq!(errors.spec.categories, [WebhookCategory::Error]);
    Ok(())
}

#[test]
fn cutover_plan_rejects_wrong_slack_encryption_secret() -> TestResult {
    let snapshot = LegacySnapshot::new(vec![encrypted(
        json!([{
            "id": "wh_operations",
            "name": "Operations",
            "url": "https://hooks.slack.test/services/token",
            "categories": ["info"]
        }]),
        MASTER_SECRET,
    )?])?;

    assert!(matches!(
        plan_legacy_snapshot(&snapshot, "wrong secret"),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

#[test]
fn cutover_plan_rejects_duplicate_or_invalid_slack_controls() -> TestResult {
    let duplicate_ids = LegacySnapshot::new(vec![encrypted(
        json!([
            {
                "id": "wh_operations",
                "name": "Operations",
                "url": "https://hooks.slack.test/services/one",
                "categories": ["info"]
            },
            {
                "id": "wh_operations",
                "name": "Other",
                "url": "https://hooks.slack.test/services/two",
                "categories": ["error"]
            }
        ]),
        MASTER_SECRET,
    )?])?;
    assert!(matches!(
        plan_legacy_snapshot(&duplicate_ids, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));

    let duplicate_categories = LegacySnapshot::new(vec![encrypted(
        json!([{
            "id": "wh_operations",
            "name": "Operations",
            "url": "https://hooks.slack.test/services/token",
            "categories": ["error", "error"]
        }]),
        MASTER_SECRET,
    )?])?;
    assert!(matches!(
        plan_legacy_snapshot(&duplicate_categories, MASTER_SECRET),
        Err(LegacyPlanError::DecodeLegacyState { .. })
    ));
    Ok(())
}

fn encrypted(value: serde_json::Value, secret: &str) -> Result<LegacyEntry, LegacyPlanError> {
    encrypt_for_test(secret, value.to_string().as_bytes())
        .map(|value| LegacyEntry::new(SLACK_KEY, value))
        .map_err(|error| LegacyPlanError::DecodeLegacyState {
            message: error.to_string(),
        })
}

fn decode_webhook(
    plan: &crate::MigrationPlan,
    id: &str,
) -> Result<Webhook, Box<dyn std::error::Error>> {
    let write = plan
        .writes()
        .iter()
        .find(|write| write.kind() == BuiltinKind::Webhook && write.id().as_str() == id)
        .ok_or_else(|| std::io::Error::other(format!("missing Webhook/{id}")))?;
    Ok(serde_json::from_slice(write.value())?)
}
