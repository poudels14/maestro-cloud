use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::sync::Arc;
use std::time::{Duration, Instant};

use axum::Json;
use axum::extract::{Path, State};
use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use maestro_control::{AgentHub, ObservedReplicas, StoreWriter};
use maestro_core::{
    DeploymentId, DeploymentStatus, NodeId, NodeState, ServiceId, SlackCategory, SlackWebhook,
};
use maestro_store::{Consensus, Mutation, SecretKey};
use rand::distr::{Alphanumeric, SampleString};
use serde::{Deserialize, Serialize};
use serde_json::json;

use crate::node_service::ForwardingWriter;

const SEND_TIMEOUT: Duration = Duration::from_secs(10);
const NOTIFY_INTERVAL: Duration = Duration::from_secs(2);
const ID_PREFIX: &str = "wh_";
const ID_LENGTH: usize = 10;
const SHORT_ID: usize = 6;
const URL_PREFIX: &str = "https://";

#[derive(Clone)]
pub struct SlackState {
    pub writer: Arc<ForwardingWriter>,
    pub key: Arc<SecretKey>,
    pub cluster: String,
    pub config_webhook: Option<String>,
    pub http: reqwest::Client,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct WebhookView {
    id: String,
    name: String,
    url: String,
    categories: Vec<SlackCategory>,
    enabled: bool,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct CreateWebhook {
    name: String,
    url: String,
    categories: Vec<SlackCategory>,
    enabled: Option<bool>,
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct UpdateWebhook {
    name: Option<String>,
    url: Option<String>,
    categories: Option<Vec<SlackCategory>>,
    enabled: Option<bool>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum Event {
    Queued {
        service: ServiceId,
        deployment: DeploymentId,
    },
    Ready {
        service: ServiceId,
        deployment: DeploymentId,
    },
    Crashed {
        service: ServiceId,
        deployment: DeploymentId,
        reason: String,
    },
    ServiceRemoved {
        service: ServiceId,
    },
    NodeDown {
        node: String,
        node_id: NodeId,
        role: String,
        address: String,
    },
    NodeRecovered {
        node: String,
        node_id: NodeId,
        downtime: Duration,
    },
}

#[derive(Default)]
struct Snapshot {
    deployments: HashMap<(ServiceId, DeploymentId), (DeploymentStatus, Option<String>)>,
    services: BTreeSet<ServiceId>,
    unavailable: BTreeMap<NodeId, Instant>,
}

pub async fn list_webhooks(State(state): State<SlackState>) -> Response {
    match stored(&state) {
        Ok(webhooks) => Json(
            webhooks
                .iter()
                .map(|webhook| view(&state.key, webhook))
                .collect::<Vec<_>>(),
        )
        .into_response(),
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
    }
}

pub async fn create_webhook(
    State(state): State<SlackState>,
    Json(request): Json<CreateWebhook>,
) -> Response {
    let problem = validate(
        Some(&request.name),
        Some(&request.url),
        Some(&request.categories),
    );
    match (problem, stored(&state)) {
        (Some(problem), _) => error_response(StatusCode::BAD_REQUEST, problem),
        (None, Err(error)) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        (None, Ok(mut webhooks)) => {
            let webhook = SlackWebhook {
                id: format!(
                    "{ID_PREFIX}{}",
                    Alphanumeric
                        .sample_string(&mut rand::rng(), ID_LENGTH)
                        .to_lowercase()
                ),
                name: request.name.trim().to_string(),
                url: state.key.seal(&request.url),
                categories: request.categories,
                enabled: request.enabled.unwrap_or(true),
            };
            let created = view(&state.key, &webhook);
            webhooks.push(webhook);
            match save(&state, webhooks).await {
                Ok(()) => Json(created).into_response(),
                Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
            }
        }
    }
}

pub async fn update_webhook(
    State(state): State<SlackState>,
    Path(id): Path<String>,
    Json(request): Json<UpdateWebhook>,
) -> Response {
    let problem = validate(
        request.name.as_ref(),
        request.url.as_ref(),
        request.categories.as_ref(),
    );
    match (problem, stored(&state)) {
        (Some(problem), _) => error_response(StatusCode::BAD_REQUEST, problem),
        (None, Err(error)) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        (None, Ok(mut webhooks)) => match webhooks.iter_mut().find(|webhook| webhook.id == id) {
            None => error_response(StatusCode::NOT_FOUND, format!("webhook {id} not found")),
            Some(webhook) => {
                if let Some(name) = &request.name {
                    webhook.name = name.trim().to_string();
                }
                if let Some(url) = &request.url {
                    webhook.url = state.key.seal(url);
                }
                if let Some(categories) = &request.categories {
                    webhook.categories = categories.clone();
                }
                if let Some(enabled) = request.enabled {
                    webhook.enabled = enabled;
                }
                let updated = view(&state.key, webhook);
                match save(&state, webhooks).await {
                    Ok(()) => Json(updated).into_response(),
                    Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
                }
            }
        },
    }
}

pub async fn delete_webhook(State(state): State<SlackState>, Path(id): Path<String>) -> Response {
    match stored(&state) {
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        Ok(webhooks) => {
            let remaining = webhooks
                .iter()
                .filter(|webhook| webhook.id != id)
                .cloned()
                .collect::<Vec<_>>();
            match (
                remaining.len() == webhooks.len(),
                save(&state, remaining).await,
            ) {
                (true, _) => {
                    error_response(StatusCode::NOT_FOUND, format!("webhook {id} not found"))
                }
                (false, Ok(())) => StatusCode::NO_CONTENT.into_response(),
                (false, Err(error)) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
            }
        }
    }
}

pub async fn test_webhook(State(state): State<SlackState>, Path(id): Path<String>) -> Response {
    let webhook =
        stored(&state).map(|webhooks| webhooks.into_iter().find(|webhook| webhook.id == id));
    match webhook {
        Err(error) => error_response(StatusCode::INTERNAL_SERVER_ERROR, error),
        Ok(None) => error_response(StatusCode::NOT_FOUND, format!("webhook {id} not found")),
        Ok(Some(webhook)) => {
            let text = format!(
                ":test_tube: [{}] test message from maestro webhook `{}`",
                state.cluster, webhook.name
            );
            let url = state.key.open(&webhook.url).unwrap_or_default();
            match send(&state.http, &url, &text).await {
                Ok(()) => Json(json!({ "sent": true })).into_response(),
                Err(error) => error_response(StatusCode::BAD_GATEWAY, error),
            }
        }
    }
}

pub async fn run_notifier(state: SlackState, consensus: Arc<Consensus>, hub: Arc<AgentHub>) {
    let mut ticker = tokio::time::interval(NOTIFY_INTERVAL);
    let mut previous: Option<Snapshot> = None;
    loop {
        ticker.tick().await;
        previous = match consensus.is_leader() {
            false => None,
            true => match snapshot(&state, &hub, previous.as_ref()) {
                Ok(current) => {
                    if let Some(previous) = &previous {
                        for event in events(previous, &current, &state, &hub) {
                            dispatch(&state, &event).await;
                        }
                    }
                    Some(current)
                }
                Err(error) => {
                    tracing::warn!(%error, "slack notifier could not read cluster state");
                    previous
                }
            },
        };
    }
}

fn snapshot(
    state: &SlackState,
    hub: &AgentHub,
    previous: Option<&Snapshot>,
) -> Result<Snapshot, String> {
    let store = state.writer.state();
    let services = store.services().map_err(|error| error.to_string())?;
    let deployments = services
        .iter()
        .map(|(id, _)| store.deployments(id))
        .collect::<Result<Vec<_>, _>>()
        .map_err(|error| error.to_string())?
        .concat()
        .into_iter()
        .map(|deployment| {
            (
                (deployment.service_id, deployment.id),
                (deployment.status, deployment.failure),
            )
        })
        .collect();
    let unavailable = store
        .nodes()
        .map_err(|error| error.to_string())?
        .into_iter()
        .filter(|node| node.state == NodeState::Active && !hub.node_available(node.id))
        .map(|node| {
            let since = previous
                .and_then(|previous| previous.unavailable.get(&node.id).copied())
                .unwrap_or_else(Instant::now);
            (node.id, since)
        })
        .collect();
    Ok(Snapshot {
        deployments,
        services: services.into_iter().map(|(id, _)| id).collect(),
        unavailable,
    })
}

fn events(
    previous: &Snapshot,
    current: &Snapshot,
    state: &SlackState,
    hub: &AgentHub,
) -> Vec<Event> {
    let nodes = state
        .writer
        .state()
        .nodes()
        .unwrap_or_default()
        .into_iter()
        .map(|node| (node.id, node))
        .collect::<BTreeMap<_, _>>();
    let down = current
        .unavailable
        .keys()
        .filter(|node_id| !previous.unavailable.contains_key(node_id))
        .filter_map(|node_id| nodes.get(node_id))
        .map(|node| Event::NodeDown {
            node: node.name.clone(),
            node_id: node.id,
            role: format!("{:?}", node.role).to_lowercase(),
            address: node.endpoint.clone(),
        });
    let recovered = previous
        .unavailable
        .iter()
        .filter(|(node_id, _)| {
            !current.unavailable.contains_key(node_id) && hub.node_available(**node_id)
        })
        .filter_map(|(node_id, since)| {
            nodes.get(node_id).map(|node| Event::NodeRecovered {
                node: node.name.clone(),
                node_id: node.id,
                downtime: since.elapsed(),
            })
        });
    deployment_and_service_events(previous, current)
        .into_iter()
        .chain(down)
        .chain(recovered)
        .collect()
}

fn deployment_and_service_events(previous: &Snapshot, current: &Snapshot) -> Vec<Event> {
    let deployment_events = current
        .deployments
        .iter()
        .filter_map(|(key, (status, failure))| {
            let before = previous.deployments.get(key).map(|(status, _)| *status);
            let (service, deployment) = key.clone();
            match (before, status) {
                (None, _) => Some(Event::Queued {
                    service,
                    deployment,
                }),
                (Some(DeploymentStatus::Ready), DeploymentStatus::Ready) => None,
                (Some(_), DeploymentStatus::Ready) => Some(Event::Ready {
                    service,
                    deployment,
                }),
                (Some(DeploymentStatus::Failed), DeploymentStatus::Failed) => None,
                (Some(_), DeploymentStatus::Failed) => Some(Event::Crashed {
                    service,
                    deployment,
                    reason: failure.clone().unwrap_or_else(|| "unknown".to_string()),
                }),
                _ => None,
            }
        });
    let removed = previous
        .services
        .difference(&current.services)
        .map(|service| Event::ServiceRemoved {
            service: service.clone(),
        });
    deployment_events.chain(removed).collect()
}

async fn dispatch(state: &SlackState, event: &Event) {
    let (category, text) = render(&state.cluster, event);
    let configured = state.config_webhook.iter().cloned();
    let subscribed = stored(state)
        .unwrap_or_default()
        .into_iter()
        .filter(|webhook| webhook.enabled && webhook.categories.contains(&category))
        .filter_map(|webhook| state.key.open(&webhook.url).ok());
    for url in configured.chain(subscribed) {
        if let Err(error) = send(&state.http, &url, &text).await {
            tracing::warn!(%error, "slack notification failed");
        }
    }
}

fn render(cluster: &str, event: &Event) -> (SlackCategory, String) {
    match event {
        Event::Queued {
            service,
            deployment,
        } => (
            SlackCategory::Info,
            format!(
                ":rocket: *Deployment queued* — `{service}` (`{}`)\n> cluster: `{cluster}`",
                short(deployment.as_str())
            ),
        ),
        Event::Ready {
            service,
            deployment,
        } => (
            SlackCategory::Info,
            format!(
                ":white_check_mark: *Deployment ready* — `{service}` (`{}`)\n> cluster: `{cluster}`",
                short(deployment.as_str())
            ),
        ),
        Event::Crashed {
            service,
            deployment,
            reason,
        } => (
            SlackCategory::Error,
            format!(
                ":x: *Deployment crashed* — `{service}` (`{}`)\n> cluster: `{cluster}`\n> reason: {}",
                short(deployment.as_str()),
                escape(reason).replace('\n', "\n> ")
            ),
        ),
        Event::ServiceRemoved { service } => (
            SlackCategory::Info,
            format!(":wastebasket: *Service removed* — `{service}`\n> cluster: `{cluster}`"),
        ),
        Event::NodeDown {
            node,
            node_id,
            role,
            address,
        } => (
            SlackCategory::Error,
            format!(
                ":red_circle: *Node down* — `{}` (`{node_id}`)\n> cluster: `{cluster}` · role: `{role}` · address: `{address}`\n> control-plane heartbeat expired",
                escape(node)
            ),
        ),
        Event::NodeRecovered {
            node,
            node_id,
            downtime,
        } => (
            SlackCategory::Info,
            format!(
                ":large_green_circle: *Node recovered* — `{}` (`{node_id}`)\n> cluster: `{cluster}` · downtime: `{}`",
                escape(node),
                format_duration(*downtime)
            ),
        ),
    }
}

fn escape(text: &str) -> String {
    text.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
}

async fn send(http: &reqwest::Client, url: &str, text: &str) -> Result<(), String> {
    let response = http
        .post(url)
        .timeout(SEND_TIMEOUT)
        .json(&json!({ "text": text }))
        .send()
        .await
        .map_err(|error| error.without_url().to_string())?;
    match response.status().is_success() {
        true => Ok(()),
        false => Err(format!("slack returned {}", response.status())),
    }
}

async fn save(state: &SlackState, webhooks: Vec<SlackWebhook>) -> Result<(), String> {
    state
        .writer
        .write(vec![Mutation::SetSlackWebhooks { webhooks }])
        .await
        .map(|_| ())
        .map_err(|error| error.to_string())
}

fn stored(state: &SlackState) -> Result<Vec<SlackWebhook>, String> {
    state
        .writer
        .state()
        .slack_webhooks()
        .map_err(|error| error.to_string())
}

fn validate(
    name: Option<&String>,
    url: Option<&String>,
    categories: Option<&Vec<SlackCategory>>,
) -> Option<String> {
    let problems = [
        name.filter(|name| name.trim().is_empty())
            .map(|_| "name cannot be empty".to_string()),
        url.filter(|url| !url.starts_with(URL_PREFIX))
            .map(|_| "webhook url must start with https://".to_string()),
        categories
            .filter(|categories| categories.is_empty())
            .map(|_| "at least one category must be selected".to_string()),
    ];
    problems.into_iter().flatten().next()
}

fn view(key: &SecretKey, webhook: &SlackWebhook) -> WebhookView {
    WebhookView {
        id: webhook.id.clone(),
        name: webhook.name.clone(),
        url: mask_url(&key.open(&webhook.url).unwrap_or_default()),
        categories: webhook.categories.clone(),
        enabled: webhook.enabled,
    }
}

fn mask_url(url: &str) -> String {
    let (base, last) = url.rsplit_once('/').unwrap_or(("", url));
    format!("{base}/{}…", last.chars().take(4).collect::<String>())
}

fn short(id: &str) -> &str {
    &id[..id.len().min(SHORT_ID)]
}

fn format_duration(duration: Duration) -> String {
    let seconds = duration.as_secs();
    match seconds {
        seconds if seconds < 60 => format!("{seconds}s"),
        seconds if seconds < 3600 => format!("{}m {}s", seconds / 60, seconds % 60),
        seconds => format!("{}h {}m", seconds / 3600, (seconds % 3600) / 60),
    }
}

fn error_response(status: StatusCode, message: String) -> Response {
    (status, Json(json!({ "error": message }))).into_response()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn crash_reasons_cannot_ping_or_link() {
        let (_, text) = render(
            "prod",
            &Event::Crashed {
                service: ServiceId::from("api"),
                deployment: DeploymentId::from("d1"),
                reason: "boom <!channel> <https://evil.example|click>\nline two".to_string(),
            },
        );
        assert!(!text.contains("<!channel>"));
        assert!(text.contains("&lt;!channel&gt;"));
        assert!(text.contains("\n> line two"));
    }

    #[test]
    fn masks_urls_and_formats_events() {
        assert_eq!(
            mask_url("https://hooks.slack.com/services/T000/B000/abcdefgh"),
            "https://hooks.slack.com/services/T000/B000/abcd…"
        );
        assert_eq!(format_duration(Duration::from_secs(125)), "2m 5s");
        assert_eq!(format_duration(Duration::from_secs(3700)), "1h 1m");
        let (category, text) = render(
            "prod",
            &Event::Crashed {
                service: ServiceId::from("api"),
                deployment: DeploymentId::from("dmv7n1a4xao07"),
                reason: "a replica failed to start".to_string(),
            },
        );
        assert_eq!(category, SlackCategory::Error);
        assert_eq!(
            text,
            ":x: *Deployment crashed* — `api` (`dmv7n1`)\n> cluster: `prod`\n> reason: a replica failed to start"
        );
        assert_eq!(
            validate(Some(&" ".to_string()), None, None).as_deref(),
            Some("name cannot be empty")
        );
        assert!(validate(None, Some(&"http://x".to_string()), None).is_some());
    }

    #[test]
    fn detects_deployment_service_and_node_transitions() {
        let key = (ServiceId::from("api"), DeploymentId::from("d1"));
        let mut previous = Snapshot::default();
        previous.services.insert(ServiceId::from("old"));
        previous
            .deployments
            .insert(key.clone(), (DeploymentStatus::Deploying, None));
        let mut current = Snapshot::default();
        current
            .deployments
            .insert(key, (DeploymentStatus::Ready, None));
        current.deployments.insert(
            (ServiceId::from("api"), DeploymentId::from("d2")),
            (DeploymentStatus::Queued, None),
        );
        let found = deployment_and_service_events(&previous, &current);
        assert!(found.contains(&Event::Ready {
            service: ServiceId::from("api"),
            deployment: DeploymentId::from("d1")
        }));
        assert!(found.contains(&Event::Queued {
            service: ServiceId::from("api"),
            deployment: DeploymentId::from("d2")
        }));
        assert!(found.contains(&Event::ServiceRemoved {
            service: ServiceId::from("old")
        }));
    }
}
