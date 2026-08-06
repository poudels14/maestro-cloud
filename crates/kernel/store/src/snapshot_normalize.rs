use kernel_api::{
    ArtifactTemplate, BuildTemplate, ResourceKind, ResourceName, SecretValue, ServiceSpec,
};
use serde::Serialize;
use serde_json::Value;

use crate::{ClusterSnapshot, SnapshotError};

const REDACTED: &str = "[REDACTED]";

impl ClusterSnapshot {
    /// Builds a stable, serializable view for final-state snapshot assertions.
    ///
    /// Store revisions, timestamps, runtime identities, public keys, and
    /// endpoints are normalized. Built-in secrets are replaced with a fixed
    /// marker. Unregistered payloads are omitted entirely because their schema
    /// cannot prove which fields are safe to print.
    pub fn normalized(&self) -> Result<NormalizedClusterSnapshot, SnapshotError> {
        let mut services = self.services.clone();
        for service in &mut services {
            redact_service_spec(&mut service.spec);
        }
        let mut deployments = self.deployments.clone();
        for deployment in &mut deployments {
            redact_service_spec(&mut deployment.spec.service);
        }
        let mut builds = self.builds.clone();
        for build in &mut builds {
            redact_build_template(&mut build.spec.template);
        }
        let mut webhooks = self.webhooks.clone();
        for webhook in &mut webhooks {
            webhook.spec.endpoint = SecretValue::new(REDACTED);
            if webhook.spec.signing_secret.is_some() {
                webhook.spec.signing_secret = Some(SecretValue::new(REDACTED));
            }
        }

        Ok(NormalizedClusterSnapshot {
            nodes: normalize_resources("Node", &self.nodes)?,
            node_tombstones: normalize_resources("NodeTombstone", &self.node_tombstones)?,
            node_networks: normalize_resources("NodeNetwork", &self.node_networks)?,
            node_firewalls: normalize_resources("NodeFirewall", &self.node_firewalls)?,
            services: normalize_resources("Service", &services)?,
            deployments: normalize_resources("Deployment", &deployments)?,
            assignments: normalize_resources("Assignment", &self.assignments)?,
            placement_histories: normalize_resources(
                "PlacementHistory",
                &self.placement_histories,
            )?,
            replica_states: normalize_resources("ReplicaState", &self.replica_states)?,
            ingress_routes: normalize_resources("IngressRoute", &self.ingress_routes)?,
            ingress_blocklists: normalize_resources("IngressBlocklist", &self.ingress_blocklists)?,
            traffic_generations: normalize_resources(
                "TrafficGeneration",
                &self.traffic_generations,
            )?,
            firewall_policies: normalize_resources("FirewallPolicy", &self.firewall_policies)?,
            dns_records: normalize_resources("DnsRecord", &self.dns_records)?,
            builds: normalize_resources("Build", &builds)?,
            previews: normalize_resources("Preview", &self.previews)?,
            upgrade_runs: normalize_resources("UpgradeRun", &self.upgrade_runs)?,
            webhooks: normalize_resources("Webhook", &webhooks)?,
            unregistered_resources: self
                .unregistered_resources
                .iter()
                .map(|resource| NormalizedUnregisteredResource {
                    kind: resource.kind.clone(),
                    id: resource.id.clone(),
                    payload: REDACTED,
                })
                .collect(),
        })
    }
}

/// Stable, secret-safe serialization tree used by YAML or JSON snapshots.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedClusterSnapshot {
    /// Cluster node resources in key order.
    pub nodes: Vec<Value>,
    /// Removed-node identity guards in key order.
    pub node_tombstones: Vec<Value>,
    /// WireGuard publications in key order.
    pub node_networks: Vec<Value>,
    /// Desired node firewall rulesets in key order.
    pub node_firewalls: Vec<Value>,
    /// Deployable services in key order.
    pub services: Vec<Value>,
    /// Immutable deployments in key order.
    pub deployments: Vec<Value>,
    /// Scheduled assignments in key order.
    pub assignments: Vec<Value>,
    /// Durable placement audit records in key order.
    pub placement_histories: Vec<Value>,
    /// Observed replica states in key order.
    pub replica_states: Vec<Value>,
    /// Ingress routes in key order.
    pub ingress_routes: Vec<Value>,
    /// Ingress client-address blocklists in key order.
    pub ingress_blocklists: Vec<Value>,
    /// Blue/green traffic generations in key order.
    pub traffic_generations: Vec<Value>,
    /// Atomic firewall policies in key order.
    pub firewall_policies: Vec<Value>,
    /// Authoritative DNS records in key order.
    pub dns_records: Vec<Value>,
    /// Artifact builds in key order.
    pub builds: Vec<Value>,
    /// Pull-request previews in key order.
    pub previews: Vec<Value>,
    /// Cluster maintenance runs in key order.
    pub upgrade_runs: Vec<Value>,
    /// Outbound webhook configurations in key order.
    pub webhooks: Vec<Value>,
    /// Identities of future or custom resources, with payloads redacted.
    pub unregistered_resources: Vec<NormalizedUnregisteredResource>,
}

/// Secret-safe identity of one unregistered resource.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct NormalizedUnregisteredResource {
    /// Open resource kind parsed from the canonical store key.
    pub kind: ResourceKind,
    /// Open resource identity parsed from the canonical store key.
    pub id: ResourceName,
    /// Fixed marker replacing the schema-unknown payload.
    pub payload: &'static str,
}

fn redact_service_spec(spec: &mut ServiceSpec) {
    if let ArtifactTemplate::Build { template } = &mut spec.artifact {
        redact_build_template(template);
    }
    if let Some(secrets) = &mut spec.secrets {
        for secret in secrets.values_mut().values_mut() {
            *secret = SecretValue::new(REDACTED);
        }
    }
}

fn redact_build_template(template: &mut BuildTemplate) {
    for secret in template.secrets.values_mut() {
        *secret = SecretValue::new(REDACTED);
    }
}

fn normalize_resources<Resource>(
    kind: &'static str,
    resources: &[Resource],
) -> Result<Vec<Value>, SnapshotError>
where
    Resource: Serialize,
{
    resources
        .iter()
        .map(|resource| {
            let mut value =
                serde_json::to_value(resource).map_err(|error| SnapshotError::Normalize {
                    kind,
                    message: error.to_string(),
                })?;
            normalize_value(&mut value);
            Ok(value)
        })
        .collect()
}

fn normalize_value(value: &mut Value) {
    match value {
        Value::Array(values) => {
            for value in values {
                normalize_value(value);
            }
        }
        Value::Object(fields) => {
            for (name, value) in fields {
                if matches!(name.as_str(), "revision" | "resourceRevision") {
                    *value = Value::from(0);
                } else if is_timestamp(name) {
                    *value = Value::String("<timestamp>".to_string());
                } else if let Some(marker) = runtime_marker(name) {
                    *value = Value::String(marker.to_string());
                } else {
                    normalize_value(value);
                }
            }
        }
        _ => {}
    }
}

fn is_timestamp(name: &str) -> bool {
    name.ends_with("At")
        || matches!(
            name,
            "deletionTimestamp" | "lastSeen" | "lastTransitionTime"
        )
}

fn runtime_marker(name: &str) -> Option<&'static str> {
    match name {
        "instanceId" | "previousInstanceId" => Some("<instance-id>"),
        "workloadId" => Some("<workload-id>"),
        "publicKey" => Some("<public-key>"),
        "endpoint" => Some("<endpoint>"),
        _ => None,
    }
}
