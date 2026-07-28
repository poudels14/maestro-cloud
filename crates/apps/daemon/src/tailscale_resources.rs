use std::collections::{BTreeMap, BTreeSet};

use cluster::ClusterConfig;
use firewall::SystemHostAccess;
use kernel_api::{
    AnnotationKey, ArtifactTemplate, CommandSpec, ExecPolicy, FirewallDirection, FirewallPolicy,
    FirewallPolicyId, FirewallPolicySpec, FirewallPolicyStatus, FirewallRule, FirewallSubject,
    FirewallVerdict, Generation, HealthCheckSpec, HealthProbe, NodeApiAccess, Object, ObjectMeta,
    OwnerReference, Ownership, PlacementConstraint, ReplicaSpread, ResourceId, ResourceKind,
    ResourceName, ResourceRevision, RolloutState, SecretMountSpec, SecretValue, Service, ServiceId,
    ServiceSpec, ServiceStatus, TransportProtocol, VolumeAccess, VolumeMountSpec, VolumeSource,
};

const GATEWAY_SERVICE_ID: &str = "maestro-system-tailscale-gateway";
const GATEWAY_POLICY_ID: &str = "maestro-system-tailscale-egress";
const MANAGED_ANNOTATION: &str = "system.maestro.dev/owner";
const MANAGED_VALUE: &str = "tailscale-gateway";
const TAILSCALE_SOCKS_PORT: u16 = 1_055;
const TAILSCALE_HOSTNAME_PREFIX_MAX_LEN: usize = 52;
pub(crate) const TAILSCALE_IMAGE: &str = "ghcr.io/tailscale/tailscale:v1.98.8@sha256:d54b2e6a9c09f0e5ec52e82b9ad4af3d446b54a7c08075e92f11c39dd410105f";
const TAILSCALE_VERSION: &str = "tailscale-1.98.8";
pub(crate) const AUTH_SCRIPT: &str = r#"export PATH=/usr/local/bin:/usr/bin:/bin
replica="${HOSTNAME##*-}"
export TS_HOSTNAME="${MAESTRO_TAILSCALE_HOSTNAME_PREFIX}-${replica}"
set -a
. /run/secrets/tailscale.env
set +a

/usr/local/bin/containerboot &
containerboot_pid="$!"

stop_containerboot() {
  kill -TERM "$containerboot_pid" 2>/dev/null || true
}

fail_gateway() {
  echo "maestro Tailscale gateway: $1" >&2
  stop_containerboot
  wait "$containerboot_pid" 2>/dev/null || true
  exit 1
}

trap stop_containerboot HUP INT TERM

ready=false
attempt=0
while [ "$attempt" -lt 60 ]; do
  if /usr/local/bin/tailscale --socket=/tmp/tailscaled.sock status --json 2>/dev/null \
    | grep -q '"BackendState": "Running"'
  then
    ready=true
    break
  fi
  kill -0 "$containerboot_pid" 2>/dev/null \
    || fail_gateway "containerboot exited before Tailscale became ready"
  attempt=$((attempt + 1))
  sleep 1
done
[ "$ready" = true ] || fail_gateway "Tailscale did not become ready within 60 seconds"

/usr/local/bin/tailscale --socket=/tmp/tailscaled.sock set \
  --hostname="$TS_HOSTNAME" \
  || fail_gateway "could not reconcile the Tailscale hostname"

gateway="$(awk '$1 == "nameserver" { print $2; exit }' /etc/resolv.conf)"
[ -n "$gateway" ] || fail_gateway "could not discover the node workload gateway"

api_port=""
api_attempt=0
while [ "$api_attempt" -lt 60 ]; do
  previous_ifs="$IFS"
  IFS=,
  for port in $MAESTRO_NODE_API_PORTS; do
    if nc -z -w 1 "$gateway" "$port"; then
      api_port="$port"
      break
    fi
  done
  IFS="$previous_ifs"
  if [ -n "$api_port" ]; then
    break
  fi
  kill -0 "$containerboot_pid" 2>/dev/null \
    || fail_gateway "containerboot exited before the node API became reachable"
  api_attempt=$((api_attempt + 1))
  sleep 1
done
[ -n "$api_port" ] || fail_gateway "could not discover the node API listener"

/usr/local/bin/tailscale --socket=/tmp/tailscaled.sock serve reset \
  || fail_gateway "could not reset stale Tailscale Serve configuration"
/usr/local/bin/tailscale --socket=/tmp/tailscaled.sock serve --bg --yes --http=80 \
  "https+insecure://${gateway}:${api_port}" \
  || fail_gateway "could not expose the node API with Tailscale Serve"

wait "$containerboot_pid"
"#;

/// Ordinary resources that provide optional operator access through Tailscale.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TailscaleSystemResources {
    /// Highly available subnet-router service.
    pub(crate) service: Service,
    /// Workload egress policy attached to the subnet routers.
    pub(crate) firewall_policy: FirewallPolicy,
    /// Protected daemon API access granted only to running gateway replicas.
    pub(crate) system_host_access: SystemHostAccess,
}

impl TailscaleSystemResources {
    pub(crate) fn from_cluster(
        cluster: &ClusterConfig,
    ) -> Result<Option<Self>, TailscaleResourceError> {
        let Some(config) = &cluster.tailscale else {
            return Ok(None);
        };
        let service_id = ServiceId::new(GATEWAY_SERVICE_ID)?;
        let workload_subnets = cluster
            .nodes
            .values()
            .filter(|node| node.role.runs_workloads())
            .map(|node| node.workload_subnet)
            .collect::<Vec<_>>();
        let routes = config
            .advertised_routes(&workload_subnets)
            .into_iter()
            .map(|route| route.to_string())
            .collect::<Vec<_>>();
        let extra_args = if config.tags.is_empty() {
            "--accept-routes".to_owned()
        } else {
            format!("--accept-routes --advertise-tags={}", config.tags.join(","))
        };
        let environment = BTreeMap::from([
            ("TS_ACCEPT_DNS".to_owned(), "false".to_owned()),
            ("TS_AUTH_ONCE".to_owned(), "true".to_owned()),
            ("TS_ENABLE_HEALTH_CHECK".to_owned(), "true".to_owned()),
            ("TS_EXTRA_ARGS".to_owned(), extra_args),
            ("TS_KUBE_SECRET".to_owned(), String::new()),
            ("TS_LOCAL_ADDR_PORT".to_owned(), "0.0.0.0:9002".to_owned()),
            ("TS_ROUTES".to_owned(), routes.join(",")),
            (
                "MAESTRO_TAILSCALE_HOSTNAME_PREFIX".to_owned(),
                tailscale_hostname_prefix(&cluster.name),
            ),
            (
                "MAESTRO_NODE_API_PORTS".to_owned(),
                api_ports(&cluster.nodes)
                    .into_iter()
                    .map(|port| port.to_string())
                    .collect::<Vec<_>>()
                    .join(","),
            ),
            (
                "TS_SOCKS5_SERVER".to_owned(),
                format!(":{TAILSCALE_SOCKS_PORT}"),
            ),
            ("TS_STATE_DIR".to_owned(), "/state".to_owned()),
            ("TS_USERSPACE".to_owned(), "true".to_owned()),
        ]);
        let annotations = BTreeMap::from([(managed_annotation(), MANAGED_VALUE.to_owned())]);
        let system_host_access = SystemHostAccess {
            service_id: service_id.clone(),
            host_ports: api_ports(&cluster.nodes),
        };
        let service = Object {
            meta: ObjectMeta {
                id: service_id.clone(),
                labels: BTreeMap::new(),
                annotations: annotations.clone(),
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: Vec::new(),
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: ServiceSpec {
                name: "Tailscale Gateway".to_owned(),
                version: TAILSCALE_VERSION.to_owned(),
                artifact: ArtifactTemplate::Image {
                    reference: TAILSCALE_IMAGE.to_owned(),
                },
                preview: None,
                command: Some(CommandSpec {
                    executable: "/bin/sh".to_owned(),
                    arguments: vec!["-ceu".to_owned(), AUTH_SCRIPT.to_owned()],
                }),
                replicas: config.replicas,
                exposed_ports: vec![TAILSCALE_SOCKS_PORT, 9_002],
                health_check: Some(HealthCheckSpec {
                    probe: HealthProbe::Http {
                        port: 9_002,
                        path: "/healthz".to_owned(),
                    },
                    interval_secs: 10,
                    unhealthy_threshold: 3,
                }),
                max_restarts: None,
                environment,
                user: None,
                node_api: NodeApiAccess::Disabled,
                secrets: Some(SecretMountSpec::Dotenv {
                    mount_path: "/run/secrets/tailscale.env".to_owned(),
                    items: BTreeMap::from([("TS_AUTHKEY".to_owned(), config.auth_key.clone())]),
                }),
                volumes: vec![VolumeMountSpec {
                    source: VolumeSource::ReplicaManaged {
                        name: "tailscale-state".to_owned(),
                    },
                    target: "/state".to_owned(),
                    access: VolumeAccess::ReadWrite,
                }],
                placement: PlacementConstraint {
                    replica_spread: ReplicaSpread::BestEffort,
                    ..PlacementConstraint::default()
                },
                exec: ExecPolicy::Denied,
            },
            status: ServiceStatus {
                active_deployment_id: None,
                replica_override: None,
                rollout: RolloutState::Active,
                rollout_bypass_generation: None,
                conditions: Vec::new(),
            },
        };
        service.spec.validate()?;

        let firewall_policy = Object {
            meta: ObjectMeta {
                id: FirewallPolicyId::new(GATEWAY_POLICY_ID)?,
                labels: BTreeMap::new(),
                annotations,
                revision: ResourceRevision::default(),
                generation: Generation(1),
                owner_refs: vec![OwnerReference {
                    resource: ResourceId::new(
                        ResourceKind::new("Service")?,
                        ResourceName::from(service_id.clone()),
                    ),
                    ownership: Ownership::Controller,
                }],
                finalizers: BTreeSet::new(),
                deletion_timestamp: None,
            },
            spec: FirewallPolicySpec {
                direction: FirewallDirection::Egress,
                subject: FirewallSubject::Service(service_id),
                rules: routes
                    .iter()
                    .map(|route| FirewallRule {
                        cidr: route.clone(),
                        protocol: TransportProtocol::Any,
                        ports: Vec::new(),
                        verdict: FirewallVerdict::Allow,
                    })
                    .collect(),
                // Tailscale coordination, DERP, STUN, and direct peers use a
                // changing public endpoint set, so the remaining egress stays open.
                default_verdict: FirewallVerdict::Allow,
            },
            status: FirewallPolicyStatus {
                applied_generation: Generation::default(),
                ruleset_digest: None,
                conditions: Vec::new(),
            },
        };
        Ok(Some(Self {
            service,
            firewall_policy,
            system_host_access,
        }))
    }

    pub(crate) fn with_auth_key(
        mut self,
        auth_key: SecretValue,
    ) -> Result<Self, TailscaleResourceError> {
        let secrets = self
            .service
            .spec
            .secrets
            .as_mut()
            .ok_or(TailscaleResourceError::MissingAuthSecretMount)?;
        let SecretMountSpec::Dotenv { items, .. } = secrets else {
            return Err(TailscaleResourceError::MissingAuthSecretMount);
        };
        items.insert("TS_AUTHKEY".to_owned(), auth_key);
        Ok(self)
    }
}

fn tailscale_hostname_prefix(cluster_name: &str) -> String {
    const LEADING: &str = "maestro-";
    const TRAILING: &str = "-gateway";

    let available = TAILSCALE_HOSTNAME_PREFIX_MAX_LEN - LEADING.len() - TRAILING.len();
    let cluster_name = cluster_name[..cluster_name.len().min(available)].trim_end_matches('-');
    format!("{LEADING}{cluster_name}{TRAILING}")
}

fn api_ports(nodes: &BTreeMap<kernel_api::NodeId, cluster::NodeDefinition>) -> Vec<u16> {
    let mut ports = nodes
        .values()
        .map(|node| node.endpoint.api_port)
        .collect::<Vec<_>>();
    ports.sort_unstable();
    ports.dedup();
    ports
}

pub(crate) fn is_managed(annotations: &BTreeMap<AnnotationKey, String>) -> bool {
    annotations
        .get(&managed_annotation())
        .is_some_and(|value| value == MANAGED_VALUE)
}

pub(crate) fn resource_ids() -> Result<(ServiceId, FirewallPolicyId), kernel_api::InvalidIdentifier>
{
    Ok((
        ServiceId::new(GATEWAY_SERVICE_ID)?,
        FirewallPolicyId::new(GATEWAY_POLICY_ID)?,
    ))
}

fn managed_annotation() -> AnnotationKey {
    AnnotationKey(MANAGED_ANNOTATION.to_owned())
}

/// Invalid built-in Tailscale resource construction.
#[derive(Debug, thiserror::Error)]
pub enum TailscaleResourceError {
    /// A fixed built-in identifier no longer satisfies the resource contract.
    #[error(transparent)]
    Identifier(#[from] kernel_api::InvalidIdentifier),
    /// The generated service violated the public workload contract.
    #[error(transparent)]
    Service(#[from] kernel_api::ServiceSpecError),
    /// An internal resource template lost the private auth-key mount.
    #[error("built-in Tailscale service is missing its auth-key secret mount")]
    MissingAuthSecretMount,
}
