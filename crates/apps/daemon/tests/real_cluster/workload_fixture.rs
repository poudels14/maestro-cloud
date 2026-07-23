use std::collections::{BTreeMap, BTreeSet};

use kernel_api::{
    ArtifactTemplate, ClusterId, CommandSpec, ExecPolicy, Generation, HealthCheckSpec, HealthProbe,
    NodeApiAccess, Object, ObjectMeta, PlacementConstraint, ResourceKind, ResourceName,
    ResourceRevision, RolloutState, SecretMountSpec, SecretValue, ServiceId, ServiceSpec,
    ServiceStatus,
};
use kernel_store::{CasOutcome, ExpectedVersion, Keyspace, PutRequest, Store};

use super::RealClusterError;

pub(super) const WORKLOAD_IMAGE: &str = "mirror.gcr.io/library/busybox:1.36.1";
pub(super) const LOG_STDOUT_MARKER: &str = "m3-runtime-stdout";
pub(super) const LOG_STDERR_MARKER: &str = "m3-runtime-stderr";

pub(super) async fn put_service(
    store: &dyn Store,
    cluster_id: &ClusterId,
) -> Result<(), RealClusterError> {
    let id = ServiceId::new("m3-runtime").map_err(RealClusterError::from_display)?;
    let service = Object {
        meta: ObjectMeta {
            id: id.clone(),
            labels: BTreeMap::new(),
            annotations: BTreeMap::new(),
            revision: ResourceRevision::default(),
            generation: Generation(1),
            owner_refs: Vec::new(),
            finalizers: BTreeSet::new(),
            deletion_timestamp: None,
        },
        spec: ServiceSpec {
            name: "M3 runtime acceptance".to_owned(),
            version: "1".to_owned(),
            artifact: ArtifactTemplate::Image {
                reference: WORKLOAD_IMAGE.to_owned(),
            },
            preview: None,
            command: Some(CommandSpec {
                executable: "/bin/sh".to_owned(),
                arguments: vec![
                    "-c".to_owned(),
                    format!(
                        "set -eu; test -s /run/secrets/maestro.env; cat /proc/sys/kernel/random/uuid > /tmp/maestro-boot-id; printf '{LOG_STDOUT_MARKER} %s\\n' \"$(cat /tmp/maestro-boot-id)\"; printf '{LOG_STDERR_MARKER} %s\\n' \"$(cat /tmp/maestro-boot-id)\" >&2; mkdir -p /www; printf ok > /www/health; exec httpd -f -p 8080 -h /www"
                    ),
                ],
            }),
            replicas: 1,
            exposed_ports: vec![8080],
            health_check: Some(HealthCheckSpec {
                probe: HealthProbe::Http {
                    port: 8080,
                    path: "/health".to_owned(),
                },
                interval_secs: 1,
                unhealthy_threshold: 5,
            }),
            max_restarts: Some(3),
            environment: BTreeMap::new(),
            user: None,
            node_api: NodeApiAccess::Disabled,
            secrets: Some(SecretMountSpec {
                mount_path: "/run/secrets/maestro.env".to_owned(),
                items: BTreeMap::from([(
                    "TOKEN".to_owned(),
                    SecretValue::new("real-runtime-secret"),
                )]),
            }),
            volumes: Vec::new(),
            placement: PlacementConstraint::default(),
            exec: ExecPolicy::Allowed,
        },
        status: ServiceStatus {
            active_deployment_id: None,
            replica_override: None,
            rollout: RolloutState::Active,
            rollout_bypass_generation: None,
            conditions: Vec::new(),
        },
    };
    let key = Keyspace::new(cluster_id).resource(
        &ResourceKind::new("Service").map_err(RealClusterError::from_display)?,
        &ResourceName::new(id.as_str()).map_err(RealClusterError::from_display)?,
    );
    let outcome = store
        .put_cas(PutRequest {
            key,
            value: serde_json::to_vec(&service).map_err(RealClusterError::from_display)?,
            expected: ExpectedVersion::Missing,
            session: None,
        })
        .await
        .map_err(RealClusterError::from_display)?;
    if matches!(outcome, CasOutcome::Applied(_)) {
        Ok(())
    } else {
        Err(RealClusterError::new("service seed conflicted"))
    }
}
