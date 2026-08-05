use std::collections::{BTreeMap, BTreeSet};
use std::net::{IpAddr, Ipv4Addr};

use kernel_api::{
    Assignment, AssignmentId, AssignmentPhase, AssignmentSpec, AssignmentStatus, Deployment,
    DeploymentGoal, DeploymentId, DeploymentPhase, DeploymentSpec, DeploymentStatus, ExecPolicy,
    Generation, NodeApiAccess, NodeId, Object, ObjectMeta, PlacementConstraint, ResourceRevision,
    SecretValue, ServiceId, ServiceSpec, Timestamp, WorkloadId,
};

use crate::CliError;
use crate::contexts::Context;
use crate::exec_command::{
    SelectionInteraction, TerminalSize, exec_endpoint, select_assignment, select_deployment,
};

#[test]
fn deployment_selection_prefers_an_active_running_deployment_then_newest()
-> Result<(), Box<dyn std::error::Error>> {
    let old = deployment("old", DeploymentPhase::Ready, 1_000)?;
    let active = deployment("active", DeploymentPhase::PendingReady, 2_000)?;
    let newest = deployment("newest", DeploymentPhase::Draining, 3_000)?;
    let crashed = deployment("crashed", DeploymentPhase::Crashed, 4_000)?;
    let deployments = vec![old, active.clone(), newest.clone(), crashed];

    assert_eq!(
        select_deployment(
            deployments.clone(),
            None,
            Some(&DeploymentId::new("active")?),
        )?
        .meta
        .id,
        active.meta.id
    );
    assert_eq!(
        select_deployment(deployments.clone(), None, None)?.meta.id,
        newest.meta.id
    );
    assert!(select_deployment(deployments, Some(&DeploymentId::new("crashed")?), None,).is_err());
    Ok(())
}

#[test]
fn assignment_selection_filters_targets_and_prompts_for_ambiguous_replicas()
-> Result<(), Box<dyn std::error::Error>> {
    let assignments = vec![
        assignment("assignment-0", 0, "node-a", AssignmentPhase::Running)?,
        assignment("assignment-1", 1, "node-b", AssignmentPhase::Running)?,
        assignment("assignment-old", 2, "node-a", AssignmentPhase::Stopped)?,
    ];
    let mut input = std::io::Cursor::new(b"2\n".to_vec());
    let mut output = Vec::new();
    let selected = select_assignment(
        assignments.clone(),
        None,
        None,
        SelectionInteraction::Interactive,
        &mut input,
        &mut output,
    )?;
    assert_eq!(selected.meta.id, AssignmentId::new("assignment-1")?);
    assert!(String::from_utf8(output)?.contains("replica #1 on node-b"));

    let selected = select_assignment(
        assignments,
        Some(0),
        Some(&NodeId::new("node-a")?),
        SelectionInteraction::NonInteractive,
        &mut std::io::empty(),
        &mut Vec::new(),
    )?;
    assert_eq!(selected.meta.id, AssignmentId::new("assignment-0")?);
    let mut non_interactive_output = Vec::new();
    let error = select_assignment(
        vec![
            assignment("assignment-0b", 0, "node-a", AssignmentPhase::Running)?,
            assignment("assignment-1b", 1, "node-b", AssignmentPhase::Running)?,
        ],
        None,
        None,
        SelectionInteraction::NonInteractive,
        &mut std::io::empty(),
        &mut non_interactive_output,
    )
    .expect_err("non-interactive selection must reject an ambiguous target");
    assert!(
        matches!(
            error,
            CliError::InvalidInput { ref message }
                if message == "multiple running replicas match; use --replica or --node when piping input"
        ),
        "unexpected selection error: {error}"
    );
    assert!(non_interactive_output.is_empty());
    Ok(())
}

#[test]
fn exec_endpoint_uses_websocket_tls_and_json_argv() -> Result<(), Box<dyn std::error::Error>> {
    let context = Context {
        host: "https://maestro.example:3443".to_owned(),
        token: Some(SecretValue::new("secret")),
        ca_certificate_pem: None,
    };
    let endpoint = exec_endpoint(
        &context,
        &ServiceId::new("api")?,
        &DeploymentId::new("api-v1")?,
        &AssignmentId::new("api-v1-0")?,
        &["/bin/echo".to_owned(), "hello world".to_owned()],
        Some(TerminalSize {
            columns: 120,
            rows: 40,
        }),
    )?;
    assert_eq!(endpoint.scheme(), "wss");
    assert_eq!(
        endpoint.path(),
        "/api/services/api/deployments/api-v1/assignments/api-v1-0/exec"
    );
    let query = endpoint.query_pairs().collect::<BTreeMap<_, _>>();
    assert_eq!(query.get("tty").map(|value| value.as_ref()), Some("true"));
    assert_eq!(
        query.get("columns").map(|value| value.as_ref()),
        Some("120")
    );
    assert_eq!(
        query
            .get("command")
            .map(|value| serde_json::from_str::<Vec<String>>(value))
            .transpose()?,
        Some(vec!["/bin/echo".to_owned(), "hello world".to_owned()])
    );
    Ok(())
}

fn deployment(
    id: &str,
    phase: DeploymentPhase,
    created_at: i64,
) -> Result<Deployment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(DeploymentId::new(id)?),
        spec: DeploymentSpec {
            service_id: ServiceId::new("api")?,
            service_generation: Generation(1),
            restart_generation: Generation(1),
            bypass_rollout_freeze: false,
            service: service_spec(),
            environment_template: Default::default(),
            goal: DeploymentGoal::Run,
            build_id: None,
        },
        status: DeploymentStatus {
            phase,
            created_at: Timestamp(created_at),
            ready_at: None,
            draining_at: None,
            image_digest: None,
            git_commit: None,
            resolved_secrets: Default::default(),
            conditions: Vec::new(),
        },
    })
}

fn assignment(
    id: &str,
    replica_index: u32,
    node_id: &str,
    phase: AssignmentPhase,
) -> Result<Assignment, kernel_api::InvalidIdentifier> {
    Ok(Object {
        meta: metadata(AssignmentId::new(id)?),
        spec: AssignmentSpec {
            service_id: ServiceId::new("api")?,
            deployment_id: DeploymentId::new("api-v1")?,
            restart_generation: Generation(1),
            replica_index,
            node_id: NodeId::new(node_id)?,
            placement_epoch: 1,
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 80, 0, 10))),
            replaces_assignment_id: None,
        },
        status: AssignmentStatus {
            phase,
            workload_id: Some(WorkloadId::new(id)?),
            workload_address: Some(IpAddr::V4(Ipv4Addr::new(10, 80, 0, 10))),
            conditions: Vec::new(),
        },
    })
}

fn service_spec() -> ServiceSpec {
    ServiceSpec {
        name: "API".to_owned(),
        version: "1.0.0".to_owned(),
        artifact: kernel_api::ArtifactTemplate::Image {
            reference: "registry.test/api:latest".to_owned(),
        },
        preview: None,
        command: None,
        replicas: 2,
        exposed_ports: Vec::new(),
        health_check: None,
        max_restarts: None,
        environment: BTreeMap::new(),
        environment_sources: Vec::new(),
        user: None,
        node_api: NodeApiAccess::Disabled,
        secrets: None,
        volumes: Vec::new(),
        placement: PlacementConstraint::default(),
        exec: ExecPolicy::Allowed,
    }
}

fn metadata<Id>(id: Id) -> ObjectMeta<Id> {
    ObjectMeta {
        id,
        labels: BTreeMap::new(),
        annotations: BTreeMap::new(),
        revision: ResourceRevision::default(),
        generation: Generation(1),
        owner_refs: Vec::new(),
        finalizers: BTreeSet::new(),
        deletion_timestamp: None,
    }
}
