use serde_json::{Map, Value, json};

use crate::openapi_commands::{
    command_operation, command_path, deployment_command_path, firewall_dry_run_path,
    firewall_policy_operation, insert_command_schemas, node_operation, webhook_operation,
    webhook_test_path,
};

/// Composes server path operations over the canonical kernel component schemas.
pub fn openapi_document() -> Value {
    let mut document = kernel_api::openapi_document();
    let mut paths = Map::from_iter([
        ("/api/auth/session".to_string(), browser_session_operation()),
        (
            "/api/cluster".to_string(),
            singleton_operation("getClusterInfo", "ClusterInfo"),
        ),
        (
            "/api/config".to_string(),
            singleton_operation("getClusterConfig", "MaskedClusterConfig"),
        ),
        (
            "/api/config/preview".to_string(),
            preview_launch_config_operation(),
        ),
        (
            "/api/cluster/nodes".to_string(),
            list_operation("listNodes", "Node"),
        ),
        (
            "/api/cluster/unschedulable".to_string(),
            list_operation("listUnschedulableReplicas", "UnschedulableReplica"),
        ),
        (
            "/api/cluster/placements".to_string(),
            placement_history_operation(),
        ),
        (
            "/api/artifact-archives/{archiveId}".to_string(),
            artifact_archive_operation(),
        ),
        ("/api/cluster/nodes/{nodeId}".to_string(), node_operation()),
        (
            "/api/cluster/nodes/{nodeId}/drain".to_string(),
            command_path(
                "post",
                "drainNode",
                &["nodeId"],
                "CommandRequest",
                "NodeCommandResponse",
            ),
        ),
        (
            "/api/cluster/nodes/{nodeId}/restore".to_string(),
            command_path(
                "post",
                "restoreNode",
                &["nodeId"],
                "CommandRequest",
                "NodeCommandResponse",
            ),
        ),
        (
            "/api/cluster/upgrades".to_string(),
            upgrade_collection_operation(),
        ),
        (
            "/api/cluster/upgrades/{upgradeRunId}".to_string(),
            upgrade_operation(),
        ),
        (
            "/api/cluster/networks".to_string(),
            list_operation("listNodeNetworks", "NodeNetwork"),
        ),
        (
            "/api/cluster/networks/{networkId}".to_string(),
            get_operation("getNodeNetwork", "networkId", "NodeNetwork"),
        ),
        (
            "/api/cluster/node-firewalls".to_string(),
            list_operation("listNodeFirewalls", "NodeFirewall"),
        ),
        (
            "/api/cluster/node-firewalls/{firewallId}".to_string(),
            get_operation("getNodeFirewall", "firewallId", "NodeFirewall"),
        ),
        (
            "/api/cluster/tailscale/auth-key".to_string(),
            crate::openapi_tailscale::path(),
        ),
        (
            "/api/cluster/dns-records".to_string(),
            list_operation("listDnsRecords", "DnsRecord"),
        ),
        (
            "/api/cluster/dns-records/{recordId}".to_string(),
            get_operation("getDnsRecord", "recordId", "DnsRecord"),
        ),
        (
            "/api/firewall/policies".to_string(),
            list_operation("listFirewallPolicies", "FirewallPolicy"),
        ),
        (
            "/api/firewall/policies/{policyId}".to_string(),
            firewall_policy_operation(),
        ),
        (
            "/api/firewall/policies/{policyId}/dry-run".to_string(),
            firewall_dry_run_path(),
        ),
        (
            "/api/previews".to_string(),
            list_operation("listPreviews", "Preview"),
        ),
        (
            "/api/previews/{previewId}".to_string(),
            get_operation("getPreview", "previewId", "Preview"),
        ),
        (
            "/api/webhooks".to_string(),
            list_operation("listWebhooks", "Webhook"),
        ),
        ("/api/webhooks/{webhookId}".to_string(), webhook_operation()),
        (
            "/api/webhooks/{webhookId}/test".to_string(),
            webhook_test_path(),
        ),
        (
            "/api/services".to_string(),
            list_operation("listServices", "Service"),
        ),
        ("/api/services/{serviceId}".to_string(), service_operation()),
        (
            "/api/services/{serviceId}/diff".to_string(),
            service_diff_operation(),
        ),
        (
            "/api/services/{serviceId}/rollout".to_string(),
            service_rollout_operation(),
        ),
        (
            "/api/services/{serviceId}/rollout/diff".to_string(),
            service_rollout_diff_operation(),
        ),
        (
            "/api/services/{serviceId}/redeploy".to_string(),
            command_path(
                "post",
                "redeployService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/freeze".to_string(),
            command_path(
                "post",
                "freezeService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/unfreeze".to_string(),
            command_path(
                "post",
                "unfreezeService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/replicas".to_string(),
            command_path(
                "put",
                "setServiceReplicas",
                &["serviceId"],
                "ServiceReplicaOverrideRequest",
                "ServiceCommandResponse",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments".to_string(),
            nested_list_operation("listDeployments", &["serviceId"], "Deployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}".to_string(),
            deployment_operation(),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/assignments".to_string(),
            nested_list_operation(
                "listAssignments",
                &["serviceId", "deploymentId"],
                "Assignment",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/assignments/{assignmentId}"
                .to_string(),
            scoped_get_operation(
                "getAssignment",
                &["serviceId", "deploymentId", "assignmentId"],
                "Assignment",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/replicas".to_string(),
            nested_list_operation(
                "listReplicas",
                &["serviceId", "deploymentId"],
                "ReplicaState",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/replicas/{replicaId}".to_string(),
            scoped_get_operation(
                "getReplica",
                &["serviceId", "deploymentId", "replicaId"],
                "ReplicaState",
            ),
        ),
        (
            "/api/services/{serviceId}/builds".to_string(),
            nested_list_operation("listBuilds", &["serviceId"], "Build"),
        ),
        (
            "/api/services/{serviceId}/builds/{buildId}".to_string(),
            scoped_get_operation("getBuild", &["serviceId", "buildId"], "Build"),
        ),
        (
            "/api/services/{serviceId}/routes".to_string(),
            nested_list_operation("listIngressRoutes", &["serviceId"], "IngressRoute"),
        ),
        (
            "/api/services/{serviceId}/routes/{routeId}".to_string(),
            scoped_get_operation("getIngressRoute", &["serviceId", "routeId"], "IngressRoute"),
        ),
        (
            "/api/services/{serviceId}/traffic-generations".to_string(),
            nested_list_operation(
                "listTrafficGenerations",
                &["serviceId"],
                "TrafficGeneration",
            ),
        ),
        (
            "/api/services/{serviceId}/traffic-generations/{generationId}".to_string(),
            scoped_get_operation(
                "getTrafficGeneration",
                &["serviceId", "generationId"],
                "TrafficGeneration",
            ),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/restart".to_string(),
            deployment_command_path("restartDeployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/cancel".to_string(),
            deployment_command_path("cancelDeployment"),
        ),
        (
            "/api/services/{serviceId}/deployments/{deploymentId}/remove".to_string(),
            deployment_command_path("removeDeployment"),
        ),
        ("/healthz".to_string(), health_operation()),
        ("/openapi.json".to_string(), openapi_operation()),
    ]);
    paths.extend(crate::openapi_logs::paths());
    paths.extend(crate::openapi_admission::paths());
    paths.extend(crate::openapi_metrics::paths());
    paths.extend(crate::openapi_stats::paths());
    paths.extend(crate::openapi_traffic::paths());
    let paths = Value::Object(paths);
    let security_schemes = json!({
        "bearerAuth": {
            "type": "http",
            "scheme": "bearer",
            "bearerFormat": "JWT",
            "description": "HS256 token with a non-empty subject and either the read-only or operator scope; used to create browser sessions and by non-browser clients"
        },
        "browserSession": {
            "type": "apiKey",
            "in": "cookie",
            "name": "__Host-maestro-session",
            "description": "Secure, HttpOnly, SameSite=Strict session preserving the bearer token access level and expiration"
        }
    });
    if let Some(root) = document.as_object_mut() {
        root.insert("paths".to_string(), paths);
        if let Some(components) = root.get_mut("components").and_then(Value::as_object_mut) {
            components.insert("securitySchemes".to_string(), security_schemes);
            if let Some(schemas) = components.get_mut("schemas").and_then(Value::as_object_mut) {
                insert_command_schemas(schemas);
                crate::openapi_admission::insert_schemas(schemas);
                crate::openapi_logs::insert_schemas(schemas);
                crate::openapi_metrics::insert_schemas(schemas);
                crate::openapi_stats::insert_schemas(schemas);
                crate::openapi_tailscale::insert_schemas(schemas);
                crate::openapi_traffic::insert_schemas(schemas);
            }
        }
    }
    document
}

fn browser_session_operation() -> Value {
    json!({
        "post": {
            "operationId": "createBrowserSession",
            "security": [{"bearerAuth": []}],
            "responses": {
                "204": {"description": "Secure browser session created"},
                "401": {"description": "Bearer credential is missing or invalid"},
                "403": {"description": "Bearer credential lacks read-only or operator access"},
                "503": {"description": "Operator authentication is disabled on loopback"}
            }
        },
        "delete": {
            "operationId": "deleteBrowserSession",
            "responses": {
                "204": {"description": "Browser session cookie cleared"}
            }
        }
    })
}

fn preview_launch_config_operation() -> Value {
    json!({
        "put": {
            "operationId": "updatePreviewLaunchConfig",
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "requestBody": {
                "required": true,
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/PreviewLaunchConfigUpdateRequest"
                }}}
            },
            "responses": {
                "200": {
                    "description": "Protected local launch document inspected or updated",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/PreviewLaunchConfigUpdateResponse"
                    }}}
                },
                "400": {"description": "Invalid config or request"},
                "409": {"description": "Contacted Admin endpoint does not match the target node"},
                "413": {"description": "Request body exceeds the command limit"},
                "503": {"description": "Node-local launch configuration is unavailable"}
            }
        }
    })
}

fn artifact_archive_operation() -> Value {
    json!({
        "put": {
            "operationId": "uploadArtifactArchive",
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": [{
                "name": "archiveId",
                "in": "path",
                "required": true,
                "schema": {"$ref": "#/components/schemas/ArtifactArchiveId"}
            }],
            "requestBody": {
                "required": true,
                "content": {
                    "application/gzip": {
                        "schema": {"type": "string", "format": "binary"}
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Identical archive already stored",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/ArtifactArchiveUploadResponse"
                    }}}
                },
                "201": {
                    "description": "Archive stored",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/ArtifactArchiveUploadResponse"
                    }}}
                },
                "400": {"description": "Invalid archive content or content address"},
                "413": {"description": "Archive exceeds the upload limit"},
                "503": {"description": "Archive storage is unavailable on this node"}
            }
        }
    })
}

fn singleton_operation(operation_id: &str, schema: &str) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "responses": {
                "200": {
                    "description": "Resource summary",
                    "content": {"application/json": {"schema": {
                        "$ref": format!("#/components/schemas/{schema}")
                    }}}
                }
            }
        }
    })
}

fn upgrade_collection_operation() -> Value {
    let mut operation = list_operation("listUpgrades", "UpgradeRun");
    if let Some(item) = operation.as_object_mut() {
        item.insert(
            "post".to_string(),
            command_operation(
                "startUpgrade",
                &[],
                "UpgradeCreateRequest",
                "UpgradeCommandResponse",
            ),
        );
    }
    operation
}

fn upgrade_operation() -> Value {
    let mut operation = get_operation("getUpgrade", "upgradeRunId", "UpgradeRun");
    if let Some(item) = operation.as_object_mut() {
        item.insert(
            "delete".to_string(),
            command_operation(
                "cancelUpgrade",
                &["upgradeRunId"],
                "CommandRequest",
                "UpgradeCommandResponse",
            ),
        );
    }
    operation
}

fn service_operation() -> Value {
    let mut operation = get_operation("getService", "serviceId", "Service");
    if let Some(item) = operation.as_object_mut() {
        item.insert("put".to_string(), put_service_operation());
        item.insert(
            "delete".to_string(),
            command_operation(
                "deleteService",
                &["serviceId"],
                "CommandRequest",
                "ServiceCommandResponse",
            ),
        );
    }
    operation
}

fn put_service_operation() -> Value {
    json!({
        "operationId": "putService",
        "security": [{"bearerAuth": []}, {"browserSession": []}],
        "parameters": [
            {
                "name": "serviceId",
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            },
            {
                "name": "Idempotency-Key",
                "in": "header",
                "required": true,
                "schema": {"type": "string"}
            }
        ],
        "requestBody": {
            "required": true,
            "content": {
                "application/json": {
                    "schema": {"$ref": "#/components/schemas/ServiceWriteRequest"}
                }
            }
        },
        "responses": {
            "202": {
                "description": "Desired state accepted for reconciliation",
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ServiceWriteResponse"}
                    }
                }
            },
            "400": {"description": "Invalid service request"},
            "409": {"description": "Revision or idempotency conflict"},
            "413": {"description": "Request body exceeds the service limit"}
        }
    })
}

fn service_diff_operation() -> Value {
    json!({
        "post": {
            "operationId": "diffService",
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": [{
                "name": "serviceId",
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }],
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ServiceDiffRequest"}
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Masked desired-state comparison",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ServiceDiffResponse"}
                        }
                    }
                },
                "400": {"description": "Invalid service request"},
                "413": {"description": "Request body exceeds the service limit"}
            }
        }
    })
}

fn service_rollout_operation() -> Value {
    json!({
        "post": {
            "operationId": "applyServiceRollout",
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": [
                {
                    "name": "serviceId",
                    "in": "path",
                    "required": true,
                    "schema": {"type": "string"}
                },
                {
                    "name": "Idempotency-Key",
                    "in": "header",
                    "required": true,
                    "schema": {"type": "string"}
                }
            ],
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ServiceRolloutRequest"}
                    }
                }
            },
            "responses": {
                "202": {
                    "description": "Atomic desired resource set accepted",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ServiceRolloutResponse"}
                        }
                    }
                },
                "400": {"description": "Invalid rollout request"},
                "409": {"description": "Revision, route, policy, or idempotency conflict"},
                "413": {"description": "Request body exceeds the service limit"}
            }
        }
    })
}

fn service_rollout_diff_operation() -> Value {
    json!({
        "post": {
            "operationId": "diffServiceRollout",
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": [{
                "name": "serviceId",
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }],
            "requestBody": {
                "required": true,
                "content": {
                    "application/json": {
                        "schema": {"$ref": "#/components/schemas/ServiceRolloutDiffRequest"}
                    }
                }
            },
            "responses": {
                "200": {
                    "description": "Masked atomic rollout comparison",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/ServiceRolloutDiffResponse"}
                        }
                    }
                },
                "400": {"description": "Invalid rollout request"},
                "409": {"description": "Route or policy ownership conflict"},
                "413": {"description": "Request body exceeds the service limit"}
            }
        }
    })
}

fn list_operation(operation_id: &str, schema: &str) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "responses": {
                "200": {
                    "description": "Ordered resource list",
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "array",
                                "items": {"$ref": format!("#/components/schemas/{schema}")}
                            }
                        }
                    }
                }
            }
        }
    })
}

fn placement_history_operation() -> Value {
    let mut operation = list_operation("listPlacementHistory", "PlacementHistory");
    if let Some(get) = operation.get_mut("get").and_then(Value::as_object_mut) {
        get.insert(
            "parameters".to_string(),
            json!([
                {
                    "name": "serviceId",
                    "in": "query",
                    "required": false,
                    "schema": {"type": "string"}
                },
                {
                    "name": "deploymentId",
                    "in": "query",
                    "required": false,
                    "schema": {"type": "string"}
                },
                {
                    "name": "replicaIndex",
                    "in": "query",
                    "required": false,
                    "schema": {"type": "integer", "format": "uint32", "minimum": 0}
                }
            ]),
        );
    }
    operation
}

pub(crate) fn get_operation(operation_id: &str, parameter: &str, schema: &str) -> Value {
    json!({
        "get": {
            "operationId": operation_id,
            "security": [{"bearerAuth": []}, {"browserSession": []}],
            "parameters": [{
                "name": parameter,
                "in": "path",
                "required": true,
                "schema": {"type": "string"}
            }],
            "responses": {
                "200": {
                    "description": "Requested resource",
                    "content": {
                        "application/json": {
                            "schema": {"$ref": format!("#/components/schemas/{schema}")}
                        }
                    }
                },
                "404": {"description": "Resource not found"}
            }
        }
    })
}

fn nested_list_operation(operation_id: &str, parameters: &[&str], schema: &str) -> Value {
    let mut operation = list_operation(operation_id, schema);
    if let Some(get) = operation.get_mut("get").and_then(Value::as_object_mut) {
        get.insert("parameters".to_string(), path_parameters(parameters));
    }
    operation
}

fn deployment_operation() -> Value {
    scoped_get_operation(
        "getDeployment",
        &["serviceId", "deploymentId"],
        "Deployment",
    )
}

fn scoped_get_operation(operation_id: &str, parameters: &[&str], schema: &str) -> Value {
    let parameter = parameters.last().copied().unwrap_or("resourceId");
    let mut operation = get_operation(operation_id, parameter, schema);
    if let Some(get) = operation.get_mut("get").and_then(Value::as_object_mut) {
        get.insert("parameters".to_string(), path_parameters(parameters));
    }
    operation
}

fn path_parameters(parameters: &[&str]) -> Value {
    Value::Array(
        parameters
            .iter()
            .map(|parameter| {
                json!({
                    "name": parameter,
                    "in": "path",
                    "required": true,
                    "schema": {"type": "string"}
                })
            })
            .collect(),
    )
}

fn health_operation() -> Value {
    json!({
        "get": {
            "operationId": "health",
            "responses": {"200": {"description": "Server is accepting requests"}}
        }
    })
}

fn openapi_operation() -> Value {
    json!({
        "get": {
            "operationId": "getOpenApi",
            "responses": {"200": {"description": "Canonical OpenAPI document"}}
        }
    })
}
