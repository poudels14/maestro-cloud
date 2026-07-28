use serde_json::{Map, Value, json};

pub(crate) fn paths() -> Map<String, Value> {
    Map::from_iter([
        ("/api/cluster/ca".to_string(), discovery_operation()),
        ("/api/cluster/join".to_string(), join_operation()),
    ])
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.extend(Map::from_iter([
        (
            "CaDiscoveryRequest".to_string(),
            object_schema(
                &["clusterName", "nonce"],
                json!({
                    "clusterName": {"type": "string"},
                    "nonce": {"type": "string", "pattern": "^[0-9a-f]{64}$"}
                }),
            ),
        ),
        (
            "CaDiscoveryResponse".to_string(),
            object_schema(
                &["clusterId", "caCertificatePem", "proof"],
                json!({
                    "clusterId": {"$ref": "#/components/schemas/ClusterId"},
                    "caCertificatePem": {"type": "string"},
                    "proof": {"$ref": "#/components/schemas/RequestSignature"}
                }),
            ),
        ),
        (
            "RequestSignature".to_string(),
            json!({"type": "string", "pattern": "^[0-9a-f]{64}$"}),
        ),
        (
            "ClusterPorts".to_string(),
            object_schema(
                &[
                    "formatVersion",
                    "gateway",
                    "storeClient",
                    "storePeer",
                    "wireguard",
                ],
                json!({
                    "formatVersion": {"type": "integer", "format": "uint8", "minimum": 1},
                    "gateway": port_schema(),
                    "storeClient": port_schema(),
                    "storePeer": port_schema(),
                    "wireguard": port_schema()
                }),
            ),
        ),
        (
            "NodeEndpoint".to_string(),
            object_schema(
                &["hostAddress", "apiPort"],
                json!({
                    "hostAddress": {"type": "string", "format": "ipv4"},
                    "apiPort": port_schema()
                }),
            ),
        ),
        (
            "JoinRequest".to_string(),
            object_schema(
                &[
                    "clusterId",
                    "clusterName",
                    "nodeId",
                    "hostname",
                    "role",
                    "endpoint",
                    "workloadSubnet",
                    "ports",
                    "joinerPublicKey",
                    "timestampUnixMs",
                    "nonce",
                ],
                json!({
                    "clusterId": {"$ref": "#/components/schemas/ClusterId"},
                    "clusterName": {"type": "string"},
                    "nodeId": {"$ref": "#/components/schemas/NodeId"},
                    "hostname": {"type": "string"},
                    "role": {"$ref": "#/components/schemas/NodeRole"},
                    "endpoint": {"$ref": "#/components/schemas/NodeEndpoint"},
                    "workloadSubnet": {"type": "string", "format": "ipv4-cidr"},
                    "ports": {"$ref": "#/components/schemas/ClusterPorts"},
                    "joinerPublicKey": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    "timestampUnixMs": {"type": "integer", "format": "int64"},
                    "nonce": {"type": "string", "pattern": "^[0-9a-f]{32}$"}
                }),
            ),
        ),
        (
            "SignedJoinRequest".to_string(),
            object_schema(
                &["request", "signature"],
                json!({
                    "request": {"$ref": "#/components/schemas/JoinRequest"},
                    "signature": {"$ref": "#/components/schemas/RequestSignature"}
                }),
            ),
        ),
        (
            "EncryptedJoinResponse".to_string(),
            object_schema(
                &["clusterId", "leaderPublicKey", "nonce", "ciphertext"],
                json!({
                    "clusterId": {"$ref": "#/components/schemas/ClusterId"},
                    "leaderPublicKey": {"type": "string", "pattern": "^[0-9a-f]{64}$"},
                    "nonce": {"type": "string", "format": "byte"},
                    "ciphertext": {"type": "string", "format": "byte"}
                }),
            ),
        ),
    ]));
}

fn discovery_operation() -> Value {
    public_post_operation(
        "discoverClusterCa",
        "CaDiscoveryRequest",
        "CaDiscoveryResponse",
        "Authenticated cluster trust root",
    )
}

fn join_operation() -> Value {
    public_post_operation(
        "joinCluster",
        "SignedJoinRequest",
        "EncryptedJoinResponse",
        "Encrypted node-specific cluster grant",
    )
}

fn public_post_operation(
    operation_id: &str,
    request_schema: &str,
    response_schema: &str,
    response_description: &str,
) -> Value {
    json!({
        "post": {
            "operationId": operation_id,
            "requestBody": {
                "required": true,
                "content": {"application/json": {"schema": {
                    "$ref": format!("#/components/schemas/{request_schema}")
                }}}
            },
            "responses": {
                "200": {
                    "description": response_description,
                    "content": {"application/json": {"schema": {
                        "$ref": format!("#/components/schemas/{response_schema}")
                    }}}
                },
                "400": {"description": "Malformed or topology-mismatched request"},
                "403": {"description": "Join authentication or source address was rejected"},
                "409": {"description": "The configured node already admitted another request"},
                "503": {"description": "Cluster admission is unavailable on this node"}
            }
        }
    })
}

fn object_schema(required: &[&str], properties: Value) -> Value {
    json!({
        "type": "object",
        "additionalProperties": false,
        "required": required,
        "properties": properties
    })
}

fn port_schema() -> Value {
    json!({"type": "integer", "format": "uint16", "minimum": 1, "maximum": 65_535})
}
