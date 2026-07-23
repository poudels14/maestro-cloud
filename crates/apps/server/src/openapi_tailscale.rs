use serde_json::{Map, Value, json};

pub(crate) fn path() -> Value {
    json!({
        "get": {
            "operationId": "getTailscaleAuthKeyStatus",
            "security": [{"bearerAuth": []}],
            "responses": {
                "200": {
                    "description": "Secret-free live override status",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/TailscaleAuthKeyStatus"
                    }}}
                },
                "409": {"description": "Managed Tailscale gateways are disabled"},
                "503": {"description": "Cluster configuration is unavailable"}
            }
        },
        "put": {
            "operationId": "rotateTailscaleAuthKey",
            "security": [{"bearerAuth": []}],
            "parameters": [{
                "name": "Idempotency-Key",
                "in": "header",
                "required": true,
                "schema": {"type": "string"}
            }],
            "requestBody": {
                "required": true,
                "content": {"application/json": {"schema": {
                    "$ref": "#/components/schemas/TailscaleAuthKeyRotationRequest"
                }}}
            },
            "responses": {
                "202": {
                    "description": "Secret override durably accepted for fenced reconciliation",
                    "content": {"application/json": {"schema": {
                        "$ref": "#/components/schemas/TailscaleAuthKeyRotationResponse"
                    }}}
                },
                "400": {"description": "Invalid key or request"},
                "409": {"description": "Feature, revision, or idempotency conflict"},
                "413": {"description": "Request body exceeds the command limit"},
                "503": {"description": "Cluster configuration is unavailable"}
            }
        }
    })
}

pub(crate) fn insert_schemas(schemas: &mut Map<String, Value>) {
    schemas.insert(
        "TailscaleAuthKeyStatus".to_string(),
        json!({
            "type": "object",
            "properties": {
                "overrideRevision": {
                    "$ref": "#/components/schemas/ResourceRevision",
                    "description": "Live override revision; absence means the launch-document key is active"
                }
            }
        }),
    );
    schemas.insert(
        "TailscaleAuthKeyRotationRequest".to_string(),
        json!({
            "type": "object",
            "additionalProperties": false,
            "required": ["authKey"],
            "properties": {
                "expectedRevision": {
                    "$ref": "#/components/schemas/ResourceRevision",
                    "description": "Required current override revision; omit only when creating the first override"
                },
                "authKey": {"$ref": "#/components/schemas/SecretValue"}
            }
        }),
    );
    schemas.insert(
        "TailscaleAuthKeyRotationResponse".to_string(),
        json!({
            "type": "object",
            "required": ["requestId"],
            "properties": {
                "requestId": {"type": "string"}
            }
        }),
    );
}
