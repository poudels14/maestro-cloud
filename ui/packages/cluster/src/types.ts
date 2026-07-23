import type { ApiSchemas } from "@maestro/api-client";

type Webhook = ApiSchemas["Webhook"];
type WebhookCategory = ApiSchemas["WebhookCategory"];
type WebhookEvent = ApiSchemas["WebhookEvent"];
type WebhookFormat = ApiSchemas["WebhookFormat"];
type ClusterSummary = ApiSchemas["ClusterInfo"];
type UpgradeRun = ApiSchemas["UpgradeRun"];
type UnschedulableReplica = ApiSchemas["UnschedulableReplica"];
type ClusterStats = ApiSchemas["ClusterStatsResponse"];
type ControllerStats = ApiSchemas["ControllerStatsSnapshot"];
type SinkStats = ApiSchemas["SinkStatsSnapshot"];
type BackupStats = ApiSchemas["BackupStatsSnapshot"];
type StatsWarning = ApiSchemas["StatsWarning"];
type StatsMetricPoint = ApiSchemas["StatsMetricPoint"];
type MaskedConfig = ApiSchemas["MaskedClusterConfig"];
type NodeJoinApproval = ApiSchemas["NodeJoinApproval"];
type NodeJoinApprovalRequest = ApiSchemas["NodeJoinApprovalRequest"];

interface ClusterNode {
  nodeId: string;
  hostname: string;
  role: ApiSchemas["NodeRole"];
  hostAddress: string;
  subnet: string;
  dataPlaneReady: boolean;
  dataPlaneError?: string | null;
  version: string;
  alive: boolean;
  lastSeenAtMs: number;
  revision: number;
  state: {
    unschedulable: boolean;
    drainPending?: boolean;
    drainedAtMs?: number | null;
    reason?: string | null;
  };
}

interface ClusterInfo extends ClusterSummary {
  nodes: ClusterNode[];
  activeUpgrade: UpgradeRun | null;
}

interface WebhookCreateRequest {
  id: string;
  name: string;
  endpoint: string;
  events: WebhookEvent[];
  categories: WebhookCategory[];
  enabled: boolean;
  format: WebhookFormat;
  signingSecret?: string;
}

interface WebhookUpdateRequest {
  name: string;
  endpoint?: string;
  events: WebhookEvent[];
  categories: WebhookCategory[];
  enabled: boolean;
  format: WebhookFormat;
  signingSecret?: string;
}

export type {
  BackupStats,
  ClusterInfo,
  ClusterNode,
  ClusterStats,
  ClusterSummary,
  ControllerStats,
  MaskedConfig,
  NodeJoinApproval,
  NodeJoinApprovalRequest,
  SinkStats,
  StatsMetricPoint,
  StatsWarning,
  UnschedulableReplica,
  UpgradeRun,
  Webhook,
  WebhookCategory,
  WebhookCreateRequest,
  WebhookEvent,
  WebhookFormat,
  WebhookUpdateRequest
};
