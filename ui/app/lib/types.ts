import type { ApiSchemas } from "@maestro/api-client";

export type Webhook = ApiSchemas["Webhook"];
export type WebhookEvent = ApiSchemas["WebhookEvent"];
export type ClusterSummary = ApiSchemas["ClusterInfo"];
export type UpgradeRun = ApiSchemas["UpgradeRun"];

export interface ClusterNode {
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
    drainedAtMs?: number | null;
    reason?: string | null;
  };
}

export type UnschedulableReplica = ApiSchemas["UnschedulableReplica"];

export type ClusterStats = ApiSchemas["ClusterStatsResponse"];
export type ControllerStats = ApiSchemas["ControllerStatsSnapshot"];
export type SinkStats = ApiSchemas["SinkStatsSnapshot"];
export type BackupStats = ApiSchemas["BackupStatsSnapshot"];
export type StatsWarning = ApiSchemas["StatsWarning"];
export type StatsMetricPoint = ApiSchemas["StatsMetricPoint"];

export type MaskedConfig = ApiSchemas["MaskedClusterConfig"];
