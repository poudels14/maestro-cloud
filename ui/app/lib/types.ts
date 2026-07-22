import type { ApiSchemas } from "@maestro/api-client";

export type IngressBlocklist = ApiSchemas["BlockedIpsResponse"];
export type IngressRouting = ApiSchemas["IngressRouting"];

export type Webhook = ApiSchemas["Webhook"];
export type WebhookEvent = ApiSchemas["WebhookEvent"];
export type ClusterSummary = ApiSchemas["ClusterInfo"];
export type UpgradeRun = ApiSchemas["UpgradeRun"];
export type FirewallPolicy = ApiSchemas["FirewallPolicy"];
export type FirewallPolicySpec = ApiSchemas["FirewallPolicySpec"];
export type FirewallDryRun = ApiSchemas["FirewallDryRunResponse"];

export type Service = ApiSchemas["Service"] & {
  previewResource?: ApiSchemas["Preview"];
};
export type Deployment = ApiSchemas["Deployment"];
export type ReplicaState = ApiSchemas["ReplicaState"];

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

export type MetricPoint = ApiSchemas["ResourceMetricPoint"];

export type TrafficPoint = ApiSchemas["TrafficMetricPoint"];

export type TrafficBreakdownEntry = ApiSchemas["TrafficBreakdownEntry"];
export type IngressTrafficBreakdown = ApiSchemas["IngressTrafficBreakdown"];

export type DiskInfo = ApiSchemas["DiskInfo"];
export type ClusterStats = ApiSchemas["ClusterStatsResponse"];
export type ControllerStats = ApiSchemas["ControllerStatsSnapshot"];
export type SinkStats = ApiSchemas["SinkStatsSnapshot"];
export type BackupStats = ApiSchemas["BackupStatsSnapshot"];
export type StatsWarning = ApiSchemas["StatsWarning"];
export type StatsMetricPoint = ApiSchemas["StatsMetricPoint"];

export type MaskedConfig = ApiSchemas["MaskedClusterConfig"];
