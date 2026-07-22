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

export interface UnschedulableReplica {
  serviceId: string;
  deploymentId: string;
  replicaIndex: number;
  reason: string;
}

export type MetricPoint = ApiSchemas["ResourceMetricPoint"];

export interface TrafficPoint {
  ts: number;
  serviceId: string;
  deploymentId: string | null;
  statusCode: number;
  method: string;
  requests: number;
  bytesIn: number;
  bytesOut: number;
  latLe1s: number;
  latLe5s: number;
  latLe10s: number;
  latTotal: number;
}

export type TrafficBreakdownEntry = ApiSchemas["TrafficBreakdownEntry"];
export type IngressTrafficBreakdown = ApiSchemas["IngressTrafficBreakdown"];

export type DiskInfo = ApiSchemas["DiskInfo"];
export type ClusterStats = ApiSchemas["ClusterStatsResponse"];
export type ControllerStats = ApiSchemas["ControllerStatsSnapshot"];
export type SinkStats = ApiSchemas["SinkStatsSnapshot"];
export type BackupStats = ApiSchemas["BackupStatsSnapshot"];
export type StatsWarning = ApiSchemas["StatsWarning"];
export type StatsMetricPoint = ApiSchemas["StatsMetricPoint"];

export type MaskedConfig = {
  cluster: {
    name: string;
    nodes: Record<
      string,
      {
        endpoint: string;
        subnet: string;
        role: "master" | "hybrid" | "voter" | "worker";
      }
    >;
  };
  node: {
    name?: string | null;
    role: "master" | "hybrid" | "voter" | "worker";
    "api-port": number;
    "gateway-port": number;
    "etcd-client-port": number;
    "etcd-peer-port": number;
  };
  ingress: { ports: number[] };
  subnet?: string | null;
  egress: { deny: string[]; allow: string[] };
  "encryption-key": string | null;
  tailscale?: { "auth-key": string | null; "advertise-routes": string[] } | null;
  "jwt-secret-key": string | null;
  tags: string[];
  datadog?: {
    "api-key": string | null;
    site?: string | null;
    "include-ingress-logs": boolean;
    "include-tailscale-logs": boolean;
    logs: { "include-healthcheck": boolean };
    "include-metrics": boolean;
  } | null;
  system?: string | null;
  runtime: string;
  depot?: { token: string | null } | null;
  cloudflare?: {
    tunnel: { token: string | null; replicas?: number | null };
  } | null;
  slack?: { "webhook-url": string | null } | null;
  github?: {
    token: string | null;
    "preview-domain": string;
    "poll-interval-secs": number;
    "max-concurrent-previews": number;
  } | null;
  homepage?: string | null;
  "disable-etcd-cert": boolean;
  "allow-cli-deployment": boolean;
};
