import type { ApiSchemas } from "@maestro/api-client";

export interface BuildCommand {
  command: string;
  args: string[];
}

export interface EnvConfig {
  source?: string | null;
  items?: Record<string, string>;
}

export interface Build {
  repo?: string | null;
  branch?: string | null;
  dockerfile: string;
  watch?: boolean;
  registry?: string | null;
  depot?: { project: string } | null;
  env?: EnvConfig;
  secrets?: EnvConfig;
}

export interface Ingress {
  host?: string | null;
  hosts?: string[];
  port?: number | null;
  sessionAffinity?: { header: string } | null;
}

export type IngressBlocklist = ApiSchemas["BlockedIpsResponse"];
export type IngressRouting = ApiSchemas["IngressRouting"];

export interface SecretKeyMeta {
  hash?: string;
  changed: boolean;
}

export interface SecretsConfig {
  mountPath: string;
  source?: string | null;
  items?: Record<string, string>;
  keys?: Record<string, SecretKeyMeta>;
}

export interface Deploy {
  flags?: string[];
  exposePorts?: number[];
  command: BuildCommand | null;
  healthcheckPath?: string | null;
  healthcheckInterval: number;
  replicas: number;
  maxRestarts?: number | null;
  env?: EnvConfig;
  secrets?: SecretsConfig | null;
  volumes?: VolumeMount[];
}

export interface VolumeMount {
  hostPath: string;
  mountPath: string;
  readOnly?: boolean;
  owner?: VolumeOwner;
}

export interface VolumeOwner {
  uid: number;
  gid?: number;
}

export namespace Preview {
  export type Config = {
    enabled: boolean;
    closeGracePeriod: string;
    replicas: number;
    env?: { items?: Record<string, string> };
  };

  export type Source = {
    baseServiceId: string;
    prNumber: number;
    headRef: string;
    headSha: string;
    title: string;
    createdAt: number;
    volumesStripped?: boolean;
    closedAt?: number | null;
  };
}

export type Webhook = ApiSchemas["Webhook"];
export type WebhookEvent = ApiSchemas["WebhookEvent"];
export type ClusterSummary = ApiSchemas["ClusterInfo"];
export type UpgradeRun = ApiSchemas["UpgradeRun"];
export type FirewallPolicy = ApiSchemas["FirewallPolicy"];
export type FirewallPolicySpec = ApiSchemas["FirewallPolicySpec"];
export type FirewallDryRun = ApiSchemas["FirewallDryRunResponse"];

export interface Service {
  id: string;
  name: string;
  version: string;
  status?: string | null;
  build?: Build | null;
  image?: string | null;
  deploy: Deploy;
  ingress?: Ingress | null;
  system?: boolean;
  deployFrozen?: boolean;
  replicasOverride?: number | null;
  preview?: Preview.Config | null;
  previewSource?: Preview.Source | null;
}

export interface ReplicaState {
  replicaIndex: number;
  status: string;
  healthcheckFailures: number;
  restartAttempts: number;
  nodeId?: string | null;
  assignmentId?: string | null;
  endpoint?: {
    containerIp: string;
    containerHostname: string;
    ingressContainerPort: number;
  } | null;
  error?: string | null;
}

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

export interface GitCommitInfo {
  reference: string;
  message: string;
}

export interface Deployment {
  id: string;
  createdAt: number;
  deployedAt?: number | null;
  drainedAt?: number | null;
  status: string;
  replicas?: ReplicaState[];
  config: Service;
  gitCommit: GitCommitInfo | null;
  build: DeploymentBuildInfo | null;
  uploadArchive?: string | null;
}

export interface DeploymentBuildInfo {
  dockerImageId: string;
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

export interface LogEntry {
  seq: number;
  ts: number;
  level: string;
  stream: "stdout" | "stderr";
  text: string;
  source?: string;
  origin?: string;
  hostname?: string;
  nodeId?: string;
  nodeName?: string;
  serviceId?: string;
  tier?: "service" | "system";
  tags?: string[];
  attrs?: [string, string][];
}

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
