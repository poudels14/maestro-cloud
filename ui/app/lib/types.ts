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
}

export interface IngressRouting {
  serviceId: string;
  rule: string;
  entryPoints: string[];
  servers: string[];
}

export interface SecretKeyMeta {
  hash: string;
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

export type SlackCategory = "info" | "error";

export interface SlackWebhook {
  id: string;
  name: string;
  url: string;
  categories: SlackCategory[];
  enabled: boolean;
}

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
}

export interface ReplicaState {
  replicaIndex: number;
  status: string;
  healthcheckFailures: number;
  restartAttempts: number;
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

export interface MetricPoint {
  ts: number;
  source: string;
  cpuPercent: number;
  memoryBytes: number;
  memoryLimitBytes: number;
  netRxBytes: number;
  netTxBytes: number;
}

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

export interface DiskInfo {
  name: string;
  mountPoint: string;
  totalBytes: number;
  availableBytes: number;
  fileSystem: string;
}

export interface LogEntry {
  seq: number;
  ts: number;
  level: string;
  stream: "stdout" | "stderr";
  text: string;
  source?: string;
  origin?: string;
  hostname?: string;
  tags?: string[];
  attrs?: [string, string][];
}

export type MaskedConfig = {
  cluster: { name: string };
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
    "include-metrics": boolean;
  } | null;
  system?: string | null;
  runtime: string;
  depot?: { token: string | null } | null;
  cloudflare?: {
    tunnel: { token: string | null; replicas?: number | null };
  } | null;
  slack?: { "webhook-url": string | null } | null;
  "disable-etcd-cert": boolean;
  "allow-cli-deployment": boolean;
};
