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

export interface IngressBlocklist {
  blockedIps: string[];
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
  role: "voter" | "worker";
  scheduling: boolean;
  clusterHostIp: string;
  clusterApiPort: number;
  subnet: string;
  dataPlaneReady: boolean;
  dataPlaneError?: string | null;
  version: string;
  alive: boolean;
  lastSeenAtMs: number;
  lostAtMs?: number | null;
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

export type UpgradePhase =
  | "draining"
  | "awaiting-leadership-transfer"
  | "upgrade-requested"
  | "self-restart-pending"
  | "verifying"
  | "restoring"
  | "succeeded"
  | "failed";

export interface UpgradeNodeStep {
  nodeId: string;
  hostname: string;
  role: "voter" | "worker";
  fromVersion: string;
  fromInstanceId?: string | null;
  status: "pending" | "draining" | "upgrading" | "verifying" | "restoring" | "succeeded" | "failed";
  startedAtMs?: number | null;
  completedAtMs?: number | null;
  upgradeStartedAtMs?: number | null;
  error?: string | null;
}

export interface UpgradeEvent {
  atMs: number;
  phase: UpgradePhase;
  nodeId?: string | null;
  message: string;
}

export interface UpgradeRun {
  runId: string;
  kind?: "upgrade" | "restart";
  targetVersion: string;
  requestedAtMs: number;
  updatedAtMs: number;
  requestedByNodeId: string;
  phase: UpgradePhase;
  currentNodeIndex: number;
  nodes: UpgradeNodeStep[];
  history: UpgradeEvent[];
  failure?: string | null;
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

export interface TrafficBreakdownEntry {
  value: string;
  statusCode: number;
  requests: number;
  lastSeenAtMs: number;
}

export interface IngressTrafficBreakdown {
  byIp: TrafficBreakdownEntry[];
  byPath: TrafficBreakdownEntry[];
  partial?: boolean;
  unavailableNodes?: number;
}

export interface DiskInfo {
  name: string;
  mountPoint: string;
  totalBytes: number;
  availableBytes: number;
  fileSystem: string;
}

export interface ClusterStats {
  generatedAtMs: number;
  probe: {
    version: string;
    uptimeMs: number;
    storageMode: string;
  };
  controller: ControllerStats | null;
  controllerHeartbeatAgeMs: number | null;
  backup: BackupStats;
  warnings: StatsWarning[];
}

export interface ControllerStats {
  reportedAtMs: number;
  version: string;
  uptimeMs: number;
  spool: {
    rowCount: number;
    highWatermark: number;
    oldestEntryAtMs: number | null;
    databaseBytes: number;
  };
  sinks: SinkStats[];
  deadLetters: {
    count: number;
    capacity: number;
    payloadBytes: number;
    latestAtMs: number | null;
    latestStatus: number | null;
    latestError: string | null;
  };
}

export interface SinkStats {
  id: string;
  cursor: number;
  pendingEntries: number;
  oldestPendingAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastErrorAtMs: number | null;
  lastError: string | null;
  consecutiveFailures: number;
  lastCursorAdvanceAtMs: number | null;
  filteredEntries: number;
}

export interface BackupStats {
  configured: boolean;
  lastAttemptAtMs: number | null;
  lastSuccessAtMs: number | null;
  lastErrorAtMs: number | null;
  lastError: string | null;
  pendingPartitions: number;
  pendingBytes: number;
  oldestPendingDate: string | null;
  uploadedBytesLastRun: number;
  completedPartitionsLastRun: number;
  failedPartitionsLastRun: number;
}

export interface StatsWarning {
  code: string;
  severity: "warning" | "error";
  message: string;
}

export interface StatsMetricPoint {
  ts: number;
  name: string;
  value: number;
  labels?: Record<string, string>;
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
  "disable-etcd-cert": boolean;
  "allow-cli-deployment": boolean;
};
