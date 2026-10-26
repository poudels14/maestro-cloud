export type BuildCommand = {
  command: string;
  args: string[];
};

export type EnvConfig = {
  source?: string | null;
  items?: Record<string, string>;
};

export type Build = {
  repo?: string | null;
  branch?: string | null;
  dockerfile: string;
  watch?: boolean;
  registry?: string | null;
  depot?: { project: string } | null;
  env?: EnvConfig;
  secrets?: EnvConfig;
};

export type Ingress = {
  host?: string | null;
  hosts?: string[];
  port: number;
  sessionAffinity?: { header: string } | null;
};

export type IngressBlocklist = {
  blockedIps: string[];
};

export namespace Routes {
  export type Backend = {
    replicaId: string;
    address: string;
  };

  export type Route = {
    hosts: string[];
    backends: Backend[];
    healthPath?: string | null;
    affinityHeader?: string | null;
  };

  export type Table = {
    routes: Record<string, Route>;
    blockedIps: string[];
  };
}

export type SecretsConfig = {
  mountPath: string;
  source?: string | null;
  items?: Record<string, string>;
};

export type ResourceConfig = {
  cpu?: number | null;
  memory?: string | null;
  request?: { cpu?: number | null; memory?: string | null } | null;
};

export type Deploy = {
  flags?: string[];
  exposePorts?: number[];
  command?: BuildCommand | null;
  healthcheckPath?: string | null;
  healthcheckInterval: number;
  replicas: number;
  exec?: boolean;
  maxRestarts?: number | null;
  env?: EnvConfig;
  secrets?: SecretsConfig | null;
  volumes?: VolumeMount[];
  nodeAffinity?: { node?: string | null; labels?: Record<string, string> } | null;
  egress?: { allow?: { cidr: string; ports?: number[] }[] };
  resources?: ResourceConfig;
  oomPriority?: "low" | "normal" | "high";
};

export type VolumeMount = {
  hostPath: string;
  mountPath: string;
  readOnly?: boolean;
  owner?: VolumeOwner;
};

export type VolumeOwner = {
  uid: number;
  gid?: number;
};

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
    createdAtMs: number;
    closedAtMs?: number | null;
  };
}

export type SlackCategory = "info" | "error";

export type SlackWebhook = {
  id: string;
  name: string;
  url: string;
  categories: SlackCategory[];
  enabled: boolean;
};

export type ServiceSpec = {
  name: string;
  image?: string | null;
  build?: Build | null;
  deploy: Deploy;
  ingress?: Ingress | null;
  preview?: Preview.Config | null;
};

export type DeploymentStatus =
  | "queued"
  | "building"
  | "deploying"
  | "ready"
  | "draining"
  | "stopped"
  | "failed"
  | "cancelled";

export type Service = ServiceSpec & {
  id: string;
  status: DeploymentStatus | "idle";
  latestDeployment?: Deployment | null;
  deployFrozen: boolean;
  replicasOverride?: number | null;
  previewSource?: Preview.Source | null;
  pullRequestUrl?: string | null;
};

export type ReplicaPhase =
  | "starting"
  | "running"
  | "healthy"
  | "unhealthy"
  | "restarting"
  | "failed";

export type ReplicaState = {
  replicaId: string;
  deploymentId: string;
  index: number;
  phase: ReplicaPhase | "pending";
  nodeId?: number | null;
  ip?: string | null;
  restarts: number;
  consecutiveFailures: number;
  lastError?: string | null;
};

export type NodeCapacity = {
  cpu: number;
  memory: string;
};

export type ClusterNode = {
  id: number;
  name: string;
  role: "voter" | "worker";
  endpoint: string;
  subnet?: string | null;
  runsWorkloads: boolean;
  capacity: NodeCapacity;
  reserved: NodeCapacity;
  labels?: Record<string, string>;
  state: "active" | "draining" | "removed";
  available: boolean;
  replicas: number;
  allocated: NodeCapacity;
};

export type UnschedulableReplica = {
  serviceId: string;
  deploymentId: string;
  replicaIndex: number;
  reason: string;
};

export type GitCommitInfo = {
  reference: string;
  message: string;
};

export type Deployment = {
  id: string;
  serviceId: string;
  spec: ServiceSpec;
  status: DeploymentStatus;
  image?: string | null;
  failure?: string | null;
  createdAtMs: number;
  updatedAtMs: number;
  gitCommit?: GitCommitInfo | null;
  builtOn?: number | null;
  replicas: ReplicaState[];
};

export type MetricPoint = {
  ts: number;
  source: string;
  cpuPercent: number;
  memoryBytes: number;
  memoryLimitBytes: number;
  netRxBytes: number;
  netTxBytes: number;
};

export type TrafficPoint = {
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
};

export type TrafficBreakdownEntry = {
  value: string;
  statusCode: number;
  requests: number;
  lastSeenAtMs: number;
};

export type IngressTrafficBreakdown = {
  byIp: TrafficBreakdownEntry[];
  byPath: TrafficBreakdownEntry[];
  partial?: boolean;
  unavailableNodes?: number;
};

export type DiskInfo = {
  name: string;
  mountPoint: string;
  totalBytes: number;
  availableBytes: number;
  fileSystem: string;
};

export namespace Stats {
  export type Sink = {
    id: string;
    cursor: number;
    pendingEntries: number;
    lastSuccessAtMs: number | null;
    lastErrorAtMs: number | null;
    lastError: string | null;
    consecutiveFailures: number;
    deadLetters: number;
    deadLetterCapacity: number;
  };

  export type Backup = {
    configured: boolean;
    lastAttemptAtMs: number | null;
    lastSuccessAtMs: number | null;
    lastErrorAtMs: number | null;
    lastError: string | null;
    pendingPartitions: number;
    completedPartitionsLastRun: number;
    failedPartitionsLastRun: number;
    uploadedBytesLastRun: number;
  };

  export type Node = {
    nodeId: number;
    nodeName: string;
    version: string | null;
    uptimeMs: number | null;
    available: boolean;
    telemetry: boolean;
    sinks: Sink[];
    backup: Backup | null;
  };

  export type Warning = {
    code: string;
    severity: "warning" | "error";
    message: string;
  };

  export type Cluster = {
    generatedAtMs: number;
    term: number;
    leader: number | null;
    nodes: Node[];
    warnings: Warning[];
  };
}

export type LogEntry = {
  seq: number;
  ts: number;
  level: string;
  stream: "stdout" | "stderr";
  text: string;
  message?: string;
  source?: string;
  nodeId?: number;
  nodeName?: string;
  serviceId?: string;
  deploymentId?: string;
  attrs?: [string, string][];
};

export namespace Maintenance {
  export type Phase = "draining" | "requested" | "verifying" | "restoring" | "succeeded" | "failed";

  export type Step = {
    nodeId: number;
    nodeName: string;
    fromVersion?: string | null;
    status: string;
    error?: string | null;
  };

  export type Run = {
    runId: string;
    kind: "upgrade" | "restart";
    targetVersion: string;
    requestedAtMs: number;
    updatedAtMs: number;
    phase: Phase;
    currentNodeIndex: number;
    nodes: Step[];
    failure?: string | null;
  };
}

export type ClusterMember = {
  id: number;
  address: string;
};

export type ClusterInfo = {
  name: string;
  cidr: string;
  version: string;
  nodeId: number;
  term: number;
  leader: ClusterMember | null;
  voters: ClusterMember[];
  learners: ClusterMember[];
  maintenance: Maintenance.Run | null;
  versions: { name: string; version: string }[];
};

export type ClusterConfig = {
  cluster: { name: string; cidr: string };
  node: { labels?: Record<string, string>; reserve: { cpu?: number; memory?: string } };
  ingress: { ports: number[] };
  defaults: {
    resources: {
      cpu: number;
      memory: string;
      request: { cpu: number; memory: string };
    };
  };
  egress: { deny?: string[]; allow?: string[] };
  tags?: string[];
  tailscale?: { authKey: string; advertiseRoutes?: string[] } | null;
  datadog?: {
    apiKey: string;
    site?: string | null;
    includeIngressLogs?: boolean;
    includeTailscaleLogs?: boolean;
    logs?: { includeHealthcheck?: boolean };
    includeMetrics?: boolean;
  } | null;
  depot?: { token: string } | null;
  cloudflare?: { tunnel: { token: string; replicas?: number } } | null;
  slack?: { webhookUrl: string } | null;
  github?: {
    token: string;
    previewDomain: string;
    pollIntervalSecs: number;
    maxConcurrentPreviews?: number | null;
  } | null;
  homepage?: string | null;
  logBackup?: {
    bucket: string;
    kmsKeyId: string;
    region?: string | null;
    prefix?: string | null;
    retentionDays?: number | null;
  } | null;
  allowCliDeployment: boolean;
  allowExec: boolean;
};
