export interface BuildCommand {
  command: string;
  args: string[];
}

export interface EnvConfig {
  source?: string | null;
  items?: Record<string, string>;
}

export interface Build {
  repo: string;
  branch?: string | null;
  dockerfile: string;
  watch?: boolean;
  registry?: string | null;
  env?: EnvConfig;
  secrets?: EnvConfig;
}

export interface Ingress {
  host: string;
  port?: number;
}

export interface IngressRouting {
  serviceId: string;
  rule: string;
  entryPoints: string[];
  servers: string[];
}

export interface SecretKeyMeta {
  changed?: boolean;
}

export interface SecretsConfig {
  mountPath: string;
  source?: string | null;
  keys: Record<string, SecretKeyMeta>;
}

export interface Deploy {
  command?: BuildCommand | null;
  healthcheckPath: string;
  replicas?: number;
  env?: EnvConfig;
  secrets?: SecretsConfig | null;
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
}

export interface ReplicaState {
  replicaIndex: number;
  status: string;
}

export interface GitCommitInfo {
  reference: string;
  message: string;
}

export interface Deployment {
  id: string;
  createdAt: number;
  status: string;
  replicas?: ReplicaState[];
  config: Service;
  gitCommit: GitCommitInfo | null;
  build: unknown | null;
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
