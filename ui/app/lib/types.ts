export interface BuildCommand {
  command: string;
  args: string[];
}

export interface Build {
  repo: string;
  branch?: string | null;
  dockerfilePath: string;
}

export interface Ingress {
  host: string;
  port?: number;
}

export interface SecretKeyMeta {
  hash: string;
  prevHash?: string | null;
}

export interface SecretsConfig {
  mountPath: string;
  keys: Record<string, SecretKeyMeta>;
}

export interface Deploy {
  command?: BuildCommand | null;
  healthcheckPath: string;
  replicas?: number;
  env?: Record<string, string>;
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

export interface LogEntry {
  ts: number;
  level: string;
  stream: "stdout" | "stderr";
  text: string;
  hostname?: string;
  source?: string;
}
