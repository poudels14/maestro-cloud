export interface BuildCommand {
  command: string;
  args: string[];
}

export interface Build {
  repo: string;
  dockerfilePath: string;
}

export interface Ingress {
  host: string;
  port?: number;
}

export interface Deploy {
  command?: BuildCommand | null;
  healthcheckPath: string;
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
}

export interface Deployment {
  id: string;
  createdAt: number;
  status: string;
  config: Service;
  gitCommit: string | null;
  build: unknown | null;
}

export interface LogEntry {
  ts: number;
  level: string;
  stream: "stdout" | "stderr";
  text: string;
}
