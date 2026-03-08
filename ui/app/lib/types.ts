export interface BuildCommand {
  command: string;
  args: string[];
}

export interface Build {
  repo: string;
  dockerfilePath: string;
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
}

export interface Deployment {
  id: string;
  status: string;
  config: Service;
  gitCommit: string | null;
  build: unknown | null;
}
