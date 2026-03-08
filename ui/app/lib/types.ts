export interface BuildCommand {
  command: string;
  args: string[];
}

export interface Build {
  gitRepo: string;
  dockerfilePath: string;
  command: BuildCommand;
}

export interface Deploy {
  command: BuildCommand;
  healthcheckPath: string;
}

export interface Service {
  id: string;
  name: string;
  version: string;
  build: Build;
  deploy: Deploy;
}

export interface Deployment {
  id: string;
  status: string;
  config: Service;
  gitCommit: string | null;
  build: unknown | null;
}
