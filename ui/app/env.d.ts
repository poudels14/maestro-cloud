/// <reference types="vite/client" />

interface ImportMetaEnv {
  readonly VITE_MAESTRO_CONTROLLER_HOST: string;
}

interface ImportMeta {
  readonly env: ImportMetaEnv;
}
