import type { ApiSchemas } from "@maestro/api-client";

type SecretMountSpec = ApiSchemas["SecretMountSpec"];

export function secretNames(secrets: SecretMountSpec | null | undefined): string[] {
  if (!secrets) return [];
  const values = secrets.format === "dotenv" ? (secrets.items ?? {}) : secrets.files;
  return Object.keys(values).sort();
}
