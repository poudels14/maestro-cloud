import type { ApiSchemas } from "@maestro/api-client";

type SecretMountSpec = ApiSchemas["SecretMountSpec"];
type ResolvedSecrets = ApiSchemas["DeploymentStatus"]["resolvedSecrets"];

export type SecretEntry = [key: string, maskedValue: string];

export function buildSecretEntries(artifact: ApiSchemas["ArtifactTemplate"]): SecretEntry[] {
  if (artifact.type !== "build") return [];
  return Object.entries(artifact.secrets ?? {}).sort(([left], [right]) =>
    left.localeCompare(right)
  );
}

export function secretNames(secrets: SecretMountSpec | null | undefined): string[] {
  if (!secrets) return [];
  const values = secrets.format === "dotenv" ? (secrets.items ?? {}) : secrets.files;
  return Object.keys(values).sort();
}

export function secretEntries(
  secrets: SecretMountSpec | null | undefined,
  resolvedSecrets?: ResolvedSecrets
): SecretEntry[] {
  if (!secrets) return [];
  const inline = secrets.format === "dotenv" ? (secrets.items ?? {}) : secrets.files;
  return Object.entries({ ...(resolvedSecrets ?? {}), ...inline }).sort(([left], [right]) =>
    left.localeCompare(right)
  );
}

export function secretSourcePath(secrets: SecretMountSpec | null | undefined): string | null {
  if (!secrets || secrets.format !== "dotenv" || !secrets.source) return null;
  return secrets.source.startsWith("aws-secret://")
    ? secrets.source.slice("aws-secret://".length)
    : secrets.source;
}
