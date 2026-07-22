import type { ApiSchemas } from "@maestro/api-client";

export interface LogEntry {
  seq: number;
  ts: number;
  level: string;
  stream: "stdout" | "stderr" | "otlp" | "system";
  text: string;
  source?: string;
  origin: "workload" | "system" | "build" | "unknown";
  hostname?: string;
  nodeId: string;
  serviceId?: string;
  tier: "service" | "system";
  tags?: string[];
  attrs?: [string, string][];
}

type OriginView = Pick<LogEntry, "origin" | "source" | "hostname" | "serviceId" | "tier" | "tags">;

function mapClusterLogEntry(wire: ApiSchemas["ClusterLogEntry"]): LogEntry {
  const origin = mapOrigin(wire.entry.origin);
  const attributes = Object.entries(wire.entry.attributes ?? {}).sort(([left], [right]) =>
    left.localeCompare(right)
  );
  return {
    seq: wire.sequence,
    ts: wire.entry.eventAt,
    level: wire.entry.severity,
    stream: wire.entry.stream,
    text: logBodyText(wire.entry.body),
    nodeId: wire.nodeId,
    ...origin,
    ...(attributes.length > 0 ? { attrs: attributes } : {})
  };
}

function sortLogEntries(entries: LogEntry[]): LogEntry[] {
  return entries.sort(
    (left, right) =>
      left.ts - right.ts || left.nodeId.localeCompare(right.nodeId) || left.seq - right.seq
  );
}

function mapOrigin(value: unknown): OriginView {
  const origin = objectValue(value);
  const type = stringValue(origin?.type);
  if (type === "workload") {
    const metadata = objectValue(origin?.metadata);
    const labels = stringRecord(metadata?.labels);
    const serviceId = stringValue(metadata?.serviceId);
    const workloadId = stringValue(metadata?.workloadId);
    const hostname = labels.hostname ?? labels["maestro.hostname"];
    const tags = Object.entries(labels).map(([name, label]) => `${name}:${label}`);
    return {
      origin: "workload",
      tier: "service",
      ...(serviceId ? { serviceId } : {}),
      ...(workloadId ? { source: `${workloadId}/deploy` } : {}),
      ...(hostname ? { hostname } : {}),
      ...(tags.length > 0 ? { tags } : {})
    };
  }
  if (type === "system") {
    const component = stringValue(origin?.component);
    return {
      origin: "system",
      tier: "system",
      ...(component ? { source: component } : {})
    };
  }
  if (type === "build") {
    const buildId = stringValue(origin?.buildId);
    return {
      origin: "build",
      tier: "system",
      ...(buildId ? { source: `${buildId}/build` } : {})
    };
  }
  return { origin: "unknown", tier: "system" };
}

function logBodyText(value: unknown): string {
  const body = objectValue(value);
  const type = stringValue(body?.type);
  if (type === "text" && typeof body?.value === "string") return body.value;
  if (type === "bytes" && Array.isArray(body?.value)) {
    const bytes = body.value.filter(
      (byte): byte is number => Number.isInteger(byte) && byte >= 0 && byte <= 255
    );
    return new TextDecoder().decode(Uint8Array.from(bytes));
  }
  return stringifyUnknown(value);
}

function objectValue(value: unknown): Record<string, unknown> | undefined {
  if (value == null || typeof value !== "object" || Array.isArray(value)) return undefined;
  return value as Record<string, unknown>;
}

function stringValue(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

function stringRecord(value: unknown): Record<string, string> {
  const record = objectValue(value);
  if (!record) return {};
  return Object.fromEntries(
    Object.entries(record).filter(
      (entry): entry is [string, string] => typeof entry[1] === "string"
    )
  );
}

function stringifyUnknown(value: unknown): string {
  if (typeof value === "string") return value;
  try {
    return JSON.stringify(value);
  } catch {
    return String(value);
  }
}

export { mapClusterLogEntry, sortLogEntries };
export type { OriginView };
