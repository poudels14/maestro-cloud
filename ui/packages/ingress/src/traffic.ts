import type { LogHistogram } from "@maestro/logs";
import type { TrafficBreakdownEntry } from "./api";

interface TrafficStatus {
  statusCode: number;
  requests: number;
}

interface TrafficGroup {
  value: string;
  requests: number;
  lastSeenAtMs: number;
  statuses: TrafficStatus[];
}

function histogramRequestTotal(histogram: LogHistogram) {
  return histogram.buckets.reduce((total, bucket) => total + bucket.count, 0);
}

function statusClassSummary(statuses: TrafficStatus[]) {
  const classes = new Map<number, number>();
  for (const status of statuses) {
    const statusClass = Math.floor(status.statusCode / 100);
    classes.set(statusClass, (classes.get(statusClass) ?? 0) + status.requests);
  }
  return Array.from(classes.entries())
    .sort(([left], [right]) => left - right)
    .map(([statusClass, requests]) => ({
      label: `${statusClass}xx`,
      statusCode: statusClass * 100,
      requests
    }));
}

function statusCodeSummary(statuses: TrafficStatus[]) {
  const codes = new Map<number, number>();
  for (const status of statuses) {
    codes.set(status.statusCode, (codes.get(status.statusCode) ?? 0) + status.requests);
  }
  return Array.from(codes.entries())
    .sort(([left], [right]) => left - right)
    .map(([statusCode, requests]) => ({ statusCode, requests }));
}

function groupEntries(entries: TrafficBreakdownEntry[]): TrafficGroup[] {
  const groups = new Map<string, TrafficGroup>();
  for (const entry of entries) {
    const group = groups.get(entry.value) ?? {
      value: entry.value,
      requests: 0,
      lastSeenAtMs: 0,
      statuses: []
    };
    group.requests += entry.requests;
    group.lastSeenAtMs = Math.max(group.lastSeenAtMs, entry.lastSeenAtMs);
    group.statuses.push({ statusCode: entry.statusCode, requests: entry.requests });
    groups.set(entry.value, group);
  }
  return Array.from(groups.values()).sort(
    (left, right) =>
      right.requests - left.requests ||
      right.lastSeenAtMs - left.lastSeenAtMs ||
      left.value.localeCompare(right.value)
  );
}

function ipLogQuery(ip: string): string {
  if (/^[^\s:()[\]"]+$/.test(ip)) return `@maestro.client_ip:${ip}`;
  return `@maestro.client_ip:"${ip.replaceAll("\\", "\\\\").replaceAll('"', '\\"')}"`;
}

function statusColor(status: number): string {
  if (status >= 500) return "bg-red-50 text-red-700";
  if (status >= 400) return "bg-amber-50 text-amber-700";
  if (status >= 300) return "bg-brand-light text-brand-hover";
  if (status >= 200) return "bg-emerald-50 text-emerald-700";
  return "bg-gray-100 text-gray-600";
}

export {
  groupEntries,
  histogramRequestTotal,
  ipLogQuery,
  statusClassSummary,
  statusCodeSummary,
  statusColor
};
export type { TrafficGroup, TrafficStatus };
