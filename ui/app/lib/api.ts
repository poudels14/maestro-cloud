import type { Deployment, LogEntry, Service } from "./types";

export async function getServices(): Promise<Service[]> {
  const res = await fetch("/api/services");
  if (!res.ok) throw new Error(`Failed to fetch services: ${res.statusText}`);
  return res.json();
}

export async function getDeployments(serviceId: string): Promise<Deployment[]> {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch deployments: ${res.statusText}`);
  return res.json();
}

export async function deleteService(serviceId: string) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}`, {
    method: "DELETE"
  });
  if (!res.ok) throw new Error(`Failed to delete service: ${res.statusText}`);
}

export async function redeployService(serviceId: string) {
  const res = await fetch(`/api/services/${encodeURIComponent(serviceId)}/redeploy`, {
    method: "POST"
  });
  if (!res.ok) throw new Error(`Failed to redeploy: ${res.statusText}`);
}

export async function cancelDeployment(serviceId: string, deploymentId: string) {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/cancel`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to cancel deployment: ${res.statusText}`);
}

export async function stopDeployment(serviceId: string, deploymentId: string) {
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/remove`;
  const res = await fetch(url, { method: "PATCH" });
  if (!res.ok) throw new Error(`Failed to stop deployment: ${res.statusText}`);
}

export async function getLogs(
  serviceId: string,
  deploymentId: string,
  tail?: number
): Promise<LogEntry[]> {
  const params = tail ? `?tail=${tail}` : "";
  const url = `/api/services/${encodeURIComponent(serviceId)}/deployments/${encodeURIComponent(deploymentId)}/logs${params}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch logs: ${res.statusText}`);
  const raw = await res.json();
  return mapLogEntries(raw);
}

export async function getSystemLogs(name: string, tail?: number): Promise<LogEntry[]> {
  const params = tail ? `?tail=${tail}` : "";
  const url = `/api/system/${encodeURIComponent(name)}/logs${params}`;
  const res = await fetch(url);
  if (!res.ok) throw new Error(`Failed to fetch system logs: ${res.statusText}`);
  const raw = await res.json();
  return mapLogEntries(raw);
}

function mapLogEntries(raw: Record<string, unknown>[]): LogEntry[] {
  return raw.map((entry) => {
    let hostname: string | undefined;
    const tags = entry.tags;
    if (Array.isArray(tags)) {
      const match = tags.find((tag: string) => tag.startsWith("hostname:"));
      if (match) hostname = match.slice("hostname:".length);
    }
    return {
      ts: entry.ts as number,
      level: entry.level as string,
      stream: entry.stream as LogEntry["stream"],
      text: entry.text as string,
      hostname,
      source: entry.source as string | undefined
    };
  });
}
