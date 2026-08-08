import type { Assignment, DnsRecord } from "./types";

type ReplicaEndpoint = {
  hostname: string;
};

function replicaDnsLabel(serviceId: string, replicaIndex: number): string {
  const suffix = `-${String(replicaIndex)}`;
  return `${serviceId.replace(/[._]/g, "-").slice(0, 63 - suffix.length)}${suffix}`;
}

function replicaEndpoint(
  serviceId: string,
  replicaIndex: number,
  records: DnsRecord[]
): ReplicaEndpoint | null {
  const label = replicaDnsLabel(serviceId, replicaIndex);
  const matches = records.filter((record) => {
    const hostname = record.spec.name.replace(/\.$/, "");
    return hostname === label || hostname.startsWith(`${label}.`);
  });
  const first = matches[0];
  if (!first) return null;

  return {
    hostname: first.spec.name.replace(/\.$/, "")
  };
}

function assignmentWorkloadAddress(assignment: Assignment | undefined): string | null {
  return assignment?.status.workloadAddress ?? assignment?.spec.workloadAddress ?? null;
}

export { assignmentWorkloadAddress, replicaDnsLabel, replicaEndpoint };
export type { ReplicaEndpoint };
