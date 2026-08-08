import { describe, expect, test } from "vitest";
import type { DnsRecord } from "./types";
import type { Assignment } from "./types";
import { assignmentWorkloadAddress, replicaDnsLabel, replicaEndpoint } from "./replicaEndpoint";

function record(name: string, type: "a" | "aaaa", value: string): DnsRecord {
  return {
    spec: { name, ttlSecs: 30, values: [{ type, value }] }
  } as DnsRecord;
}

describe("replicaEndpoint", () => {
  test("finds the internal replica hostname without matching the service record", () => {
    const records = [
      record("app-0.sandbox.maestro.internal.", "a", "10.52.0.35"),
      record("app-0.sandbox.maestro.internal.", "aaaa", "fd00:52::35"),
      record("app.sandbox.maestro.internal.", "a", "10.52.0.35")
    ];

    expect(replicaEndpoint("app", 0, records)).toEqual({
      hostname: "app-0.sandbox.maestro.internal"
    });
  });

  test("uses the same normalized and bounded DNS label as the control plane", () => {
    const serviceId = `api.preview.${"a".repeat(70)}`;
    const label = replicaDnsLabel(serviceId, 12);

    expect(label).toHaveLength(63);
    expect(label).toMatch(/-12$/);
    expect(label).not.toContain(".");
  });

  test("prefers the runtime-observed workload address over the reservation", () => {
    const assignment = {
      spec: { workloadAddress: "10.52.0.34" },
      status: { workloadAddress: "10.52.0.35" }
    } as Assignment;

    expect(assignmentWorkloadAddress(assignment)).toBe("10.52.0.35");
    expect(assignmentWorkloadAddress(undefined)).toBeNull();
  });
});
