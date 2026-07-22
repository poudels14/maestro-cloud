import { expect, test } from "vitest";
import {
  emptyFirewallPolicy,
  firewallPolicyDraft,
  firewallPolicySpec,
  parsePortRanges
} from "./editor";
import type { FirewallPolicy } from "./api";

test("parses, deduplicates, and sorts port ranges", () => {
  expect(parsePortRanges("443, 8000-8080, 443, 53", 0)).toEqual([
    { start: 53, end: 53 },
    { start: 443, end: 443 },
    { start: 8000, end: 8080 }
  ]);
  expect(() => parsePortRanges("0, 90-80", 1)).toThrow(/Rule 2 ports/);
});

test("builds a typed service egress policy", () => {
  const draft = emptyFirewallPolicy();
  draft.id = "api-egress";
  draft.subjectType = "service";
  draft.subjectId = "api";
  draft.rules = [{ cidr: "10.0.0.0/8", protocol: "tcp", ports: "443", verdict: "allow" }];

  expect(firewallPolicySpec(draft)).toEqual({
    direction: "egress",
    subject: { type: "service", id: "api" },
    defaultVerdict: "deny",
    rules: [
      {
        cidr: "10.0.0.0/8",
        protocol: "tcp",
        ports: [{ start: 443, end: 443 }],
        verdict: "allow"
      }
    ]
  });
});

test("round trips an existing node host-input policy", () => {
  const policy = {
    meta: { id: "worker-input", generation: 2, revision: 8 },
    spec: {
      direction: "hostInput",
      subject: { type: "node", id: "worker-a" },
      defaultVerdict: "allow",
      rules: [
        {
          cidr: "192.0.2.0/24",
          protocol: "udp",
          ports: [{ start: 51820, end: 51820 }],
          verdict: "allow"
        }
      ]
    },
    status: { appliedGeneration: 2 }
  } satisfies FirewallPolicy;

  expect(firewallPolicyDraft(policy)).toEqual({
    id: "worker-input",
    direction: "hostInput",
    subjectType: "node",
    subjectId: "worker-a",
    defaultVerdict: "allow",
    rules: [
      {
        cidr: "192.0.2.0/24",
        protocol: "udp",
        ports: "51820",
        verdict: "allow"
      }
    ]
  });
});
