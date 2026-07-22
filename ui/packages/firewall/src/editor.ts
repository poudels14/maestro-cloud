import type { ApiSchemas } from "@maestro/api-client";
import type { FirewallPolicy, FirewallPolicySpec } from "./api";

type FirewallDirection = ApiSchemas["FirewallDirection"];
type FirewallVerdict = ApiSchemas["FirewallVerdict"];
type TransportProtocol = ApiSchemas["TransportProtocol"];
type FirewallSubjectType = "global" | "service" | "node";

interface FirewallRuleDraft {
  cidr: string;
  protocol: TransportProtocol;
  ports: string;
  verdict: FirewallVerdict;
}

interface FirewallPolicyDraft {
  id: string;
  direction: FirewallDirection;
  subjectType: FirewallSubjectType;
  subjectId: string;
  defaultVerdict: FirewallVerdict;
  rules: FirewallRuleDraft[];
}

function emptyFirewallRule(): FirewallRuleDraft {
  return { cidr: "", protocol: "tcp", ports: "", verdict: "allow" };
}

function emptyFirewallPolicy(): FirewallPolicyDraft {
  return {
    id: "",
    direction: "egress",
    subjectType: "global",
    subjectId: "",
    defaultVerdict: "deny",
    rules: [emptyFirewallRule()]
  };
}

function firewallPolicyDraft(policy: FirewallPolicy): FirewallPolicyDraft {
  return {
    id: policy.meta.id,
    direction: policy.spec.direction,
    subjectType: policy.spec.subject.type,
    subjectId: policy.spec.subject.type === "global" ? "" : policy.spec.subject.id,
    defaultVerdict: policy.spec.defaultVerdict,
    rules: (policy.spec.rules ?? []).map((rule) => ({
      cidr: rule.cidr,
      protocol: rule.protocol,
      ports: formatPortRanges(rule.ports ?? []),
      verdict: rule.verdict
    }))
  };
}

function firewallPolicySpec(draft: FirewallPolicyDraft): FirewallPolicySpec {
  const id = draft.id.trim();
  validateResourceId(id);
  if (draft.direction === "egress" && draft.subjectType === "node") {
    throw new Error("Egress policies can target the cluster or a service");
  }
  if (draft.direction === "hostInput" && draft.subjectType === "service") {
    throw new Error("Host-input policies can target the cluster or a node");
  }
  const subject = firewallSubject(draft.subjectType, draft.subjectId);
  const rules = draft.rules.map((rule, index) => {
    const cidr = rule.cidr.trim();
    if (!cidr) throw new Error(`Rule ${index + 1} needs a CIDR`);
    return {
      cidr,
      protocol: rule.protocol,
      ports: parsePortRanges(rule.ports, index),
      verdict: rule.verdict
    } satisfies ApiSchemas["FirewallRule"];
  });
  return {
    direction: draft.direction,
    subject,
    rules,
    defaultVerdict: draft.defaultVerdict
  };
}

function firewallSubject(type: FirewallSubjectType, rawId: string): ApiSchemas["FirewallSubject"] {
  if (type === "global") return { type: "global" };
  const id = rawId.trim();
  if (!id) throw new Error(`${type === "service" ? "Service" : "Node"} ID is required`);
  validateResourceId(id);
  return { type, id };
}

function validateResourceId(value: string) {
  if (!value) throw new Error("Policy ID is required");
  if (value.length > 253) throw new Error("Resource IDs cannot exceed 253 characters");
  if (!/^[A-Za-z0-9](?:[A-Za-z0-9._-]*[A-Za-z0-9])?$/.test(value)) {
    throw new Error(
      "Resource IDs must start and end with a letter or digit and use only letters, digits, -, _, or ."
    );
  }
}

function parsePortRanges(value: string, ruleIndex = 0): ApiSchemas["PortRange"][] {
  const trimmed = value.trim();
  if (!trimmed) return [];
  const ranges = trimmed.split(",").map((token) => {
    const match = token.trim().match(/^(\d+)(?:\s*-\s*(\d+))?$/);
    if (!match) {
      throw new Error(`Rule ${ruleIndex + 1} has an invalid port range: ${token.trim()}`);
    }
    const start = Number(match[1]);
    const end = Number(match[2] ?? match[1]);
    if (start < 1 || end > 65_535 || start > end) {
      throw new Error(`Rule ${ruleIndex + 1} ports must be between 1 and 65535`);
    }
    return { start, end };
  });
  return ranges
    .filter(
      (range, index) =>
        ranges.findIndex(
          (candidate) => candidate.start === range.start && candidate.end === range.end
        ) === index
    )
    .sort((left, right) => left.start - right.start || left.end - right.end);
}

function formatPortRanges(ranges: ApiSchemas["PortRange"][]): string {
  return ranges
    .map((range) =>
      range.start === range.end ? String(range.start) : `${range.start}-${range.end}`
    )
    .join(", ");
}

export {
  emptyFirewallPolicy,
  emptyFirewallRule,
  firewallPolicyDraft,
  firewallPolicySpec,
  formatPortRanges,
  parsePortRanges
};
export type { FirewallPolicyDraft, FirewallRuleDraft, FirewallSubjectType };
