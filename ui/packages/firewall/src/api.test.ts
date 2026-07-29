import { expect, test } from "vitest";
import type { ApiSchemas, MaestroApiClient } from "@maestro/api-client";
import { createFirewallApi } from "./api";

test("writes policies with optimistic revisions and idempotency keys", async () => {
  const writes: Array<{
    policyId: string;
    request: ApiSchemas["FirewallPolicyWriteRequest"];
    idempotencyKey: string;
  }> = [];
  const client = {
    async putFirewallPolicy(
      policyId: string,
      request: ApiSchemas["FirewallPolicyWriteRequest"],
      idempotencyKey: string
    ) {
      writes.push({ policyId, request, idempotencyKey });
      return {};
    }
  } as unknown as MaestroApiClient;
  const api = createFirewallApi(
    () => client,
    (error) => error as Error
  );
  const spec = {
    direction: "egress",
    subject: { type: "global" },
    defaultVerdict: "deny",
    rules: []
  } satisfies ApiSchemas["FirewallPolicySpec"];

  await api.savePolicy("default-egress", spec, 12);

  expect(writes).toHaveLength(1);
  expect(writes[0]?.policyId).toBe("default-egress");
  expect(writes[0]?.request).toEqual({ spec, expectedRevision: 12 });
  expect(writes[0]?.idempotencyKey).toMatch(/^[0-9A-Za-z]{24}$/);
});

test("maps generated client failures at the feature boundary", async () => {
  const client = {
    async listFirewallPolicies() {
      throw new Error("transport detail");
    }
  } as unknown as MaestroApiClient;
  const api = createFirewallApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.listPolicies()).rejects.toThrow("Failed to load firewall policies (mapped)");
});
