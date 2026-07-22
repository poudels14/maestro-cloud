import type { ApiSchemas, MaestroApiClient } from "@maestro/api-client";

type FirewallDryRun = ApiSchemas["FirewallDryRunResponse"];
type FirewallPolicy = ApiSchemas["FirewallPolicy"];
type FirewallPolicySpec = ApiSchemas["FirewallPolicySpec"];

interface FirewallApi {
  listPolicies: () => Promise<FirewallPolicy[]>;
  savePolicy: (
    policyId: string,
    spec: FirewallPolicySpec,
    expectedRevision?: number
  ) => Promise<void>;
  deletePolicy: (policyId: string, expectedRevision: number) => Promise<void>;
  dryRunPolicy: (policyId: string, spec: FirewallPolicySpec) => Promise<FirewallDryRun>;
}

type FirewallErrorMapper = (error: unknown, fallback: string) => Error;

function createFirewallApi(
  client: () => MaestroApiClient,
  mapError: FirewallErrorMapper
): FirewallApi {
  return {
    async listPolicies() {
      try {
        return await client().listFirewallPolicies();
      } catch (error) {
        throw mapError(error, "Failed to load firewall policies");
      }
    },
    async savePolicy(policyId, spec, expectedRevision) {
      const request: ApiSchemas["FirewallPolicyWriteRequest"] =
        expectedRevision == null ? { spec } : { spec, expectedRevision };
      try {
        await client().putFirewallPolicy(policyId, request, crypto.randomUUID());
      } catch (error) {
        throw mapError(error, "Failed to save firewall policy");
      }
    },
    async deletePolicy(policyId, expectedRevision) {
      try {
        await client().deleteFirewallPolicy(policyId, { expectedRevision }, crypto.randomUUID());
      } catch (error) {
        throw mapError(error, "Failed to delete firewall policy");
      }
    },
    async dryRunPolicy(policyId, spec) {
      try {
        return await client().dryRunFirewallPolicy(policyId, { spec });
      } catch (error) {
        throw mapError(error, "Failed to plan firewall policy");
      }
    }
  };
}

export { createFirewallApi };
export type {
  FirewallApi,
  FirewallDryRun,
  FirewallErrorMapper,
  FirewallPolicy,
  FirewallPolicySpec
};
