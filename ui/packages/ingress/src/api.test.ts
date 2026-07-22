import { expect, test } from "vitest";
import type { ApiSchemas, IngressTrafficQuery, MaestroApiClient } from "@maestro/api-client";
import { createIngressApi } from "./api";

test("routes ingress reads and blocklist writes through the generated client", async () => {
  const calls: Array<{ operation: string; value: unknown }> = [];
  const emptyTraffic = { byIp: [], byPath: [] };
  const client = {
    async listActiveIngressRoutes() {
      calls.push({ operation: "routes", value: null });
      return [];
    },
    async getIngressTraffic(query: IngressTrafficQuery) {
      calls.push({ operation: "traffic", value: query });
      return emptyTraffic;
    },
    async getBlockedIngressTraffic(query: IngressTrafficQuery) {
      calls.push({ operation: "blocked-traffic", value: query });
      return emptyTraffic;
    },
    async getIngressBlocklist() {
      calls.push({ operation: "blocklist", value: null });
      return { blockedIps: [] };
    },
    async setBlockedIngressIp(request: ApiSchemas["BlockedIpRequest"]) {
      calls.push({ operation: "set-blocked", value: request });
      return { blockedIps: request.blocked ? [request.ip] : [] };
    }
  } as unknown as MaestroApiClient;
  const api = createIngressApi(
    () => client,
    (error) => error as Error
  );

  await api.listRoutes();
  await api.getTraffic(10, 20);
  await api.getBlockedTraffic(30, 40);
  await api.getBlocklist();
  await api.setBlockedIp("192.0.2.7", true);

  expect(calls).toEqual([
    { operation: "routes", value: null },
    { operation: "traffic", value: { from: 10, to: 20, limit: 200 } },
    { operation: "blocked-traffic", value: { from: 30, to: 40, limit: 200 } },
    { operation: "blocklist", value: null },
    { operation: "set-blocked", value: { ip: "192.0.2.7", blocked: true } }
  ]);
});

test("maps generated client failures at the ingress boundary", async () => {
  const client = {
    async listActiveIngressRoutes() {
      throw new Error("transport detail");
    }
  } as unknown as MaestroApiClient;
  const api = createIngressApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.listRoutes()).rejects.toThrow("Failed to load ingress routes (mapped)");
});
