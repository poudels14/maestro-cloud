import { expect, test } from "vitest";
import type { MaestroApiClient } from "@maestro/api-client";
import { createMetricsApi } from "./api";

test("passes exact service metric ranges to the generated client", async () => {
  const calls: Array<{ serviceId: string; from?: number; to?: number }> = [];
  const client = {
    async listServiceMetrics(serviceId: string, query: { from?: number; to?: number }) {
      calls.push({ serviceId, ...query });
      return [];
    }
  } as unknown as MaestroApiClient;
  const api = createMetricsApi(
    () => client,
    (error) => error as Error
  );

  await api.listServiceMetrics("api/service", 100, 200);

  expect(calls).toEqual([{ serviceId: "api/service", from: 100, to: 200 }]);
});

test("maps generated client failures at the metrics boundary", async () => {
  const client = {
    async listLocalDisks() {
      throw new Error("transport detail");
    }
  } as unknown as MaestroApiClient;
  const api = createMetricsApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.listDisks()).rejects.toThrow("Failed to load disks (mapped)");
});
