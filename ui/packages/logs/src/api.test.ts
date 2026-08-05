import { expect, test } from "vitest";
import type { MaestroApiClient, SystemLogReadQuery } from "@maestro/api-client";
import { createLogsApi } from "./api";

test("routes system log reads with component and cluster cursor filters", async () => {
  const calls: SystemLogReadQuery[] = [];
  const client = {
    async listSystemLogs(query: SystemLogReadQuery) {
      calls.push(query);
      return { entries: [], cursor: {}, previousCursor: null, hasPrevious: false };
    }
  } as unknown as MaestroApiClient;
  const api = createLogsApi(
    () => client,
    (error) => error as Error
  );

  const page = await api.getLogPage({
    scope: { type: "system", component: "daemon" },
    cursor: { "node-a": 42 },
    beforeCursor: { "node-a": 7 },
    nodeId: "node-a",
    query: "level:error",
    tail: 100
  });

  expect(page).toEqual({
    entries: [],
    cursor: {},
    previousCursor: null,
    hasPrevious: false
  });
  expect(calls).toEqual([
    {
      component: "daemon",
      cursor: '{"node-a":42}',
      beforeCursor: '{"node-a":7}',
      nodeId: "node-a",
      query: "level:error",
      tail: 100
    }
  ]);
});

test("maps generated client failures at the log boundary", async () => {
  const client = {
    async listLogs() {
      throw new Error("transport detail");
    }
  } as unknown as MaestroApiClient;
  const api = createLogsApi(
    () => client,
    (_error, fallback) => new Error(`${fallback} (mapped)`)
  );

  await expect(api.getLogPage({ scope: { type: "all" } })).rejects.toThrow(
    "Failed to load logs (mapped)"
  );
});
