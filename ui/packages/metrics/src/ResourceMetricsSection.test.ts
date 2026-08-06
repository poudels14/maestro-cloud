import { expect, test } from "vitest";
import { nodeIdFromMetricSource } from "./labels";

test("extracts the exact node identity without adding presentation separators", () => {
  expect(nodeIdFromMetricSource("node:node-1")).toBe("node-1");
  expect(nodeIdFromMetricSource("node:1ksov5kv11rc")).toBe("1ksov5kv11rc");
  expect(nodeIdFromMetricSource("node:")).toBeNull();
  expect(nodeIdFromMetricSource(undefined)).toBeNull();
});
