import assert from "node:assert/strict";
import { test } from "vitest";
import { isCurrentMaster } from "./clusterLeadership.ts";

test("identifies only the currently elected master", () => {
  assert.equal(isCurrentMaster("node-b", "node-b"), true);
  assert.equal(isCurrentMaster("node-a", "node-b"), false);
  assert.equal(isCurrentMaster("node-a", null), false);
});
