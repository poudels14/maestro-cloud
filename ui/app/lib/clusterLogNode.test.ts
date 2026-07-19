import assert from "node:assert/strict";
import { test } from "vitest";
import { clusterLogNodeLabel } from "./clusterLogNode.ts";

test("uses the stable node ID instead of a placeholder hostname", () => {
  assert.equal(
    clusterLogNodeLabel({ nodeId: "node-a", nodeName: "unknown-node", hostname: "unknown-host" }),
    "node-a"
  );
});
