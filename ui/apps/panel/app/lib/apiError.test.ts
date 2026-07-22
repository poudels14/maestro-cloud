import assert from "node:assert/strict";
import { test } from "vitest";
import { ApiRequestError, apiErrorFromBody } from "./apiError";

test("parses structured API errors", async () => {
  const error = apiErrorFromBody(
    409,
    JSON.stringify({
      error: {
        code: "cluster_deploys_frozen",
        message: "cluster deploys are frozen",
        details: { upgradeRunId: "run-123" }
      }
    }),
    "Redeploy failed"
  );

  assert.ok(error instanceof ApiRequestError);
  assert.equal(error.status, 409);
  assert.equal(error.code, "cluster_deploys_frozen");
  assert.equal(error.message, "cluster deploys are frozen");
  assert.deepEqual(error.details, { upgradeRunId: "run-123" });
});

test("keeps plain-text errors compatible during rolling upgrades", async () => {
  const error = apiErrorFromBody(409, "legacy controller error", "Restart failed");

  assert.equal(error.status, 409);
  assert.equal(error.code, null);
  assert.equal(error.message, "legacy controller error");
});
