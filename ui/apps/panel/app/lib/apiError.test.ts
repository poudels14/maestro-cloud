import assert from "node:assert/strict";
import { test } from "vitest";
import { ApiRequestError, apiErrorFromBody } from "./apiError";

test("parses structured API errors", async () => {
  const error = apiErrorFromBody(
    409,
    JSON.stringify({
      code: "clusterDeploysFrozen",
      message: "cluster deploys are frozen"
    }),
    "Redeploy failed"
  );

  assert.ok(error instanceof ApiRequestError);
  assert.equal(error.status, 409);
  assert.equal(error.code, "clusterDeploysFrozen");
  assert.equal(error.message, "cluster deploys are frozen");
});

test("rejects noncanonical error bodies", async () => {
  const plain = apiErrorFromBody(409, "controller error", "Restart failed");
  const nested = apiErrorFromBody(
    409,
    JSON.stringify({ error: { code: "conflict", message: "old shape" } }),
    "Restart failed"
  );

  assert.equal(plain.status, 409);
  assert.equal(plain.code, null);
  assert.equal(plain.message, "Restart failed");
  assert.equal(nested.status, 409);
  assert.equal(nested.code, null);
  assert.equal(nested.message, "Restart failed");
});
