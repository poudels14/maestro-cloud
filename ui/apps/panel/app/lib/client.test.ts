import assert from "node:assert/strict";
import { ApiHttpError } from "@maestro/api-client";
import { test } from "vitest";
import { isUnauthenticated } from "./client";

test("only HTTP 401 failures expire the browser session", () => {
  assert.equal(isUnauthenticated(new ApiHttpError(401, "unauthorized")), true);
  assert.equal(isUnauthenticated(new ApiHttpError(403, "forbidden")), false);
  assert.equal(isUnauthenticated(new Error("offline")), false);
});
