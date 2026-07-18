import assert from "node:assert/strict";
import { test } from "vitest";
import type { Service } from "./types";
import { servicePreviews, userServices } from "./previews.ts";

const service = (id: string, baseServiceId?: string, prNumber = 0): Service => ({
  id,
  name: id,
  version: "test",
  deploy: {
    command: null,
    healthcheckInterval: 60,
    replicas: 1
  },
  ...(baseServiceId
    ? {
        previewSource: {
          baseServiceId,
          prNumber,
          headRef: "feature",
          headSha: "abc",
          title: "Feature",
          createdAt: 1
        }
      }
    : {})
});

test("hides previews from the primary service list", () => {
  assert.deepEqual(
    userServices([service("api"), service("api-pr-2", "api", 2)]).map(({ id }) => id),
    ["api"]
  );
});

test("groups previews under their base service in PR order", () => {
  assert.deepEqual(
    servicePreviews(
      [
        service("api-pr-20", "api", 20),
        service("web-pr-1", "web", 1),
        service("api-pr-3", "api", 3)
      ],
      "api"
    ).map(({ id }) => id),
    ["api-pr-3", "api-pr-20"]
  );
});
