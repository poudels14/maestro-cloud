import { expect, test } from "vitest";
import { pullRequestsServiceId } from "./serviceNavigation";

test("PR breadcrumb targets the base service pull-request list", () => {
  expect(
    pullRequestsServiceId("app-pr-1081", {
      spec: { baseServiceId: "app" }
    })
  ).toBe("app");
  expect(pullRequestsServiceId("app")).toBe("app");
});
