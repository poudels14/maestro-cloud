import { expect, test } from "vitest";
import { getRouter } from "../router";

test("cluster logs is not nested beneath the nodes page", () => {
  const router = getRouter();
  const clusterLogs = router.routesByPath["/cluster/logs"];

  expect(clusterLogs).toBeDefined();
  expect(clusterLogs.parentRoute).toBe(router.routeTree);
});

test("PR previews has a routable panel page", () => {
  const router = getRouter();
  const previews = router.routesByPath["/previews"];

  expect(previews).toBeDefined();
  expect(previews.parentRoute).toBe(router.routeTree);
});
