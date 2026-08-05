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

test("PR deployment details are nested beneath their base service", () => {
  const router = getRouter();
  const preview = router.routesByPath["/services/$serviceId/prs/$prId/$tab"];

  expect(preview).toBeDefined();
  expect(preview.parentRoute).toBe(router.routeTree);
  expect(preview.fullPath).toBe("/services/$serviceId/prs/$prId/$tab");
});
