import { createRouter as createTanstackRouter } from "@tanstack/solid-router";
import { routeTree } from "./routeTree.gen";

export function getRouter() {
  return createTanstackRouter({ routeTree, defaultSsr: false });
}

declare module "@tanstack/solid-router" {
  interface Register {
    router: Awaited<ReturnType<typeof getRouter>>;
  }
}
