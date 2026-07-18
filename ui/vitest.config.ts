import { tanstackRouterGenerator } from "@tanstack/router-plugin/vite";
import { defineConfig } from "vitest/config";
import viteSolid from "vite-plugin-solid";

export default defineConfig({
  plugins: [
    tanstackRouterGenerator({
      target: "solid",
      routesDirectory: "./app/routes",
      generatedRouteTree: "./app/routeTree.gen.ts",
      routeFileIgnorePrefix: "-"
    }),
    viteSolid({
      ssr: true,
      hot: false
    })
  ],
  test: {
    environment: "node"
  }
});
