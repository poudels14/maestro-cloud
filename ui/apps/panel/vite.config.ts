import { defineConfig } from "vite";
import { tanstackStart } from "@tanstack/solid-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteSolid from "vite-plugin-solid";

export default defineConfig({
  experimental: {
    bundledDev: true
  },
  plugins: [
    tailwindcss(),
    tanstackStart({
      srcDirectory: "app",
      router: {
        routeFileIgnorePrefix: "-"
      },
      spa: {
        enabled: true,
        prerender: {
          outputPath: "/index"
        }
      }
    }),
    viteSolid({
      ssr: true,
      hot: false
    })
  ]
});
