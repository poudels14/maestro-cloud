import { defineConfig } from "vite";
import { tanstackStart } from "@tanstack/solid-start/plugin/vite";
import { nitro } from "nitro/vite";
import tailwindcss from "@tailwindcss/vite";
import viteSolid from "vite-plugin-solid";

export default defineConfig({
  plugins: [
    tailwindcss(),
    tanstackStart({
      srcDirectory: "app",
      router: {
        routeFileIgnorePrefix: "-"
      },
      prerender: {
        enabled: false
      },
      spa: {
        enabled: false
      }
    }),
    nitro({}),
    viteSolid({
      ssr: true
    })
  ]
});
