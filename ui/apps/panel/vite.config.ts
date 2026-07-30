import { defineConfig } from "vite";
import { tanstackStart } from "@tanstack/solid-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteSolid from "vite-plugin-solid";

const apiHost = process.env.MAESTRO_API_HOST;
const apiJwt = process.env.MAESTRO_API_JWT;

export default defineConfig({
  experimental: {
    bundledDev: true
  },
  server: apiHost
    ? {
        proxy: {
          "/api": {
            target: apiHost,
            changeOrigin: true,
            ...(apiJwt ? { headers: { authorization: `Bearer ${apiJwt}` } } : {})
          }
        }
      }
    : {},
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
