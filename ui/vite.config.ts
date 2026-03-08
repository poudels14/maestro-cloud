import { defineConfig, loadEnv } from "vite";
import { tanstackStart } from "@tanstack/solid-start/plugin/vite";
import tailwindcss from "@tailwindcss/vite";
import viteSolid from "vite-plugin-solid";

export default defineConfig(({ mode }) => {
  const env = loadEnv(mode, process.cwd(), "");
  const controllerHost = env.VITE_MAESTRO_CONTROLLER_HOST ?? "http://127.0.0.1:6400";

  return {
    define: {
      "import.meta.env.VITE_MAESTRO_CONTROLLER_HOST": JSON.stringify(controllerHost),
    },
    server: {
      proxy: {
        "/api": {
          target: controllerHost,
          changeOrigin: true,
        },
      },
    },
    plugins: [
      tailwindcss(),
      tanstackStart({
        srcDirectory: "app",
        router: {
          routeFileIgnorePrefix: "-",
        },
        prerender: {
          enabled: false,
        },
        spa: {
          enabled: true,
        },
      }),
      viteSolid({
        ssr: true,
      }),
    ],
  };
});
