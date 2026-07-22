import { ScrollText } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { LogsApi } from "./api";
import { HttpLogsPage } from "./HttpLogsPage";

function createLogsFeature(api: LogsApi) {
  const LogsRoute = () => <HttpLogsPage api={api} />;

  return defineFeatureManifest({
    routes: [{ path: "/http-logs", component: LogsRoute, layout: "full" }],
    nav: [
      {
        path: "/http-logs",
        label: "HTTP logs",
        icon: ScrollText,
        section: "node",
        order: 50
      }
    ]
  });
}

export { createLogsFeature };
