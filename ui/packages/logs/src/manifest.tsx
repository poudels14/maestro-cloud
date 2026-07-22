import { ScrollText } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { LogsApi } from "./api";
import type { ClusterLogsLoaders } from "./ClusterLogsPage";
import { ClusterLogsPage } from "./ClusterLogsPage";
import { HttpLogsPage } from "./HttpLogsPage";

function createLogsFeature(api: LogsApi, cluster: ClusterLogsLoaders) {
  const LogsRoute = () => <HttpLogsPage api={api} />;
  const ClusterLogsRoute = () => <ClusterLogsPage api={api} {...cluster} />;

  return defineFeatureManifest({
    routes: [
      { path: "/http-logs", component: LogsRoute, layout: "full" },
      { path: "/cluster/logs", component: ClusterLogsRoute, layout: "full" }
    ],
    nav: [
      {
        path: "/http-logs",
        label: "HTTP logs",
        icon: ScrollText,
        section: "node",
        order: 50
      },
      {
        path: "/cluster/logs",
        label: "Logs",
        icon: ScrollText,
        section: "cluster",
        order: 20
      }
    ]
  });
}

export { createLogsFeature };
