import { Activity } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { MetricsApi } from "./api";
import { MetricsPage } from "./MetricsPage";

function createMetricsFeature(api: MetricsApi) {
  const MetricsRoute = () => <MetricsPage api={api} />;

  return defineFeatureManifest({
    routes: [{ path: "/metrics", component: MetricsRoute }],
    nav: [
      {
        path: "/metrics",
        label: "Metrics",
        icon: Activity,
        section: "node",
        order: 20
      }
    ]
  });
}

export { createMetricsFeature };
