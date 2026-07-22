import { ArrowLeftRight } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { LogsApi } from "@maestro/logs";
import type { IngressApi } from "./api";
import { TrafficPage } from "./TrafficPage";

function createIngressFeature(api: IngressApi, logsApi: LogsApi) {
  const TrafficRoute = () => <TrafficPage api={api} logsApi={logsApi} />;

  return defineFeatureManifest({
    routes: [{ path: "/traffic", component: TrafficRoute, layout: "wide" }],
    nav: [
      {
        path: "/traffic",
        label: "Traffic",
        icon: ArrowLeftRight,
        section: "node",
        order: 40
      }
    ]
  });
}

export { createIngressFeature };
