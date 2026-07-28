import { Info, Network } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { ClusterApi } from "./api";
import { ClusterInfoPage } from "./ClusterInfoPage";
import { NodesPage } from "./NodesPage";

function createClusterFeature(api: ClusterApi) {
  const InfoRoute = () => <ClusterInfoPage api={api} />;
  const NodesRoute = () => <NodesPage api={api} />;
  return defineFeatureManifest({
    routes: [
      { path: "/", component: InfoRoute },
      { path: "/cluster", component: NodesRoute }
    ],
    nav: [
      {
        path: "/",
        label: "Info",
        icon: Info,
        section: "node",
        order: 10
      },
      {
        path: "/cluster",
        label: "Nodes",
        icon: Network,
        section: "cluster",
        order: 10
      }
    ]
  });
}

export { createClusterFeature };
