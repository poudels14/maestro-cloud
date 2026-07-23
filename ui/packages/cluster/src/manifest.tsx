import { BadgeCheck, Info, Network } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { ClusterApi } from "./api";
import { ClusterInfoPage } from "./ClusterInfoPage";
import { AdmissionsPage } from "./AdmissionsPage";
import { NodesPage } from "./NodesPage";

function createClusterFeature(api: ClusterApi) {
  const InfoRoute = () => <ClusterInfoPage api={api} />;
  const NodesRoute = () => <NodesPage api={api} />;
  const AdmissionsRoute = () => <AdmissionsPage api={api} />;

  return defineFeatureManifest({
    routes: [
      { path: "/", component: InfoRoute },
      { path: "/cluster", component: NodesRoute },
      { path: "/cluster/admissions", component: AdmissionsRoute }
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
      },
      {
        path: "/cluster/admissions",
        label: "Admissions",
        icon: BadgeCheck,
        section: "cluster",
        order: 20
      }
    ]
  });
}

export { createClusterFeature };
