import { LayoutGrid } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { ServicesApi } from "./api";
import { PreviewsPage } from "./PreviewsPage";
import { ServicesGrid } from "./ServicesGrid";

function createServicesFeature(api: ServicesApi) {
  const ServicesRoute = () => <ServicesGrid api={api} />;
  const PreviewsRoute = () => <PreviewsPage api={api} />;

  return defineFeatureManifest({
    routes: [
      { path: "/services", component: ServicesRoute },
      { path: "/previews", component: PreviewsRoute }
    ],
    nav: [
      {
        path: "/services",
        label: "Services",
        icon: LayoutGrid,
        section: "node",
        order: 30
      }
    ]
  });
}

export { createServicesFeature };
