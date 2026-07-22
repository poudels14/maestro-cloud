import { Shield } from "lucide-solid";
import { defineFeatureManifest } from "@maestro/sdk";
import type { FirewallApi } from "./api";
import { FirewallSection } from "./FirewallSection";

function createFirewallFeature(api: FirewallApi) {
  const FirewallRoute = () => <FirewallSection api={api} />;

  return defineFeatureManifest({
    routes: [{ path: "/firewall", component: FirewallRoute, layout: "wide" }],
    nav: [
      {
        path: "/firewall",
        label: "Firewall",
        icon: Shield,
        section: "node",
        order: 60
      }
    ]
  });
}

export { createFirewallFeature };
