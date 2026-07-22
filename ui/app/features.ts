import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const firewallApi = createFirewallApi(apiClient, apiRequestError);

const firewallFeature = createFirewallFeature(firewallApi);
const panelFeatureRegistry = composeFeatureManifests([firewallFeature]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { panelFeatureRegistry };
export type { PanelFeaturePath };
