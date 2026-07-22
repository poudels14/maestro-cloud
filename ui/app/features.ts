import { createFirewallApi, createFirewallFeature } from "@maestro/firewall";
import { createMetricsApi, createMetricsFeature } from "@maestro/metrics";
import { composeFeatureManifests } from "@maestro/sdk";
import { apiClient, apiRequestError } from "./lib/client";

const firewallApi = createFirewallApi(apiClient, apiRequestError);
const metricsApi = createMetricsApi(apiClient, apiRequestError);

const firewallFeature = createFirewallFeature(firewallApi);
const metricsFeature = createMetricsFeature(metricsApi);
const panelFeatureRegistry = composeFeatureManifests([metricsFeature, firewallFeature]);

type PanelFeaturePath = (typeof panelFeatureRegistry.routes)[number]["path"];

export { metricsApi, panelFeatureRegistry };
export type { PanelFeaturePath };
