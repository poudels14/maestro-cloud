import { expect, test } from "vitest";
import { composeFeatureManifests, defineFeatureManifest } from "./features";

const Component = () => "feature";
const Icon = () => "icon";

test("composes feature manifests and orders navigation deterministically", () => {
  const firewall = defineFeatureManifest({
    routes: [{ path: "/firewall", component: Component, layout: "wide" }],
    nav: [
      {
        path: "/firewall",
        label: "Firewall",
        icon: Icon,
        section: "node",
        order: 60
      }
    ]
  });
  const cluster = defineFeatureManifest({
    routes: [{ path: "/cluster", component: Component }],
    nav: [
      {
        path: "/cluster",
        label: "Cluster",
        icon: Icon,
        section: "cluster",
        order: 10
      }
    ],
    resourceViews: [{ kind: "Node", component: Component }]
  });

  const registry = composeFeatureManifests([firewall, cluster]);

  expect(registry.routes.map((route) => route.path)).toEqual(["/firewall", "/cluster"]);
  expect(registry.nav.map((entry) => entry.path)).toEqual(["/cluster", "/firewall"]);
  expect(registry.resourceViews.map((view) => view.kind)).toEqual(["Node"]);
});

test("rejects duplicate routes and resource views", () => {
  const feature = defineFeatureManifest({
    routes: [{ path: "/firewall", component: Component }],
    nav: [],
    resourceViews: [{ kind: "Service", component: Component }]
  });

  expect(() => composeFeatureManifests([feature, feature])).toThrow(
    "Feature route /firewall is registered more than once"
  );
  expect(() =>
    composeFeatureManifests([
      feature,
      {
        routes: [{ path: "/services", component: Component }],
        nav: [],
        resourceViews: [{ kind: "Service", component: Component }]
      }
    ])
  ).toThrow("Resource view Service is registered more than once");
});

test("rejects navigation that escapes its feature manifest", () => {
  expect(() =>
    composeFeatureManifests([
      {
        routes: [{ path: "/firewall", component: Component }],
        nav: [
          {
            path: "/services",
            label: "Wrong feature",
            icon: Icon,
            section: "node",
            order: 1
          }
        ]
      }
    ])
  ).toThrow("Feature navigation target /services has no route in its manifest");
});
