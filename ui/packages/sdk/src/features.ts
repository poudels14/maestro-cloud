import type { Component } from "solid-js";

type FeaturePath = `/${string}`;
type FeatureLayout = "standard" | "wide" | "full";
type NavSection = "node" | "cluster";

interface RouteDefinition {
  path: FeaturePath;
  component: Component;
  layout?: FeatureLayout;
}

interface NavEntry {
  path: FeaturePath;
  label: string;
  icon: Component<{ class?: string }>;
  section: NavSection;
  order: number;
}

interface ResourceViewDefinition {
  kind: string;
  component: Component<{ resource: unknown }>;
}

interface FeatureManifest {
  routes: readonly RouteDefinition[];
  nav: readonly NavEntry[];
  resourceViews?: readonly ResourceViewDefinition[];
}

interface FeatureRegistry {
  routes: RouteDefinition[];
  nav: NavEntry[];
  resourceViews: ResourceViewDefinition[];
}

type ComposedFeatureRegistry<Manifests extends readonly FeatureManifest[]> = {
  routes: Array<Manifests[number]["routes"][number]>;
  nav: Array<Manifests[number]["nav"][number]>;
  resourceViews: ResourceViewDefinition[];
};

function defineFeatureManifest<const Manifest extends FeatureManifest>(
  manifest: Manifest
): Manifest {
  return manifest;
}

function composeFeatureManifests<const Manifests extends readonly FeatureManifest[]>(
  manifests: Manifests
): ComposedFeatureRegistry<Manifests> {
  const routes: RouteDefinition[] = [];
  const nav: NavEntry[] = [];
  const resourceViews: ResourceViewDefinition[] = [];
  const routePaths = new Set<string>();
  const resourceKinds = new Set<string>();

  for (const manifest of manifests) {
    const manifestPaths = new Set(manifest.routes.map((route) => route.path));
    for (const route of manifest.routes) {
      if (routePaths.has(route.path)) {
        throw new Error(`Feature route ${route.path} is registered more than once`);
      }
      routePaths.add(route.path);
      routes.push(route);
    }
    for (const entry of manifest.nav) {
      if (!manifestPaths.has(entry.path)) {
        throw new Error(`Feature navigation target ${entry.path} has no route in its manifest`);
      }
      nav.push(entry);
    }
    for (const view of manifest.resourceViews ?? []) {
      if (resourceKinds.has(view.kind)) {
        throw new Error(`Resource view ${view.kind} is registered more than once`);
      }
      resourceKinds.add(view.kind);
      resourceViews.push(view);
    }
  }

  nav.sort((left, right) => left.order - right.order || left.label.localeCompare(right.label));
  return { routes, nav, resourceViews } as ComposedFeatureRegistry<Manifests>;
}

export { composeFeatureManifests, defineFeatureManifest };
export type {
  FeatureLayout,
  FeatureManifest,
  FeaturePath,
  FeatureRegistry,
  ComposedFeatureRegistry,
  NavEntry,
  NavSection,
  ResourceViewDefinition,
  RouteDefinition
};
