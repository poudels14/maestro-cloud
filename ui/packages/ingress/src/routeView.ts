import type { IngressRoute } from "./api";

function routePublicUrl(route: IngressRoute): string | null {
  const hostname = routeHostnames(route)[0];
  if (!hostname) return null;
  const secure = route.entryPoints.some(
    (entryPoint) => entryPoint.includes("secure") || entryPoint.includes("443")
  );
  return `${secure ? "https" : "http"}://${hostname}`;
}

function routeHostnames(route: IngressRoute): string[] {
  return [...route.rule.matchAll(/Host\(([^)]*)\)/g)].flatMap((match) =>
    [...match[1]!.matchAll(/`([^`]+)`/g)].map((host) => host[1]!)
  );
}

export { routeHostnames, routePublicUrl };
