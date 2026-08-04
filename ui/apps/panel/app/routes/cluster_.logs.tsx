import { createFileRoute } from "@tanstack/solid-router";
import { HomeShell } from "../components/home/HomeShell";

export const Route = createFileRoute("/cluster_/logs")({
  validateSearch: (
    search: Record<string, unknown>
  ): { node?: string; service?: string; component?: string; query?: string; range?: string } => ({
    ...(typeof search.node === "string" && search.node ? { node: search.node } : {}),
    ...(typeof search.service === "string" && search.service ? { service: search.service } : {}),
    ...(typeof search.component === "string" && search.component
      ? { component: search.component }
      : {}),
    ...(typeof search.query === "string" && search.query ? { query: search.query } : {}),
    ...(typeof search.range === "string" && search.range ? { range: search.range } : {})
  }),
  component: () => <HomeShell path="/cluster/logs" />
});
