import { createFileRoute } from "@tanstack/solid-router";
import { HomeShell } from "../components/home/HomeShell";

export const Route = createFileRoute("/http-logs")({
  validateSearch: (search: Record<string, unknown>): { query?: string; range?: string } => ({
    ...(typeof search.query === "string" && search.query ? { query: search.query } : {}),
    ...(typeof search.range === "string" && search.range ? { range: search.range } : {})
  }),
  component: () => <HomeShell tab="http-logs" />
});
