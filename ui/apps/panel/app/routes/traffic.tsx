import { createFileRoute } from "@tanstack/solid-router";
import { HomeShell } from "../components/home/HomeShell";

export const Route = createFileRoute("/traffic")({
  validateSearch: (search: Record<string, unknown>): { range?: string } =>
    typeof search.range === "string" && search.range ? { range: search.range } : {},
  component: () => <HomeShell path="/traffic" />
});
