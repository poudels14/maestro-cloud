import { createFileRoute } from "@tanstack/solid-router";
import { HomeShell } from "../components/home/HomeShell";

export const Route = createFileRoute("/metrics")({
  component: () => <HomeShell tab="metrics" />
});
