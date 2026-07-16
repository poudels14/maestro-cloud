import { Show } from "solid-js";
import type { JSX } from "solid-js";
import { DropdownMenu } from "@kobalte/core/dropdown-menu";
import { EllipsisVertical, Ban, RotateCw, RefreshCw, Square, AlertTriangle } from "lucide-solid";
import clsx from "clsx";

export function timeAgo(ms: number): string {
  const seconds = Math.floor((Date.now() - ms) / 1000);
  if (seconds < 60) return "just now";
  const minutes = Math.floor(seconds / 60);
  if (minutes < 60) return `${minutes}m ago`;
  const hours = Math.floor(minutes / 60);
  if (hours < 24) return `${hours}h ago`;
  const days = Math.floor(hours / 24);
  return `${days}d ago`;
}

export const STATUS_COLORS: Record<string, { dot: string; pill: string }> = {
  QUEUED: { dot: "bg-amber-400", pill: "bg-amber-50 text-amber-700 border-amber-100" },
  BUILDING: { dot: "bg-blue-400", pill: "bg-blue-50 text-blue-700 border-blue-100" },
  PENDING_READY: { dot: "bg-cyan-400", pill: "bg-cyan-50 text-cyan-700 border-cyan-100" },
  READY: { dot: "bg-emerald-400", pill: "bg-emerald-100 text-emerald-800 border-emerald-200" },
  DEPLOYING: { dot: "bg-indigo-400", pill: "bg-indigo-50 text-indigo-700 border-indigo-100" },
  RUNNING: { dot: "bg-emerald-400", pill: "bg-emerald-100 text-emerald-800 border-emerald-200" },
  FAILED: { dot: "bg-red-400", pill: "bg-red-50 text-red-700 border-red-100" },
  CRASHED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  TERMINATED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  CANCELLED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  CANCELED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  SYSTEM: { dot: "bg-violet-400", pill: "bg-violet-50 text-violet-700 border-violet-100" },
  IDLE: { dot: "bg-gray-400", pill: "bg-gray-100 text-gray-500 border-gray-200" },
  STOPPED: { dot: "bg-gray-400", pill: "bg-gray-100 text-gray-500 border-gray-200" }
};

export function StatusBadge(props: { status: string; class?: string }) {
  const colors = () => STATUS_COLORS[props.status] ?? STATUS_COLORS.STOPPED!;
  return (
    <span
      class={clsx(
        "inline-flex items-center text-xs font-medium px-2 py-0.5 rounded-md border",
        colors().pill,
        props.class
      )}
    >
      {props.status.toLowerCase()}
    </span>
  );
}

export function StatusDot(props: { status: string }) {
  const colors = () => STATUS_COLORS[props.status] ?? STATUS_COLORS.STOPPED!;
  return <span class={clsx("size-2 rounded-full inline-block shrink-0", colors().dot)} />;
}

export function TabButton(props: {
  active: boolean;
  label: string;
  count?: number;
  onClick: () => void;
}) {
  return (
    <button
      type="button"
      onClick={props.onClick}
      class={clsx(
        "px-1 pb-2 text-sm font-medium border-b-2 transition-[color,border-color] duration-150 ease-out-strong outline-none",
        {
          "border-indigo-500 text-indigo-600": props.active,
          "border-transparent text-gray-400 hover:text-gray-600": !props.active
        }
      )}
    >
      {props.label}
      <Show when={props.count !== undefined}>
        <span
          class={clsx("ml-1.5 text-xs", {
            "text-indigo-400": props.active,
            "text-gray-400": !props.active
          })}
        >
          {props.count}
        </span>
      </Show>
    </button>
  );
}

const CANCELLABLE_STATUSES = new Set(["QUEUED", "BUILDING", "DEPLOYING"]);
const STOPPABLE_STATUSES = new Set(["READY", "PENDING_READY", "RUNNING"]);

export function DeploymentMenu(props: {
  status: string;
  onCancel: () => void;
  onStop: () => void;
  onRedeploy: () => void;
  onRestart: () => void;
}) {
  const canCancel = () => CANCELLABLE_STATUSES.has(props.status);
  const canStop = () => STOPPABLE_STATUSES.has(props.status);

  return (
    <DropdownMenu placement="bottom-end">
      <DropdownMenu.Trigger class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-[color,background-color,transform] duration-150 ease-out-strong active:scale-[0.96] outline-none">
        <EllipsisVertical class="size-4" />
      </DropdownMenu.Trigger>
      <DropdownMenu.Portal>
        <DropdownMenu.Content class="bg-white border border-gray-200 rounded-lg shadow-lg z-50 py-1.5 w-48">
          <Show when={canCancel()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
              onSelect={() => props.onCancel()}
            >
              <Ban class="size-3" />
              Cancel deployment
            </DropdownMenu.Item>
          </Show>
          <Show when={canStop()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 cursor-pointer outline-none"
              onSelect={() => props.onStop()}
            >
              <Square class="size-3" />
              Stop deployment
            </DropdownMenu.Item>
          </Show>
          <Show when={!canCancel()}>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer outline-none"
              onSelect={() => props.onRedeploy()}
            >
              <RotateCw class="size-3" />
              Redeploy
            </DropdownMenu.Item>
            <DropdownMenu.Item
              class="flex items-center gap-2.5 px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 cursor-pointer outline-none"
              onSelect={() => props.onRestart()}
            >
              <RefreshCw class="size-3" />
              Restart
            </DropdownMenu.Item>
          </Show>
        </DropdownMenu.Content>
      </DropdownMenu.Portal>
    </DropdownMenu>
  );
}

export function ErrorBanner(props: { message: string; onRetry?: () => void }) {
  return (
    <div class="bg-red-50 border border-red-200 rounded-lg px-4 py-3 flex items-center gap-3">
      <AlertTriangle class="size-4 text-red-500 shrink-0" />
      <span class="text-sm text-red-700 flex-1">{props.message}</span>
      <Show when={props.onRetry}>
        <button
          type="button"
          onClick={props.onRetry}
          class="text-xs font-medium text-red-600 hover:text-red-700 outline-none"
        >
          Retry
        </button>
      </Show>
    </div>
  );
}

export function Card(props: { class?: string; children: JSX.Element }) {
  return (
    <div
      class={clsx(
        "bg-white rounded-lg border border-gray-200 shadow-[0_1px_2px_rgb(0_0_0/0.04)]",
        props.class
      )}
    >
      {props.children}
    </div>
  );
}

export function SectionHeader(props: { class?: string; children: JSX.Element }) {
  return (
    <h2 class={clsx("text-sm font-semibold tracking-tight text-gray-900", props.class)}>
      {props.children}
    </h2>
  );
}
