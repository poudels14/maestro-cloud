import clsx from "clsx";
import { AlertTriangle } from "lucide-solid";
import { For, Show, type JSX } from "solid-js";

export { ConfirmDialog } from "./ConfirmDialog";
export { SidebarNavItem, SidebarSection } from "./Sidebar";
export {
  formatBytes,
  formatBytesRate,
  formatDateTime,
  formatMs,
  formatPercent,
  formatRate
} from "./format";

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
  BUILDING: { dot: "bg-brand", pill: "bg-brand-light text-brand-hover border-brand-ring" },
  PENDING_READY: { dot: "bg-brand", pill: "bg-brand-light text-brand-hover border-brand-ring" },
  READY: { dot: "bg-emerald-400", pill: "bg-emerald-100 text-emerald-800 border-emerald-200" },
  DEPLOYING: { dot: "bg-brand", pill: "bg-brand-light text-brand-hover border-brand-ring" },
  RUNNING: { dot: "bg-emerald-400", pill: "bg-emerald-100 text-emerald-800 border-emerald-200" },
  FAILED: { dot: "bg-red-400", pill: "bg-red-50 text-red-700 border-red-100" },
  CRASHED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  TERMINATED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  CANCELLED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  CANCELED: { dot: "bg-red-400", pill: "bg-red-50 text-red-500 border-red-100" },
  SYSTEM: { dot: "bg-brand", pill: "bg-brand-light text-brand-hover border-brand-ring" },
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
          "border-brand text-brand": props.active,
          "border-transparent text-gray-400 hover:text-gray-600": !props.active
        }
      )}
    >
      {props.label}
      <Show when={props.count !== undefined}>
        <span
          class={clsx("ml-1.5 text-xs", {
            "text-brand/60": props.active,
            "text-gray-400": !props.active
          })}
        >
          {props.count}
        </span>
      </Show>
    </button>
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

export function CardSkeleton(props: { rows?: number; class?: string }) {
  return (
    <div
      class={clsx(
        "bg-white rounded-lg border border-gray-200 divide-y divide-gray-100",
        props.class
      )}
    >
      <For each={Array.from({ length: props.rows ?? 4 })}>
        {(_, index) => (
          <div class="px-4 py-3 flex items-center justify-between gap-6 animate-pulse">
            <span
              class="h-3 rounded bg-gray-100"
              style={{ width: `${[30, 40, 25, 35][index() % 4]}%` }}
            />
            <span
              class="h-3 rounded bg-gray-100"
              style={{ width: `${[15, 20, 25, 10][index() % 4]}%` }}
            />
          </div>
        )}
      </For>
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
