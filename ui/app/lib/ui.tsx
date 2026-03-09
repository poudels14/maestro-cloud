import { createSignal, onCleanup, onMount, Show } from "solid-js";
import { EllipsisVertical, Ban, RotateCw, Square } from "lucide-solid";

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

export const STATUS_COLORS: Record<string, { dot: string; bg: string; text: string }> = {
  QUEUED: { dot: "bg-amber-400", bg: "bg-amber-50", text: "text-amber-700" },
  BUILDING: { dot: "bg-blue-400", bg: "bg-blue-50", text: "text-blue-700" },
  READY: { dot: "bg-emerald-400", bg: "bg-emerald-50", text: "text-emerald-700" },
  DEPLOYING: { dot: "bg-indigo-400", bg: "bg-indigo-50", text: "text-indigo-700" },
  RUNNING: { dot: "bg-emerald-400", bg: "bg-emerald-50", text: "text-emerald-700" },
  FAILED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CRASHED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  TERMINATED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CANCELLED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  CANCELED: { dot: "bg-red-400", bg: "bg-red-50", text: "text-red-700" },
  IDLE: { dot: "bg-gray-400", bg: "bg-gray-100", text: "text-gray-600" },
  STOPPED: { dot: "bg-gray-400", bg: "bg-gray-100", text: "text-gray-600" }
};

export function StatusBadge(props: { status: string }) {
  const colors = () => STATUS_COLORS[props.status] ?? STATUS_COLORS.STOPPED!;
  return (
    <span
      class={`inline-flex items-center gap-1.5 text-xs font-medium px-2 py-0.5 rounded-full ${colors().bg} ${colors().text}`}
    >
      <span class={`size-1.5 rounded-full ${colors().dot} inline-block`} />
      {props.status.toLowerCase()}
    </span>
  );
}

export function StatusDot(props: { status: string }) {
  const colors = () => STATUS_COLORS[props.status] ?? STATUS_COLORS.STOPPED!;
  return <span class={`size-2 rounded-full ${colors().dot} inline-block shrink-0`} />;
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
      class={`px-1 pb-2.5 text-sm font-medium border-b-2 transition-colors outline-none ${
        props.active
          ? "border-indigo-500 text-indigo-600"
          : "border-transparent text-gray-400 hover:text-gray-600"
      }`}
    >
      {props.label}
      <Show when={props.count !== undefined}>
        <span class={`ml-1.5 text-xs ${props.active ? "text-indigo-400" : "text-gray-400"}`}>
          {props.count}
        </span>
      </Show>
    </button>
  );
}

const CANCELLABLE_STATUSES = new Set(["QUEUED", "BUILDING", "DEPLOYING"]);
const STOPPABLE_STATUSES = new Set(["READY", "RUNNING"]);

export function DeploymentMenu(props: {
  status: string;
  onCancel: () => void;
  onStop: () => void;
  onRedeploy: () => void;
}) {
  const [open, setOpen] = createSignal(false);
  let menuRef: HTMLDivElement | undefined;

  const handleClickOutside = (e: MouseEvent) => {
    if (menuRef && !menuRef.contains(e.target as Node)) {
      setOpen(false);
    }
  };

  onMount(() => document.addEventListener("click", handleClickOutside));
  onCleanup(() => document.removeEventListener("click", handleClickOutside));

  const canCancel = () => CANCELLABLE_STATUSES.has(props.status);
  const canStop = () => STOPPABLE_STATUSES.has(props.status);

  return (
    <div class="relative" ref={menuRef}>
      <button
        type="button"
        onClick={(e) => {
          e.stopPropagation();
          setOpen(!open());
        }}
        class="text-gray-400 hover:text-gray-600 p-1 rounded hover:bg-gray-100 transition-colors outline-none"
      >
        <EllipsisVertical class="size-4" />
      </button>
      <Show when={open()}>
        <div class="absolute right-0 top-full mt-1 w-48 bg-white border border-gray-200 rounded-lg shadow-lg z-10 py-1.5">
          <Show when={canCancel()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onCancel();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2.5 outline-none"
            >
              <Ban class="size-3" />
              Cancel deployment
            </button>
          </Show>
          <Show when={canStop()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onStop();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-red-600 hover:bg-red-50 flex items-center gap-2.5 outline-none"
            >
              <Square class="size-3" />
              Stop deployment
            </button>
          </Show>
          <Show when={!canCancel()}>
            <button
              type="button"
              onClick={() => {
                setOpen(false);
                props.onRedeploy();
              }}
              class="w-full text-left px-4 py-2.5 text-sm text-gray-700 hover:bg-gray-50 flex items-center gap-2.5 outline-none"
            >
              <RotateCw class="size-3" />
              Redeploy
            </button>
          </Show>
        </div>
      </Show>
    </div>
  );
}
