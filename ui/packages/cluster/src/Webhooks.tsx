import { createSignal, For, Show } from "solid-js";
import { useMutation, useQueryClient } from "@tanstack/solid-query";
import { useQuery } from "@maestro/sdk";
import { Pencil, Plus, Send, Trash2 } from "lucide-solid";
import type { ClusterApi } from "./api";
import { clusterQueryKeys, webhooksQuery } from "./queries";
import type { Webhook } from "./types";
import { EVENT_OPTIONS, WebhookForm } from "./WebhookForm";
import { ConfirmDialog, ErrorBanner, SectionHeader } from "@maestro/kit";

function Webhooks(props: { api: ClusterApi }) {
  const queryClient = useQueryClient();
  const webhooks = useQuery(() => webhooksQuery(props.api));
  const [showForm, setShowForm] = createSignal(false);
  const [editing, setEditing] = createSignal<Webhook | null>(null);
  const [actionError, setActionError] = createSignal<string | null>(null);
  const [pendingDelete, setPendingDelete] = createSignal<Webhook | null>(null);

  const invalidate = () => queryClient.invalidateQueries({ queryKey: clusterQueryKeys.webhooks });

  const deleteMutation = useMutation(() => ({
    mutationFn: (webhook: Webhook) => props.api.deleteWebhook(webhook),
    onSuccess: () => {
      setPendingDelete(null);
      invalidate();
    },
    onError: (err) => {
      setActionError(err instanceof Error ? err.message : "delete failed");
      setPendingDelete(null);
    }
  }));

  return (
    <div>
      <div class="mb-4">
        <SectionHeader>Webhooks</SectionHeader>
        <p class="mt-1 text-xs text-gray-400">
          Deliver deployment and node notifications to Slack, or signed transition documents to
          another HTTPS endpoint.
        </p>
      </div>
      <Show when={actionError()}>
        {(message) => (
          <div class="mb-3">
            <ErrorBanner message={message()} />
          </div>
        )}
      </Show>
      <div class="bg-white border border-gray-200 rounded-lg divide-y divide-gray-100">
        <Show
          when={(webhooks.data?.length ?? 0) > 0}
          fallback={<div class="px-4 py-3 text-xs text-gray-400">No webhooks configured.</div>}
        >
          <For each={webhooks.data}>
            {(webhook) => (
              <WebhookRow
                api={props.api}
                webhook={webhook}
                onError={(msg) => setActionError(msg)}
                onRequestEdit={() => {
                  setActionError(null);
                  setShowForm(false);
                  setEditing(webhook);
                }}
                onRequestDelete={() => {
                  setActionError(null);
                  setPendingDelete(webhook);
                }}
              />
            )}
          </For>
        </Show>
        <Show
          keyed
          when={editing() ?? (showForm() ? ("new" as const) : null)}
          fallback={
            <button
              type="button"
              onClick={() => {
                setActionError(null);
                setEditing(null);
                setShowForm(true);
              }}
              class="w-full px-4 py-2.5 flex items-center gap-2 text-xs font-medium text-indigo-600 hover:bg-indigo-50 transition-colors outline-none"
            >
              <Plus class="size-3.5" />
              Add webhook
            </button>
          }
        >
          {(target) => (
            <WebhookForm
              api={props.api}
              {...(target === "new" ? {} : { webhook: target })}
              onCancel={() => {
                setShowForm(false);
                setEditing(null);
              }}
              onSaved={() => {
                setShowForm(false);
                setEditing(null);
                setActionError(null);
              }}
              onError={(msg) => setActionError(msg)}
            />
          )}
        </Show>
      </div>
      <ConfirmDialog
        open={pendingDelete() !== null}
        title="Delete webhook"
        description={
          <>
            Are you sure you want to delete{" "}
            <span class="font-medium text-gray-700">{pendingDelete()?.meta.id}</span>? No more
            events will be sent to this endpoint.
          </>
        }
        confirmLabel="Delete"
        confirmBusyLabel="Deleting…"
        busy={deleteMutation.isPending}
        onConfirm={() => {
          const target = pendingDelete();
          if (target) deleteMutation.mutate(target);
        }}
        onCancel={() => setPendingDelete(null)}
      />
    </div>
  );
}

function WebhookRow(props: {
  api: ClusterApi;
  webhook: Webhook;
  onError: (msg: string) => void;
  onRequestEdit: () => void;
  onRequestDelete: () => void;
}) {
  const testMutation = useMutation(() => ({
    mutationFn: () => props.api.testWebhook(props.webhook.meta.id),
    onError: (err) => props.onError(err instanceof Error ? err.message : "test failed")
  }));
  const busy = () => testMutation.isPending;

  return (
    <div class="px-4 py-3 flex items-center justify-between gap-4">
      <div class="min-w-0 flex-1">
        <div class="flex items-center gap-2">
          <span class="text-sm font-medium text-gray-800 truncate">
            {props.webhook.spec.name || props.webhook.meta.id}
          </span>
          <Show when={props.webhook.spec.name}>
            <span class="text-[11px] font-mono text-gray-400">{props.webhook.meta.id}</span>
          </Show>
          <span class="text-[11px] font-medium px-1.5 py-0.5 rounded bg-gray-50 text-gray-600 border border-gray-200">
            {props.webhook.spec.format === "slack" ? "Slack" : "Maestro"}
          </span>
          <Show when={!props.webhook.spec.enabled}>
            <span class="text-[11px] font-medium text-gray-500">Disabled</span>
          </Show>
          <For each={props.webhook.spec.events}>
            {(event) => (
              <span class="text-[11px] font-medium px-1.5 py-0.5 rounded border border-indigo-100 bg-indigo-50/60 text-indigo-700">
                {EVENT_OPTIONS.find((option) => option.value === event)?.label ?? event}
              </span>
            )}
          </For>
          <Show when={props.webhook.status.consecutiveFailures > 0}>
            <span class="text-[11px] font-medium text-red-600">
              {props.webhook.status.consecutiveFailures} failed
            </span>
          </Show>
        </div>
        <p class="text-xs font-mono text-gray-400 truncate mt-0.5">{props.webhook.spec.endpoint}</p>
        <p class="text-[11px] text-gray-400 mt-0.5">
          {props.webhook.spec.categories
            .map((category) => (category === "error" ? "Errors" : "Info"))
            .join(" · ")}
        </p>
      </div>
      <div class="flex items-center gap-1 shrink-0">
        <button
          type="button"
          onClick={() => props.onRequestEdit()}
          disabled={busy()}
          title="Edit webhook"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-md disabled:opacity-50"
        >
          <Pencil class="size-3.5" />
        </button>
        <button
          type="button"
          onClick={() => testMutation.mutate()}
          disabled={busy()}
          title="Send test message"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-indigo-600 hover:bg-indigo-50 rounded-md disabled:opacity-50"
        >
          <Send class="size-3.5" />
        </button>
        <button
          type="button"
          onClick={() => props.onRequestDelete()}
          disabled={busy()}
          title="Delete webhook"
          class="size-7 flex items-center justify-center text-gray-400 hover:text-red-600 hover:bg-red-50 rounded-md disabled:opacity-50"
        >
          <Trash2 class="size-3.5" />
        </button>
      </div>
    </div>
  );
}

export { Webhooks };
