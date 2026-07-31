import { For, Show } from "solid-js";
import { createStore } from "solid-js/store";
import { Dialog } from "@kobalte/core/dialog";
import { FlaskConical, Loader2, Lock, Plus, ShieldCheck, Trash2, X } from "lucide-solid";
import clsx from "clsx";
import { useQuery } from "@maestro/sdk";
import { ConfirmDialog } from "@maestro/kit";
import {
  emptyFirewallPolicy,
  emptyFirewallRule,
  firewallPolicyDraft,
  firewallPolicySpec
} from "./editor";
import type { FirewallPolicyDraft, FirewallRuleDraft } from "./editor";
import type { FirewallApi, FirewallDryRun, FirewallPolicy } from "./api";
import { FirewallDryRunPanel } from "./FirewallDryRunPanel";
import { FirewallPolicyForm } from "./FirewallPolicyForm";

type Operation = "save" | "delete" | "dry-run";

interface EditorState {
  selectedId: string | null;
  expectedRevision: number | null;
  creating: boolean;
  draft: FirewallPolicyDraft;
  operation: Operation | null;
  error: string | null;
  dryRun: FirewallDryRun | null;
  confirmDelete: boolean;
}

function FirewallSection(props: { api: FirewallApi }) {
  const policies = useQuery(() => ({
    queryKey: ["firewall", "policies"] as const,
    queryFn: typeof window === "undefined" ? () => Promise.resolve([]) : props.api.listPolicies,
    refetchInterval: 10_000
  }));
  const [editor, setEditor] = createStore<EditorState>({
    selectedId: null,
    expectedRevision: null,
    creating: false,
    draft: emptyFirewallPolicy(),
    operation: null,
    error: null,
    dryRun: null,
    confirmDelete: false
  });

  const selectedPolicy = () =>
    policies.data?.find((policy) => policy.meta.id === editor.selectedId) ?? null;
  const isEditing = () => editor.creating || selectedPolicy() != null;
  const isManaged = () => (selectedPolicy()?.meta.ownerRefs?.length ?? 0) > 0;
  const isDeleting = () => selectedPolicy()?.meta.deletionTimestamp != null;
  const readOnly = () => isManaged() || isDeleting();
  const sortedPolicies = () =>
    [...(policies.data ?? [])].sort(
      (left, right) =>
        left.spec.direction.localeCompare(right.spec.direction) ||
        left.meta.id.localeCompare(right.meta.id)
    );

  const closeEditor = () => {
    setEditor({
      selectedId: null,
      expectedRevision: null,
      creating: false,
      operation: null,
      error: null,
      dryRun: null,
      confirmDelete: false
    });
  };

  const selectPolicy = (policy: FirewallPolicy) => {
    setEditor({
      selectedId: policy.meta.id,
      expectedRevision: policy.meta.revision,
      creating: false,
      draft: firewallPolicyDraft(policy),
      operation: null,
      error: null,
      dryRun: null,
      confirmDelete: false
    });
  };

  const createPolicy = () => {
    setEditor({
      selectedId: null,
      expectedRevision: null,
      creating: true,
      draft: emptyFirewallPolicy(),
      operation: null,
      error: null,
      dryRun: null,
      confirmDelete: false
    });
  };

  const updateField = <Key extends keyof FirewallPolicyDraft>(
    key: Key,
    value: FirewallPolicyDraft[Key]
  ) => {
    setEditor("draft", (draft) => ({ ...draft, [key]: value }));
    if (key === "direction") {
      const allowed = value === "egress" ? ["global", "service"] : ["global", "node"];
      if (!allowed.includes(editor.draft.subjectType)) {
        setEditor("draft", "subjectType", "global");
        setEditor("draft", "subjectId", "");
      }
    }
    setEditor("dryRun", null);
    setEditor("error", null);
  };

  const updateRule = (index: number, update: Partial<FirewallRuleDraft>) => {
    setEditor("draft", "rules", (rules) =>
      rules.map((rule, ruleIndex) => (ruleIndex === index ? { ...rule, ...update } : rule))
    );
    setEditor("dryRun", null);
    setEditor("error", null);
  };

  const runOperation = async (operation: "save" | "dry-run") => {
    setEditor("operation", operation);
    setEditor("error", null);
    try {
      const spec = firewallPolicySpec(editor.draft);
      const policyId = editor.draft.id.trim();
      if (operation === "dry-run") {
        setEditor("dryRun", await props.api.dryRunPolicy(policyId, spec));
      } else {
        await props.api.savePolicy(policyId, spec, editor.expectedRevision ?? undefined);
        const refreshed = await policies.refetch();
        setEditor("selectedId", policyId);
        setEditor(
          "expectedRevision",
          refreshed.data?.find((policy) => policy.meta.id === policyId)?.meta.revision ?? null
        );
        setEditor("creating", false);
        setEditor("dryRun", null);
      }
    } catch (cause) {
      setEditor("error", cause instanceof Error ? cause.message : String(cause));
    } finally {
      setEditor("operation", null);
    }
  };

  const confirmDelete = async () => {
    const policy = selectedPolicy();
    if (!policy || editor.expectedRevision == null) return;
    setEditor("operation", "delete");
    setEditor("error", null);
    try {
      await props.api.deletePolicy(policy.meta.id, editor.expectedRevision);
      await policies.refetch();
      setEditor({
        selectedId: null,
        expectedRevision: null,
        creating: false,
        operation: null,
        dryRun: null,
        confirmDelete: false
      });
    } catch (cause) {
      setEditor("error", cause instanceof Error ? cause.message : String(cause));
      setEditor("operation", null);
      setEditor("confirmDelete", false);
    }
  };

  return (
    <section>
      <div class="mb-5 flex items-center justify-between gap-3">
        <h1 class="text-lg font-semibold text-neutral-900">Firewall</h1>
        <button
          type="button"
          onClick={createPolicy}
          class="inline-flex shrink-0 items-center gap-1.5 rounded bg-brand px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-brand-hover"
        >
          <Plus class="size-3.5" /> New policy
        </button>
      </div>

      <Show when={policies.isError}>
        <div class="mb-3 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
          Failed to load firewall policies.
        </div>
      </Show>

      <div class="overflow-x-auto rounded-md border border-neutral-200 bg-white">
        <div
          class={`${policyGridClass} border-b border-neutral-200 bg-neutral-50 px-4 py-2 text-[11px] font-medium text-neutral-500`}
        >
          <span>Policy</span>
          <span>Rules</span>
        </div>
        <For each={sortedPolicies()}>
          {(policy) => (
            <div
              role="button"
              tabIndex={0}
              onClick={() => selectPolicy(policy)}
              onKeyDown={(event) => {
                if (event.key === "Enter" || event.key === " ") {
                  event.preventDefault();
                  selectPolicy(policy);
                }
              }}
              class={`${policyGridClass} cursor-pointer items-start border-b border-neutral-100 px-4 py-3 outline-none last:border-b-0 hover:bg-neutral-50`}
            >
              <div class="min-w-0">
                <div class="flex items-center gap-2">
                  <span class="truncate font-mono text-xs font-medium text-neutral-800">
                    {policy.meta.id}
                  </span>
                  <PolicyStatus policy={policy} />
                </div>
                <div class="mt-1 truncate text-[11px] text-neutral-400">
                  {directionLabel(policy.spec.direction)} · {subjectLabel(policy)}
                </div>
              </div>
              <div class="min-w-0 space-y-0.5">
                <For each={policy.spec.rules ?? []}>
                  {(rule) => (
                    <div class="flex items-baseline gap-2 font-mono text-[11px]">
                      <span class="truncate text-neutral-600">{rule.cidr}</span>
                      <span
                        class={clsx("shrink-0", {
                          "text-emerald-600": rule.verdict === "allow",
                          "text-red-600": rule.verdict === "deny"
                        })}
                      >
                        {rule.verdict}
                      </span>
                    </div>
                  )}
                </For>
                <div class="font-mono text-[11px] text-neutral-400">
                  * {policy.spec.defaultVerdict}
                </div>
              </div>
            </div>
          )}
        </For>
        <Show when={policies.isLoading}>
          <div class="space-y-2 px-4 py-3">
            <div class="h-3 w-2/3 animate-pulse rounded bg-neutral-100" />
            <div class="h-2.5 w-1/2 animate-pulse rounded bg-neutral-100" />
          </div>
        </Show>
        <Show when={!policies.isLoading && (policies.data?.length ?? 0) === 0}>
          <div class="px-3 py-12 text-center">
            <ShieldCheck class="mx-auto size-7 text-neutral-200" />
            <p class="mt-2 text-xs text-neutral-400">No policies yet.</p>
          </div>
        </Show>
      </div>

      <Dialog
        open={isEditing()}
        onOpenChange={(open) => {
          if (!open) closeEditor();
        }}
      >
        <Dialog.Portal>
          <Dialog.Overlay class="fixed inset-0 z-40 bg-black/20 backdrop-blur-[1px]" />
          <Dialog.Content class="fixed top-0 right-0 bottom-0 z-50 flex w-full max-w-2xl flex-col border-l border-neutral-200 bg-white shadow-2xl outline-none">
            <div class="shrink-0 border-b border-neutral-200 px-4 py-4 sm:px-5">
              <div class="flex items-start justify-between gap-3">
                <div class="min-w-0">
                  <div class="flex items-center gap-2">
                    <h2
                      class={clsx("truncate text-sm font-semibold text-neutral-900", {
                        "font-mono": !editor.creating
                      })}
                    >
                      {editor.creating ? "New firewall policy" : editor.draft.id}
                    </h2>
                    <Show when={selectedPolicy()}>
                      {(policy) => <PolicyStatus policy={policy()} />}
                    </Show>
                  </div>
                  <Show when={isManaged()}>
                    <p class="mt-1 flex items-center gap-1.5 text-xs text-neutral-500">
                      <Lock class="size-3 shrink-0 text-neutral-400" />
                      System default firewall
                    </p>
                  </Show>
                  <Show when={isDeleting()}>
                    <p class="mt-1 text-xs text-amber-600">Deletion in progress.</p>
                  </Show>
                  <Show when={selectedPolicy()}>
                    {(policy) => <PolicyEvidence policy={policy()} />}
                  </Show>
                </div>
                <div class="flex shrink-0 items-center gap-1.5">
                  <Show when={selectedPolicy() && !readOnly()}>
                    <button
                      type="button"
                      onClick={() => setEditor("confirmDelete", true)}
                      class="inline-flex items-center gap-1.5 rounded border border-neutral-200 px-2.5 py-1.5 text-xs font-medium text-neutral-500 hover:border-red-200 hover:bg-red-50 hover:text-red-600"
                    >
                      <Trash2 class="size-3.5" /> Delete
                    </button>
                  </Show>
                  <Dialog.CloseButton class="rounded p-1 text-neutral-400 outline-none hover:bg-neutral-100 hover:text-neutral-700">
                    <X class="size-4" />
                  </Dialog.CloseButton>
                </div>
              </div>
            </div>

            <div class="min-h-0 flex-1 overflow-y-auto px-4 py-5 sm:px-5">
              <FirewallPolicyForm
                draft={editor.draft}
                existing={!editor.creating}
                readOnly={readOnly()}
                onField={updateField}
                onRule={updateRule}
                onAddRule={() => {
                  setEditor("draft", "rules", (rules) => [...rules, emptyFirewallRule()]);
                  setEditor("dryRun", null);
                }}
                onRemoveRule={(index) => {
                  setEditor("draft", "rules", (rules) =>
                    rules.filter((_, ruleIndex) => ruleIndex !== index)
                  );
                  setEditor("dryRun", null);
                }}
              />

              <Show when={editor.error}>
                {(error) => (
                  <div class="mt-4 rounded-md border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                    {error()}
                  </div>
                )}
              </Show>

              <FirewallDryRunPanel result={editor.dryRun} />
            </div>

            <div class="flex shrink-0 justify-end gap-2 border-t border-neutral-200 px-4 py-3 sm:px-5">
              <button
                type="button"
                disabled={editor.operation != null || isDeleting()}
                onClick={() => runOperation("dry-run")}
                class="inline-flex items-center gap-1.5 rounded border border-neutral-200 bg-white px-3 py-1.5 text-xs font-medium text-neutral-700 hover:bg-neutral-50 disabled:opacity-50"
              >
                <Show
                  when={editor.operation === "dry-run"}
                  fallback={<FlaskConical class="size-3.5 text-neutral-400" />}
                >
                  <Loader2 class="size-3.5 animate-spin" />
                </Show>
                Dry run
              </button>
              <Show when={!readOnly()}>
                <button
                  type="button"
                  disabled={editor.operation != null}
                  onClick={() => runOperation("save")}
                  class="inline-flex items-center gap-1.5 rounded bg-brand px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-brand-hover disabled:bg-brand/40"
                >
                  <Show when={editor.operation === "save"}>
                    <Loader2 class="size-3.5 animate-spin" />
                  </Show>
                  {editor.creating ? "Create policy" : "Save changes"}
                </button>
              </Show>
            </div>
          </Dialog.Content>
        </Dialog.Portal>
      </Dialog>

      <ConfirmDialog
        open={editor.confirmDelete}
        title="Delete firewall policy?"
        description={
          <>
            Policy <span class="font-mono">{selectedPolicy()?.meta.id}</span> will stop governing
            traffic after the controller applies the next ruleset generation.
          </>
        }
        confirmLabel="Delete policy"
        confirmBusyLabel="Deleting…"
        busy={editor.operation === "delete"}
        onConfirm={() => void confirmDelete()}
        onCancel={() => setEditor("confirmDelete", false)}
      />
    </section>
  );
}

function PolicyStatus(props: { policy: FirewallPolicy }) {
  const state = () => {
    if (props.policy.meta.deletionTimestamp != null) return "deleting";
    const failure = props.policy.status.conditions?.find(
      (condition) => condition.status === "false" || condition.status === "unknown"
    );
    if (failure) return "error";
    return props.policy.status.appliedGeneration === props.policy.meta.generation
      ? "applied"
      : "pending";
  };
  return (
    <span
      class={clsx("shrink-0 rounded-sm border px-1.5 py-0.5 text-[9px] font-medium leading-none", {
        "border-emerald-100 bg-emerald-50 text-emerald-700": state() === "applied",
        "border-amber-100 bg-amber-50 text-amber-700":
          state() === "pending" || state() === "deleting",
        "border-red-100 bg-red-50 text-red-700": state() === "error"
      })}
    >
      {state()}
    </span>
  );
}

function PolicyEvidence(props: { policy: FirewallPolicy }) {
  const failing = () =>
    (props.policy.status.conditions ?? []).filter(
      (condition) => condition.status !== "true" && condition.message
    );
  return (
    <>
      <div class="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-neutral-400">
        <span>
          generation <span class="font-mono text-neutral-600">{props.policy.meta.generation}</span>
        </span>
        <span>
          applied{" "}
          <span class="font-mono text-neutral-600">{props.policy.status.appliedGeneration}</span>
        </span>
        <Show when={props.policy.status.rulesetDigest}>
          {(digest) => (
            <span title={digest()}>
              ruleset <span class="font-mono text-neutral-600">{digest().slice(0, 12)}</span>
            </span>
          )}
        </Show>
      </div>
      <For each={failing()}>
        {(condition) => (
          <div class="mt-1.5 text-[11px] text-amber-600">
            <span class="font-medium">{condition.type}</span>: {condition.message}
          </div>
        )}
      </For>
    </>
  );
}

function directionLabel(direction: FirewallPolicy["spec"]["direction"]) {
  return direction === "egress" ? "egress" : "host input";
}

const policyGridClass =
  "grid min-w-[34rem] grid-cols-[minmax(15rem,1fr)_minmax(14rem,1.1fr)] gap-3";

function subjectLabel(policy: FirewallPolicy) {
  const subject = policy.spec.subject;
  return subject.type === "global" ? "cluster-wide" : `${subject.type} ${subject.id}`;
}

export { FirewallSection };
