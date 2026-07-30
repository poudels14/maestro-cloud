import { For, Show } from "solid-js";
import { createStore } from "solid-js/store";
import {
  ArrowDownLeft,
  ArrowUpRight,
  FlaskConical,
  Loader2,
  Lock,
  Plus,
  ShieldCheck,
  Trash2
} from "lucide-solid";
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
  const egressPolicies = () =>
    (policies.data ?? []).filter((policy) => policy.spec.direction === "egress");
  const hostInputPolicies = () =>
    (policies.data ?? []).filter((policy) => policy.spec.direction === "hostInput");

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
      <div class="mb-5 flex items-end justify-between gap-3">
        <div>
          <h1 class="text-lg font-semibold text-gray-900">Firewall</h1>
          <p class="mt-1 text-sm text-gray-400">
            Ordered workload-egress and host-input rules compiled for every affected node.
          </p>
        </div>
        <button
          type="button"
          onClick={createPolicy}
          class="inline-flex shrink-0 items-center gap-1.5 rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-indigo-500"
        >
          <Plus class="size-3.5" /> New policy
        </button>
      </div>

      <Show when={policies.isError}>
        <div class="mb-3 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
          Failed to load firewall policies.
        </div>
      </Show>

      <div class="grid items-start gap-4 lg:grid-cols-[17rem_minmax(0,1fr)]">
        <div class="overflow-hidden rounded-xl border border-gray-200 bg-white shadow-[0_1px_2px_rgb(0_0_0/0.04)]">
          <PolicyGroup
            title="Workload egress"
            icon={<ArrowUpRight class="size-3 text-gray-400" />}
            policies={egressPolicies()}
            selectedId={editor.creating ? null : editor.selectedId}
            onSelect={selectPolicy}
          />
          <PolicyGroup
            title="Host input"
            icon={<ArrowDownLeft class="size-3 text-gray-400" />}
            policies={hostInputPolicies()}
            selectedId={editor.creating ? null : editor.selectedId}
            onSelect={selectPolicy}
          />
          <Show when={!policies.isLoading && (policies.data?.length ?? 0) === 0}>
            <div class="px-3 py-12 text-center">
              <ShieldCheck class="mx-auto size-7 text-gray-200" />
              <p class="mt-2 text-xs text-gray-400">No policies yet.</p>
            </div>
          </Show>
        </div>

        <Show
          when={isEditing()}
          fallback={
            <div class="rounded-xl border border-dashed border-gray-200 bg-white px-6 py-24 text-center">
              <ShieldCheck class="mx-auto size-8 text-gray-300" />
              <p class="mt-3 text-sm font-medium text-gray-500">No policy selected</p>
              <p class="mt-1 text-xs text-gray-400">
                Pick a policy from the list or create a new one to edit its rules.
              </p>
            </div>
          }
        >
          <div class="rounded-xl border border-gray-200 bg-white p-4 shadow-[0_1px_2px_rgb(0_0_0/0.04)] sm:p-5">
            <div class="mb-5 flex flex-wrap items-start justify-between gap-3">
              <div class="min-w-0">
                <div class="flex items-center gap-2">
                  <h2
                    class={clsx("truncate text-sm font-semibold text-gray-900", {
                      "font-mono": !editor.creating
                    })}
                  >
                    {editor.creating ? "New firewall policy" : editor.draft.id}
                  </h2>
                  <Show when={selectedPolicy()}>
                    {(policy) => <PolicyStatus policy={policy()} />}
                  </Show>
                </div>
                <Show when={selectedPolicy()}>
                  {(policy) => <PolicyEvidence policy={policy()} />}
                </Show>
              </div>
              <Show when={selectedPolicy() && !readOnly()}>
                <button
                  type="button"
                  onClick={() => setEditor("confirmDelete", true)}
                  class="inline-flex items-center gap-1.5 rounded-md border border-gray-200 px-2.5 py-1.5 text-xs font-medium text-gray-500 hover:border-red-200 hover:bg-red-50 hover:text-red-600"
                >
                  <Trash2 class="size-3.5" /> Delete
                </button>
              </Show>
            </div>

            <Show when={isManaged()}>
              <div class="mb-5 flex items-start gap-2.5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5">
                <Lock class="mt-px size-3.5 shrink-0 text-amber-500" />
                <p class="text-xs leading-5 text-amber-700">
                  This policy is managed by another resource. You can inspect and dry-run it, but
                  changes must go through its owner.
                </p>
              </div>
            </Show>
            <Show when={isDeleting()}>
              <div class="mb-5 rounded-lg border border-amber-200 bg-amber-50 px-3 py-2.5 text-xs text-amber-700">
                Deletion is in progress — the policy stops governing traffic once the next ruleset
                generation is applied.
              </div>
            </Show>

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
                <div class="mt-4 rounded-lg border border-red-200 bg-red-50 px-3 py-2 text-xs text-red-700">
                  {error()}
                </div>
              )}
            </Show>

            <div class="mt-5 flex justify-end gap-2 border-t border-gray-100 pt-4">
              <button
                type="button"
                disabled={editor.operation != null || isDeleting()}
                onClick={() => runOperation("dry-run")}
                class="inline-flex items-center gap-1.5 rounded-md border border-gray-200 bg-white px-3 py-1.5 text-xs font-medium text-gray-700 hover:bg-gray-50 disabled:opacity-50"
              >
                <Show
                  when={editor.operation === "dry-run"}
                  fallback={<FlaskConical class="size-3.5 text-gray-400" />}
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
                  class="inline-flex items-center gap-1.5 rounded-md bg-indigo-600 px-3 py-1.5 text-xs font-medium text-white shadow-sm hover:bg-indigo-500 disabled:bg-indigo-300"
                >
                  <Show when={editor.operation === "save"}>
                    <Loader2 class="size-3.5 animate-spin" />
                  </Show>
                  {editor.creating ? "Create policy" : "Save changes"}
                </button>
              </Show>
            </div>

            <FirewallDryRunPanel result={editor.dryRun} />
          </div>
        </Show>
      </div>

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

function PolicyGroup(props: {
  title: string;
  icon: import("solid-js").JSX.Element;
  policies: FirewallPolicy[];
  selectedId: string | null;
  onSelect: (policy: FirewallPolicy) => void;
}) {
  return (
    <Show when={props.policies.length > 0}>
      <div class="border-b border-gray-100 last:border-b-0">
        <div class="flex items-center justify-between gap-2 bg-gray-50/80 px-3 py-1.5">
          <span class="inline-flex items-center gap-1.5 text-[10px] font-medium uppercase tracking-wide text-gray-400">
            {props.icon}
            {props.title}
          </span>
          <span class="text-[10px] tabular-nums text-gray-300">{props.policies.length}</span>
        </div>
        <For each={props.policies}>
          {(policy) => (
            <button
              type="button"
              onClick={() => props.onSelect(policy)}
              class={clsx(
                "relative block w-full border-t border-gray-100 px-3 py-2.5 text-left first:border-t-0",
                {
                  "bg-indigo-50/70": props.selectedId === policy.meta.id,
                  "hover:bg-gray-50": props.selectedId !== policy.meta.id
                }
              )}
            >
              <Show when={props.selectedId === policy.meta.id}>
                <span class="absolute inset-y-2 left-0 w-0.5 rounded-r bg-indigo-500" />
              </Show>
              <div class="flex items-center justify-between gap-2">
                <span class="truncate font-mono text-xs font-medium text-gray-800">
                  {policy.meta.id}
                </span>
                <PolicyStatus policy={policy} />
              </div>
              <div class="mt-0.5 truncate text-[11px] text-gray-400">
                {subjectLabel(policy)} · {policy.spec.rules?.length ?? 0}{" "}
                {(policy.spec.rules?.length ?? 0) === 1 ? "rule" : "rules"} · default{" "}
                {policy.spec.defaultVerdict}
              </div>
            </button>
          )}
        </For>
      </div>
    </Show>
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
      class={clsx(
        "shrink-0 rounded-full border px-1.5 py-0.5 text-[9px] font-medium leading-none",
        {
          "border-emerald-100 bg-emerald-50 text-emerald-700": state() === "applied",
          "border-amber-100 bg-amber-50 text-amber-700":
            state() === "pending" || state() === "deleting",
          "border-red-100 bg-red-50 text-red-700": state() === "error"
        }
      )}
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
      <div class="mt-1 flex flex-wrap gap-x-4 gap-y-1 text-[11px] text-gray-400">
        <span>
          generation <span class="font-mono text-gray-600">{props.policy.meta.generation}</span>
        </span>
        <span>
          applied{" "}
          <span class="font-mono text-gray-600">{props.policy.status.appliedGeneration}</span>
        </span>
        <Show when={props.policy.status.rulesetDigest}>
          {(digest) => (
            <span title={digest()}>
              ruleset <span class="font-mono text-gray-600">{digest().slice(0, 12)}</span>
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

function subjectLabel(policy: FirewallPolicy) {
  const subject = policy.spec.subject;
  return subject.type === "global" ? "cluster-wide" : `${subject.type} ${subject.id}`;
}

export { FirewallSection };
