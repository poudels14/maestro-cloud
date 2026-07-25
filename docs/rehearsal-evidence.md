# Production cutover rehearsal evidence

This checklist is the go/no-go record for the mandatory production-snapshot
rehearsal. Complete it after following [cutover.md](cutover.md) in an isolated
staging environment. Archive the completed copy with the migration artifacts.

Record an immutable artifact path, command transcript, dashboard link, or
captured API response for every checked item. A passing unit or CI test is not
staging evidence. Mark an optional integration `N/A` only when the production
launch policy leaves it disabled.

## Rehearsal identity

- Cluster ID:
- Rehearsal start and end time:
- Operators:
- Source snapshot creation time:
- Source snapshot SHA-256:
- Post-migration snapshot SHA-256:
- Maestro version:
- Release archive or Nix derivation:
- Release SHA-256:
- Reviewed `maestro.jsonc` fingerprint:
- Planned production node count and roles:
- Rehearsal node count and roles:

The rehearsal topology, paths, secrets, runtime versions, and enabled
integrations match production:

- [ ] Yes

Any intentional difference is documented here and approved before rehearsal:

-

## Migration and restored data

- [ ] The pre-migration native snapshot passed `etcdutl snapshot status`.
- [ ] `maestro-migrate plan` accepted every legacy key family and reported no
      in-flight leadership, request, maintenance, or node-lifecycle work.
- [ ] `maestro-migrate apply` completed against the exact reviewed logical
      snapshot.
- [ ] `maestro-migrate verify` passed before any rewrite daemon started.
- [ ] The post-migration native snapshot passed `etcdutl snapshot status`.
- [ ] Every control-plane member passed `store-restore` and `store-verify`
      against the same logical and native snapshot digests.
- [ ] Every node passed `telemetry-apply` and `telemetry-verify` against its
      reviewed telemetry plan.
- [ ] Service definitions, deployment history, preview policy, webhooks,
      node identities, drains, placement history, traffic generations, and
      registry-free image placement match the reviewed migration report.
- [ ] Secret-bearing values are decryptable by the rewrite, masked through
      operator APIs, and absent from captured logs and reports.
- [ ] Legacy log, metric, traffic, Parquet, and manifest counts reconcile with
      each node's telemetry verification report.

Evidence:

-

## Cluster and workload convergence

- [ ] All expected control-plane members establish quorum.
- [ ] `maestro cluster info`, `maestro cluster nodes`, and
      `maestro cluster config` report the reviewed identity, roles, endpoints,
      subnets, ports, and capabilities.
- [ ] Every expected node becomes ready and no undeclared node is admitted.
- [ ] The WireGuard mesh converges, and a workload on one node reaches a
      workload address on another node.
- [ ] Migrated assignments converge to running, healthchecked workloads under
      the native containerd runtime.
- [ ] A daemon restart reconstructs control state without duplicating
      workloads or losing status.
- [ ] A planned node restart preserves quorum and restores routing and
      workload convergence.
- [ ] Drain copies required images before evacuation; drain and restore both
      converge.
- [ ] Host volumes remain pinned to their declared node, and node affinity is
      preserved.
- [ ] Workload secrets are owner-only mounts and are removed after workload
      teardown.

Evidence:

-

## Service lifecycle

Use a dedicated rehearsal service with ingress, a healthcheck, multiple
replicas, environment values, a secret, and any production volume shape.
Exercise the same API paths used by normal automation.

- [ ] Declarative rollout preview masks secrets and reports the expected env,
      secret, replica, ingress, volume, healthcheck, and egress changes.
- [ ] Declarative rollout applies and reaches ready traffic.
- [ ] Tarball upload creates an immutable build and deployment.
- [ ] Git or Depot build reaches an immutable artifact when enabled in
      production.
- [ ] Redeploy creates a new deployment and completes blue/green cutover.
- [ ] Restart replaces workload instances without replacing the deployment.
- [ ] Cancel stops an eligible queued or building deployment.
- [ ] Remove drains and removes a selected deployment.
- [ ] Freeze blocks automatic rollout; unfreeze resumes it.
- [ ] Replica override set and clear both converge without dropping valid
      traffic.
- [ ] Service deletion cascades owned resources after draining.
- [ ] Deployment history and legal phase transitions remain visible.
- [ ] Readiness thresholds, stagger, restart backoff, and restart exhaustion
      produce the expected replica and deployment conditions.

Evidence:

-

## Network, ingress, and security

- [ ] Workloads resolve local service names through the bridge-bound
      authoritative resolver.
- [ ] External or recursive DNS queries are refused by that resolver.
- [ ] Cross-cluster DNS resolves through Tailscale only when configured.
- [ ] The nftables dry run matches the reviewed global and per-service policy.
- [ ] Allowed egress succeeds and denied egress fails for a rehearsal
      workload.
- [ ] The host-input chain admits only the documented DNS and control-plane
      traffic from workload and control networks.
- [ ] Ingress routes, preview wildcard routes, client certificates, and
      ingress-denied behavior match the reviewed configuration.
- [ ] Cloudflare tunnel replicas become ready when configured.
- [ ] Blocking and unblocking one test address changes both routing behavior
      and the blocked-traffic view.
- [ ] Operator endpoints reject missing, invalid, or insufficient JWT scopes;
      browser sessions enforce secure cookie and CSRF behavior.
- [ ] Node-to-node control traffic uses the expected mutual-TLS identities.

Evidence:

-

## Logs, metrics, backup, and integrations

- [ ] Service, deployment, build, system, and cluster-wide log queries return
      normalized records with resumable cursors.
- [ ] Log tailing resumes without duplicates or gaps visible to the operator.
- [ ] JSON, RFC3339, logrus, date-prefixed, ANSI, and Traefik fixture lines
      normalize as expected.
- [ ] LogQL field predicates, attributes, ranges, wildcards, boolean
      operations, negation, and HTTP status aliases return expected records.
- [ ] Log histograms and service, node, cluster, disk, container, traffic, and
      sink-runtime metrics populate.
- [ ] Datadog logs and metrics arrive with expected tags when configured;
      healthcheck and origin filters match policy.
- [ ] S3 backup uploads encrypted objects and publishes the manifest last when
      configured.
- [ ] A backed-up partition remains queryable after hot-to-cold rollover, and
      retention removes only eligible fully backed-up data.
- [ ] A forced poison sink entry is quarantined and is inspectable,
      exportable, and purgeable through dead-letter commands.
- [ ] Slack test delivery and one deployment or node-availability
      notification succeed when configured.

Evidence:

-

## Exec, previews, upgrades, and panel

- [ ] Interactive exec supports input, output, resize, exit status, and kill
      through a local and a cross-node replica.
- [ ] Exec is denied for a service whose policy disables it.
- [ ] A pull-request preview covers open, push-to-same-URL redeploy, base edit,
      freeze propagation, close grace, reopen, and final expiry when enabled.
- [ ] Preview quota, fork/draft policy, sticky comments, and rate-limit retry
      match production policy when enabled.
- [ ] A rolling upgrade or restart transfers leadership as needed, stages,
      drains, verifies, and unfreezes each node.
- [ ] The all-node mode displays and requires the expected outage approval.
- [ ] The panel loads from the daemon origin and its home, cluster, services,
      deployments, metrics, traffic, HTTP logs, cluster logs, firewall,
      admissions, and webhook surfaces return live data.
- [ ] Confirm dialogs, error toasts, log detail, histograms, and deployment
      timeline interactions work in the production browser profile.

Evidence:

-

## Go/no-go

- [ ] No unexpected failed conditions, crash loops, dead letters, resource
      oscillation, or daemon errors remain at the end of the agreed burn-in.
- [ ] External DNS or ingress was not flipped before all preceding gates
      passed.
- [ ] The exact snapshots, plans, verification reports, launch-document
      fingerprints, versions, transcripts, and this checklist are archived.
- [ ] The rehearsal included the planned workload-restart window and measured
      recovery time is within the approved production window.
- [ ] Recovery from the retained post-migration snapshot was demonstrated or
      explicitly accepted by the production owner.
- [ ] Production cutover is approved as a one-way transition with no legacy
      compatibility or downgrade path after workloads restart.

Decision:

- [ ] GO
- [ ] NO-GO

Approver, timestamp, and notes:

-
