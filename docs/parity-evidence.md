# Rewrite parity evidence

This ledger tracks the cutover gate in `REWRITE.md` §13–§14 without treating
the existence of code as proof that an operational requirement passed. Update
it whenever behavior, acceptance coverage, or rehearsal evidence changes.

Status meanings:

- **Verified** — implemented and covered by a passing local deterministic
  suite.
- **Acceptance pending** — deterministic coverage passes, but the named real
  backend or real-process suite has not passed for the release commit.
- **Rehearsal pending** — implementation and test coverage exist, but the
  production-snapshot rehearsal or live environment proof is still missing.
- **Evidence incomplete** — a historical or comparative exit condition is not
  proven by the current worktree and needs an explicit waiver or stronger
  artifact.
- **Dropped** — deliberately removed with the sign-off recorded in
  `REWRITE.md`.

## Current verification

The following checks passed together on 2026-07-27:

```text
cargo test --workspace
cargo clippy --workspace --all-targets -- -D warnings
python3 scripts/check-source-layout.py
python3 scripts/check-crate-boundaries.py
pnpm -r test
pnpm --dir ui typecheck
pnpm --dir ui build
pnpm check:api-drift
```

The Rust workspace run excludes only tests that explicitly require real
Docker, containerd, etcd, MinIO/KES, or privileged real-cluster networking.
Those are release gates, not inferred successes.

The release workflow additionally runs formatting, dependency, documentation,
Nix, snapshot, real-runtime, real-cluster, encrypted-cutover, and MinIO/KES
checks. It must pass for the exact release commit before this ledger can
promote the affected rows from **Acceptance pending**.

## Milestone exits

| Milestone | Current evidence | Status |
|---|---|---|
| M0 — stabilize and harvest | `clustertest` contains the harvested lifecycle, orchestration, failure, and topology scenarios. The historical “green against the old system” run is not reproducible from current rewrite checks. | Evidence incomplete |
| M1 — kernel and contracts | Store/controller/kernel tests cover CAS, transactions, watches, sessions, fencing, failover, schemas, and OpenAPI. Generated UI types build and pass drift checking. The real three-member etcd failover test remains an external gate. | Acceptance pending |
| M2 — cluster forms | In-process one/three-node setup tests pass. `scripts/run-real-cluster-acceptance.sh` exercises real bootstrap, signed join, admission, encrypted etcd membership, WireGuard workload ping, loss, restart, and rejoin. | Acceptance pending |
| M3 — workloads run | Runtime and agent conformance, adoption, GC, health, logs, stats, secrets, exec, node API, and restart tests pass. Real Docker/containerd and privileged recovery suites remain external gates. | Acceptance pending |
| M4 — orchestration parity | Default `clustertest` runs lifecycle verbs and orchestration scenarios across one- and three-node topologies; deployment, scheduler, ingress, DNS, and firewall suites pass. | Verified |
| M5 — observability parity | Parser/property, LogQL, hot/cold storage, compaction, backup ordering, retention, metrics, sink retry, and dead-letter tests pass. Real MinIO/KES SSE-KMS remains an external gate. | Acceptance pending |
| M6 — build, preview, upgrade | Build, preview, upgrade, and daemon-composition suites pass, including full preview lifecycle and three-node upgrade scenarios. Real runtime/build backends remain external gates. | Acceptance pending |
| M7 — server, CLI, UI | Server routes, CLI commands, generated client, all feature packages, UI typecheck, production build, and API drift checks pass. | Verified |
| M8 — cutover | The migration tool and runbooks exist and synthetic encrypted three-member cutover is a release gate. Sandbox deployment and the mandatory production-snapshot rehearsal are not complete. | Rehearsal pending |

## Feature parity

### Services and deployments

| Requirement | Evidence | Status |
|---|---|---|
| Declarative rollout with masked diff preview | Service rollout/diff routes, CLI rollout tests, and deployment orchestration tests | Verified |
| Upload, redeploy, restart, cancel, remove deployment, delete service | CLI `up`/service commands and `clustertest` lifecycle scenarios | Verified |
| Freeze/unfreeze and replica override set/clear | Deployment planning, daemon orchestration, and CLI command tests | Verified |
| Deployment history and legal state transitions | Deployment history API/CLI tests and exhaustive `DeploymentPhase` transition matrix | Verified |
| Blue/green cutover and old-generation collection | Ingress/deployment planning and rollout/redeploy scenarios | Verified |
| Drain, finalization, exhaustion, and scale-down traffic holds | Deployment/scheduler drain tests and lifecycle scenarios | Verified |
| Volumes, host-volume pinning, and node affinity | Agent assignment planning plus scheduler and orchestration affinity tests | Verified |
| Encrypted, masked, private, zeroized secrets with diff hashes | Store encryption, API masking, secret-mount, and rollout diff tests | Verified |
| Health checks, readiness, stagger, restart budget, and backoff | Agent health/restart and deployment readiness tests | Verified |
| Per-service egress through `FirewallPolicy` | Firewall planner/store, agent application, dry-run API, and UI editor tests | Verified |
| Detached event-driven replica supervision | Runtime conformance plus agent adoption, event replay, restart, and GC tests | Acceptance pending |
| Native containerd and Docker API backends; nerdctl removed | Runtime conformance and dedicated real-backend scripts | Acceptance pending |

### Cluster

| Requirement | Evidence | Status |
|---|---|---|
| Roles, quorum shapes, and preflight validation | Cluster topology/provider and daemon role-plan tests | Verified |
| Embedded etcd, CA, signed join, approval, certificates, and removal | Cluster and server admission/removal suites; real-process cluster script | Acceptance pending |
| Leader election and fenced mutation | Kernel controller and store-backed operator failure tests | Acceptance pending |
| Scheduler, addresses, manifests, and CAS generations | Scheduler plan/writer and cluster topology scenarios | Verified |
| Liveness, availability, drain/restore, and artifact-safe drain | Node registry, artifact drain/replication, and scheduler drain tests | Verified |
| One rolling/all-node upgrade machine, staging, and coordinated restart | Upgrade transition/plan/agent/reconciler and three-node scenario tests | Verified |
| Quorum disaster recovery | Embedded-etcd recovery planning and real-process recovery coverage | Acceptance pending |
| P2P artifacts, replication, leases, and registry alternative | Runtime artifact and agent replication/holder suites | Acceptance pending |
| Persisted cluster ports including WireGuard UDP | Cluster port/topology tests and launch validation | Verified |
| Write from any node through resource storage | Request dedup, any-node claim, and API mutation tests | Verified |
| Kernel-level request deduplication | Kernel controller dedup and replay tests | Verified |
| Pre-cluster legacy migration | Superseded by the reviewed one-shot cutover migrator | Dropped |

### Networking, DNS, and firewall

| Requirement | Evidence | Status |
|---|---|---|
| WireGuard workload mesh with control-plane RPC gateway | Mesh planner/agent/netlink artifact tests and real cross-node ping script | Acceptance pending |
| Bridge-bound authoritative DNS without recursion | DNS planner/resource/server tests | Verified |
| Cluster domain, allowlists, IPAM, and overlap validation | Cluster topology/network and DNS tests | Verified |
| Optional cross-cluster resolver through scoped Tailscale access | Agent DNS plugin and daemon Tailscale resource tests | Rehearsal pending |
| Atomic nftables egress, named sets, exemptions, and DNS legs | Firewall snapshots/planner and Linux backend exact-script tests | Acceptance pending |
| Host-input protection and system-plane mTLS | Firewall baseline and daemon production-settings tests | Acceptance pending |
| Python connected-cluster landing page | Sign-off is recorded in `REWRITE.md` decisions #18 | Dropped |

### Ingress and traffic

| Requirement | Evidence | Status |
|---|---|---|
| Traefik generation, client certificates, routes, denial, previews, and Cloudflare | Ingress Traefik snapshots/store tests and daemon system-resource tests | Verified |
| Canonical blocklist and blocked-traffic views | Ingress blocklist plus server/UI traffic tests | Verified |
| Access-log-derived traffic analytics only | Logs traffic aggregation and API/client/UI traffic tests | Verified |

### Builds and previews

| Requirement | Evidence | Status |
|---|---|---|
| Git sync, credential isolation, watch backoff, Depot, and build secrets | Build source, watcher, Depot, reconciliation, and archive tests | Acceptance pending |
| Complete PR-preview lifecycle, quota, comments, and rate limits | Preview source/resource/reconciler and daemon acceptance scenarios | Verified |

### Logs

| Requirement | Evidence | Status |
|---|---|---|
| Full parser and normalization chain | Parser corpus and arbitrary-byte property tests | Verified |
| Cursor sinks, dead letters, Datadog behavior, and health filtering | Logs sink/filter/Datadog fault-injection and daemon CLI/API tests | Verified |
| DuckDB/Parquet, manifests, SSE-KMS backup, and retention | Logstore archive/backup/retention suites; real MinIO/KES script | Acceptance pending |
| Full LogQL grammar and status aliases | LogQL parser/compiler corpus and arbitrary-Unicode property tests | Verified |
| Local/cluster queries, histograms, merge cursors, CLI logs and tail | Logs query/cluster query plus server/CLI/API-client tests | Verified |
| Fabric UDS/OTLP, one wire shape, and versioned migrations | Node fabric/API, OTLP spool, and logstore migration tests | Verified |

### Metrics

| Requirement | Evidence | Status |
|---|---|---|
| Native workload/host/disk stats and node/cluster APIs | Agent cgroup/host/disk collectors, metrics projections, and server/UI tests | Acceptance pending |
| Datadog metrics, rates, tags, runtime stats, and backup stats | Metrics Datadog/sink and logs operational-stat suites | Verified |

### Exec

| Requirement | Evidence | Status |
|---|---|---|
| PTY lifecycle, cap, cross-node relay, and CLI TTY | Runtime exec conformance and server/CLI relay tests | Acceptance pending |
| Docker API exec parity | Docker runtime conformance and real Docker acceptance script | Acceptance pending |

### Config

| Requirement | Evidence | Status |
|---|---|---|
| JSONC files, extends, sources, diagnostics, init, and masked API | CLI config/source and daemon/server config tests | Verified |
| One typed config decomposed into component views | Daemon launch/plan/settings structure and crate-boundary checker | Verified |

### Authentication and API

| Requirement | Evidence | Status |
|---|---|---|
| JWT requirement and enforced scopes | Server startup/auth/session and CLI token tests | Verified |
| Per-workload fabric identity and internal mTLS | Node fabric authorization, node API, and internal client tests | Verified |
| CLI contexts and expiring login | Context/login/auth-token command tests | Verified |
| Slack webhook lifecycle and notifications | Webhook operator plus server automation tests | Verified |
| OpenAPI-generated TypeScript and typed streaming clients | API drift check, package tests, and UI production build | Verified |

### CLI

| Requirement | Evidence | Status |
|---|---|---|
| `config init\|validate` | CLI command/config tests | Verified |
| Service lifecycle command set | CLI services/up/rollout/deployment tests | Verified |
| Cluster formation, inspection, maintenance, and upgrade command set | CLI cluster formation/join/removal/upgrade tests | Verified |
| Context, daemon, exec, logs, and dead-letter command set | CLI command/context/log/exec and daemon CLI tests | Verified |

### UI

| Requirement | Evidence | Status |
|---|---|---|
| Home nodes, stats, config, and services | Panel route plus cluster/services package tests | Verified |
| Cluster overview and logs | Cluster/logs packages and route registry tests | Verified |
| Service overview, deployments, metrics, logs, volumes, freeze, replicas | Services/metrics/logs packages and production build | Verified |
| Metrics, disks, traffic, and HTTP logs | Metrics/ingress/logs packages and production build | Verified |
| Log query pills, histogram, detail, and viewer behavior | Logs package logic/component tests | Verified |
| Slack, confirmations, toasts, and timeline | Services/kit/charts package tests and production build | Verified |
| Firewall policy editor, effective rules, and dry-run | Firewall package, generated client, and server planner tests | Verified |

### Operations and packaging

| Requirement | Evidence | Status |
|---|---|---|
| Static executables, admin/daemon image, and public NixOS module | Flake outputs, `nixosModules.default`, `services.maestro`, and release workflow bundle checks | Acceptance pending |

## Cutover blockers

The rewrite is not production-ready until all of the following are attached to
the exact release commit:

1. The release workflow passes, including real Docker/containerd,
   real-process one/three-node cluster, encrypted cutover, MinIO/KES, Nix, and
   dependency gates.
2. Sandbox forms three healthy nodes, advertises its intended Tailscale
   gateway, exposes the panel over the gateway MagicDNS name, and passes the
   service/mesh/firewall/upgrade smoke checks.
3. `docs/rehearsal-evidence.md` is completed from a production snapshot in an
   isolated staging rehearsal.
4. The final release tag is built from the rehearsed commit and its static
   checksums and image archives are verified.
