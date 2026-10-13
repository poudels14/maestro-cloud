# Maestro v2

A rewrite of Maestro focused on a clean architecture, a control plane that scales to
1000 nodes, simple setup (including local development on macOS), and resource
isolation that keeps nodes alive when workloads misbehave.

## Goals

- Multi-node clusters with automatic HA: 3–5 voters today, 100–1000 nodes later.
- Consensus traffic that grows with deploys, not with node count or time.
- One binary, near-zero setup: `maestro dev` on macOS/Linux, `maestro server init` +
  `maestro join` in production.
- Workloads can never starve Maestro, Raft, Traefik, or telemetry of memory or CPU.
- Production runtime is containerd (gRPC API); ingress is Traefik.
- Small, focused crates with two reconcile loops instead of god-files.

## Non-goals

- Kubernetes API compatibility.
- A custom ingress proxy. Traefik stays.
- Docker as a production runtime.
- Keeping v1 code paths (standalone mode, legacy migration, nerdctl shell-outs,
  manual join flow).

## Why v1 struggles

| Area | v1 behavior | Effect |
|---|---|---|
| etcd reads | Leader scans all of `/maetro/services/` every 1s, the leader tick scans it 3× every 5s, every probe scans it every 5s; all linearizable, all with values | Bandwidth grows with services × history × nodes |
| etcd writes | Heartbeats (2 puts / 5s / node), stats (lease + put / 10s / node), disks (30s), unconditional `unschedulable` (5s) | Constant Raft churn unrelated to deploys |
| Data growth | Deployment history and placements never pruned; no defrag; no compaction in standalone | Every scan gets slower over time |
| Startup | Builds probe (full Rust release build) and admin UI images from source on every start; up to 8 system containers | Slow, fragile, needs `--project-dir` |
| Setup | Required `--subnet`, 3 secrets, predeclared node map with one `master`, random ports, two join flows, kebab + camel configs, ~25 duplicate CLI flags | Hard to stand up, impossible on macOS |
| Isolation | No CPU/memory/pids limits anywhere; scheduler balances by replica count | One leaking service can OOM the control plane |
| Code | 5.6k-line `server/mod.rs`, 1.6k-line `run()`, standalone vs cluster branches in ~30 places, docker/nerdctl copy-paste | Hard to change safely |

## Architecture

### Processes per node

| Unit | Nodes | Responsibility |
|---|---|---|
| `maestro agent` (systemd, `maestro-system.slice`) | all | containerd client, CNI networking, replica supervisor, health checks, DNS, Traefik file-provider writer, egress firewall. On voters also: Raft + redb store, API, scheduler, deploy reconciler, build queue |
| `maestro telemetry` (systemd, `maestro-system.slice`, own `MemoryMax`) | all | Log/metrics ingest, DuckDB store, query API, Parquet rollover, S3 backup, Datadog sink |
| Traefik (container, `maestro-system.slice`) | workload nodes | Public ingress, configured by file provider |
| Admin UI (container, `maestro-system.slice`) | voters | TanStack Start UI reachable from the tailnet; API access only via the mounted Unix socket |
| `buildkitd` (systemd, `maestro-builds.slice`) | build-capable nodes | Image builds |

Both Maestro units are the same binary.

The admin UI stays a TanStack Start app in its own system container
(`maestro-admin:<version>`, `maestro-system.slice`) so it is reachable from the tailnet
while the controller API is not. It talks to the API only through the agent's Unix
socket (`/run/maestro/api.sock`), which is bind-mounted into the admin container; the
API's TCP listener stays on `127.0.0.1:3000` for the local CLI. The admin image is built
on the node with BuildKit only when the tag for the running version is missing, then
cached. No other image is built at startup.

### Node roles

- `voter`: runs Raft and the control plane, plus workloads unless `--no-workloads`.
- `agent`: runs workloads only. Holds no Raft state and never sees other nodes' data.

A cluster has 1, 3, or 5 voters. Every other node is an agent. A single-node install is
a one-voter cluster; there is no separate standalone mode.

```
                 ┌──────────── voters (3–5) ────────────┐
                 │  agent + raft(redb) + API/scheduler  │
                 │  leader ◄─ raft ─► followers         │
                 └───────▲──────────────────▲───────────┘
          agent stream   │                  │  agent stream
      (assignments down, │                  │  (heartbeat/health/status up)
       status up)        │                  │
   ┌─────────────┐  ┌────┴────────┐   ┌─────┴───────┐
   │ agent node  │  │ agent node  │…  │ agent node  │   up to ~1000
   │ containerd  │  │ containerd  │   │ containerd  │
   │ traefik     │  │ traefik     │   │ traefik     │
   └─────────────┘  └─────────────┘   └─────────────┘
```

### State model

State is split by how often it changes. Only desired state goes through consensus.

| Class | Examples | Write rate | Storage |
|---|---|---|---|
| Desired | services, deployments, encrypted env/secrets, assignments, node membership, subnet allocations, cluster settings | Per deploy or membership change | Raft log → redb state machine on voters |
| Observed | heartbeats, replica status, health, node stats, disk usage, OOM/restart events | Every few seconds | Leader memory only. Agents stream it; a new leader rebuilds it from agents' first reports |
| Telemetry | logs, metrics, access logs | Continuous | Per node in DuckDB, shipped to S3/Datadog |

On leader change, the new leader waits a grace period (2× heartbeat interval) before
treating missing observed state as failure, so failovers never trigger rescheduling.

## Store: openraft + redb

- `openraft` 0.9 (stable line) with redb-backed log storage and state machine.
- Writes are typed commands, e.g. `PutService`, `DeleteService`, `CreateDeployment`,
  `SetDeploymentStatus`, `SetAssignments`, `AddNode`, `RemoveNode`, `AllocateSubnet`,
  `PutSecretBlob`, `PruneHistory`.
- Commands are validated on the leader before proposal; `apply` is deterministic and
  never fails on valid input.
- Each applied command bumps a global `version`. The state machine stores per-row
  `version` and keeps secondary indexes (e.g. deployments by status, assignments by
  node), so reads are indexed lookups, never prefix scans.
- Reads on the leader use the local state machine after a read-index check when they
  must be linearizable; everything else (UI lists, agent catch-up) reads locally.
- Change notification: an in-process broadcast of `(version, changed keys)` after
  apply. Subscribers resume with `subscribe(from_version)`; if the version is older
  than the retained change window they receive a fresh snapshot.
- Raft snapshots are a logical export of all tables (indexes are rebuilt on install);
  the log is compacted after each snapshot. Raft log and state machine live in separate
  redb files (`raft-log.redb`, `state.redb`).
- Retention is part of the model: deployment history keeps the last N per service
  (default 20) and is pruned by a `PruneHistory` command, not by a background sweep.

### redb tables

| Table | Key | Value |
|---|---|---|
| `meta` | `&str` | cluster id, version, settings |
| `nodes` | node id | `NodeRecord` (name, role, endpoint, subnet, capacity, labels, state) |
| `services` | service id | `ServiceSpec` + version |
| `deployments` | (service id, deployment id) | `Deployment` (spec snapshot, status, timestamps) |
| `deployments_by_status` | (status, service id, deployment id) | `()` |
| `assignments` | (node id, replica id) | `Assignment` |
| `secrets` | (scope, id) | encrypted blob |
| `subnets` | CIDR | node id |

## Agent ↔ control protocol

- Internal transport: gRPC (tonic) over mTLS on one fixed port (default `7400`).
  Raft RPCs ride the same server.
- Each agent opens one bidirectional stream to the leader (any voter redirects to the
  current leader).
  - Down: its own assignments as `(version, full set)` on connect, then deltas; the
    routing table its Traefik needs (service → backend nodes), as deltas.
  - Up: heartbeat (5s), replica status transitions, health results, node stats,
    OOM/restart events.
- Agents reconnect with jittered exponential backoff and a resume version.
- Leader cost at 1000 nodes: ~200 heartbeat msgs/s held in memory. No consensus writes.

## Runtime: containerd gRPC

- `containerd-client` for images, containers, tasks, events, and exec. No `nerdctl`
  shell-outs (nerdctl stays installed for manual debugging only).
- Containers live in the `maestro` containerd namespace.
- Networking: CNI plugins invoked directly (`bridge` + `host-local` IPAM +
  `portmap`/`firewall`), one bridge per node on the node's subnet.
- OCI spec generated by Maestro: cgroup parent, CPU/memory/pids limits, mounts, env,
  user, `oom_score_adj`.
- Task exit and OOM events come from the containerd event stream, not polling.
- A `Runtime` trait isolates containerd so tests use an in-memory implementation.

## Resource isolation

```
maestro-system.slice     CPUWeight=1000, MemoryMin=<reserve>
                         agent, telemetry (own MemoryMax), traefik, containerd
maestro-workloads.slice  CPUWeight=100, MemoryMax=total − reserve, MemoryHigh≈0.9×MemoryMax
                         every service container (cgroup parent)
maestro-builds.slice     CPUWeight=50, MemoryMax=<build cap>, 1–2 concurrent builds
```

- Per-container defaults (limits, overridable per service):
  `cpu: 4`, `memory: 50% of node memory`, plus a pids limit.
- No CPU quota on the workloads slice: CPU is compressible and the 10:1 weight gives
  system processes priority under contention while letting workloads use idle cores.
- Memory is hard-capped at the slice. `MemoryHigh` throttles and reclaims before the
  kernel OOM-kills inside the slice; system processes are never OOM candidates.
- `oomPriority` per service maps to `oom_score_adj`, so expendable services die first.
- System reserve default: `max(2 GiB, 10% of memory)` and 1 CPU worth of weight
  headroom; configurable per node.
- Scheduler bin-packs on requests, never on limits. Default requests:
  `cpu: 0.25`, `memory: 512Mi`. Replicas that don't fit are reported unschedulable.
- Cgroup throttling (`cpu.stat`) and memory pressure are surfaced in the UI.

## Networking, DNS, ingress

- One cluster CIDR (default `10.200.0.0/16`); each node is auto-allocated a `/24` at
  join through a `AllocateSubnet` command. No per-node subnet configuration.
- Fixed default ports: `7400` (internal gRPC + Raft), `3000` (API/UI), `80/443`
  (ingress), `51820/udp` (WireGuard mesh). No random ports.
- DNS: in-process `hickory-dns` server on each node answering
  `*.<cluster>.maestro.internal` from the agent's routing table; everything else is
  forwarded to the host resolvers. Replaces CoreDNS and the Python proxy.
- Ingress: Traefik with the file provider. The agent atomically writes the dynamic
  config from its routing table. Traefik never talks to the store.
- Cross-node traffic: a kernel WireGuard mesh. Each node has a `maestro-wg` interface;
  at join the control plane records the node's WireGuard public key and endpoint, and
  every agent configures peers and routes the other nodes' `/24` container subnets over
  the mesh. This gives a flat, encrypted L3 network for any protocol, so DNS keeps
  returning replica IPs and Traefik load-balances directly to remote replicas. Nodes
  need one UDP port (default `51820`) open between them; no EC2 source/dest-check
  changes are required. The v1 per-node Traefik gateway is not carried over.
- Tailscale and Cloudflare tunnels remain optional add-ons, phase 4.

## Builds

- Builds are a control-plane queue; the leader dispatches each build to a
  build-capable node.
- BuildKit runs as `buildkitd` in `maestro-builds.slice`; Maestro drives it via
  `buildctl` initially (gRPC later if it pays off). Depot stays optional.
- Multi-node builds push to `build.registry`; registry credentials are stored as
  encrypted cluster secrets and handed to agents, instead of per-host setup.

## Telemetry

- Container stdout/stderr use containerd's `binary://` log URI pointing at
  `maestro log-driver`, which the containerd shim runs as a child. It writes
  size-rotated JSON-lines files per container. The telemetry process tails those files
  with checkpointed offsets, so logs keep flowing while the agent or telemetry restarts
  or upgrades.
- Telemetry keeps one store: DuckDB with Parquet rollover (the v1 query language,
  search, and traffic analytics carry over). The SQLite spool is removed; sink
  cursors live in DuckDB.
- Queries from the UI/CLI go to the API, which fans out to the telemetry process on
  the relevant nodes and merges results.
- Datadog and S3 backup sinks carry over.

## PR previews

v1 previews suffer from churn and inconsistent routing. Root causes found in v1:
the preview version hashes `HashMap`-ordered env (likely a redeploy on every poll);
routes and DNS are emptied whenever heartbeats look stale under etcd load; teardown
removes service data before routes; one failing etcd read aborts the whole poll or
leader tick; env reads that fail silently return empty values; branch watcher and
PR watcher both queue builds for the same push; GitHub calls have no timeout.

v2 design (decisions approved):

- Previews are first-class store entities:
  `Preview { base_service, repo, pr_number, head_ref, head_sha, state, spec_hash }` with
  state `open | closing(closed_at) | removed`. The preview service spec is derived
  deterministically (ordered maps, canonical JSON); a new deployment is queued only
  when `head_sha` or the derived `spec_hash` changes.
- One source watcher on the leader handles both `build.watch` branches and PRs, keyed
  by `(repo, sha)`, so each push queues exactly one build.
- Polling only (no webhooks). Requests use timeouts, ETag conditional requests,
  `Retry-After` and secondary rate-limit handling, and per-repo backoff. A failure
  for one repo or PR never aborts the poll for others.
- Env and secret resolution failures are errors that fail the deployment visibly;
  they never produce an empty environment.
- Routing safety: a service's backend set is never emptied because observed state
  is stale. Backends are removed only when their replicas are confirmed gone or
  unhealthy, or after node expiry while healthy replacements exist. Each service's
  routing is computed independently, so one bad service cannot block others.
- Teardown is one store write that marks the preview removed; agents stop replicas
  and the same routing version removes its routes and DNS names.
- DNS: public preview hostnames use a single manual wildcard record
  (`*.preview.<domain>`) to the cluster or Cloudflare tunnel; Maestro only manages
  Traefik `Host()` rules. Internal names come from the in-process DNS server fed by
  the agent's versioned routing table (no hosts files, no reload lag).
- GitHub reporting: the sticky PR comment plus GitHub Deployment statuses
  (queued, building, ready, failed, inactive), written only on state transitions.
- Isolation: previews are scheduled like production services;
  `maxConcurrentPreviews` remains the only preview-specific limit.

## Security

- `maestro server init` creates the cluster CA, the cluster secret, and the data
  encryption key in the data dir. They can instead be imported from
  `aws-secret://...`.
- All internal traffic is mTLS with cluster-issued node certificates.
- Joining: `maestro server init` prints a join token (cluster id + CA fingerprint +
  HMAC secret). `maestro join <addr> --token <token>` verifies the CA, receives a
  node certificate, and is admitted as agent or voter.
- API auth: JWT signed with a key derived from the cluster secret.
- Secrets are encrypted before entering the Raft log.

## Configuration

Two files, one naming style (camelCase), and no CLI flags that duplicate config.

`maestro.jsonc` (optional; everything has a default):

```jsonc
{
  "cluster": { "name": "prod", "cidr": "10.200.0.0/16" },
  "node": { "labels": { "zone": "us-west-2a" }, "reserve": { "memory": "4Gi" } },
  "ingress": { "ports": [80, 443] },
  "defaults": {
    "resources": { "cpu": 4, "memory": "50%", "request": { "cpu": 0.25, "memory": "512Mi" } }
  },
  "egress": { "deny": [] },
  "datadog": { "apiKey": "..." },
  "logBackup": { "bucket": "...", "region": "us-west-2" },
  "github": { "token": "..." },
  "depot": { "token": "..." }
}
```

`$extends` stays for the common-plus-node-override pattern.

`maestro.services.jsonc` keeps the v1 shape, plus:

```jsonc
"deploy": {
  "resources": { "cpu": 2, "memory": "8Gi", "request": { "cpu": 1, "memory": "2Gi" } },
  "oomPriority": "high"   // "low" | "normal" | "high"
}
```

CLI flags on the daemon: `--config`, `--data-dir`, `--role`, `--no-workloads`.

## CLI

```
maestro dev [up|down|status]          local cluster in a Lima VM (macOS) or directly (Linux)
maestro server init                   create a cluster on this node, print join token
maestro join <addr> --token <t>       join as agent (default) or --role voter
maestro agent | maestro telemetry     long-running units (systemd)
maestro nodes ls|drain|remove
maestro services ls|rollout [--apply]|redeploy|cancel
maestro logs | maestro exec
maestro contexts set|use|ls|login
maestro config validate
maestro upgrade
```

## Local development

- Linux: `maestro dev up` runs agent + telemetry as a one-voter cluster against the
  host containerd, with resources under user slices when systemd is available.
- macOS: `maestro dev up` creates a Lima VM (vz backend, systemd, cgroup v2,
  containerd + buildkitd + CNI preinstalled), installs the Linux `maestro` binary,
  and starts the same units inside it. Lima forwards ports, so the CLI and UI use
  `localhost:3000` and apps are reachable at `http://<host>.localhost:8080`.
  The project directory is mounted with virtiofs for local builds.
- Developing Maestro itself on macOS: build the Linux binary with
  `cargo zigbuild --target aarch64-unknown-linux-gnu` or inside the VM;
  `maestro dev reload` swaps the binary and restarts the units.

## Workspace layout

```
crates/
  maestro-core        spec types (service, node, resources), ids, config parsing and validation
  maestro-store       openraft type config, redb log + state machine, commands, subscriptions
  maestro-proto       tonic/prost definitions for agent stream and Raft transport
  maestro-runtime     Runtime trait, containerd implementation, CNI, OCI spec, cgroups
  maestro-agent       replica supervisor, health checks, routing table, Traefik writer, DNS, firewall
  maestro-control     API (axum, one module per resource), scheduler, deploy reconciler, build queue
  maestro-telemetry   collector, DuckDB store, query language, sinks
  maestro-cli         client commands
  maestro             binary: role wiring
```

The v1 `controller/` crate stays untouched until v2 reaches parity, then is removed.

## Phases

### Phase 1: foundation (single node, macOS dev)

- [x] Workspace scaffold, `maestro-core` spec types, resource parsing
- [x] `maestro-core`: cluster config loading (`maestro.jsonc`, `$extends` for files; `aws-secret://` in phase 2)
- [x] `maestro-store`: redb state machine, mutations, indexes, subscriptions
- [x] `maestro-store`: openraft integration (single voter), snapshots, log compaction
- [x] `maestro-runtime`: containerd images/containers/tasks/events, CNI bridge, OCI limits, slices
- [x] `maestro-agent`: assignment reconcile loop, supervisor, health checks
- [x] `maestro-control`: services/deployments API, deploy reconciler, request-based scheduler
- [x] `maestro dev` with Lima on macOS (`maestro-v2 dev up|reload|down|status`)
- [x] Milestone: deploy an image-based service locally on macOS with limits enforced.
- [ ] Named image users (`USER node`) need `/etc/passwd` lookup from the image rootfs
- [ ] Private registry credentials for pulls (Transfer API auth stream)

The v2 binary is `maestro-v2` until v1 is removed, so both can build in one workspace.

### Phase 2: single-node parity

- [x] Traefik file-provider ingress (system container, host network) and routing table
- [x] In-process DNS (`hickory-proto`) on the bridge gateway for `*.maestro.internal`, upstream forwarding
- [x] Builds via BuildKit (`buildctl`, containerd worker in the `maestro` namespace, `maestro-builds.slice`), bounded build queue, registry push
- [ ] Build sources: GitHub token from config, local context upload (`maestro up`), external secret sources (`aws-secret://`)
- [x] `maestro log-driver` (containerd binary log URI, rotated JSON-lines files)
- [x] API served on `/run/maestro/api.sock` for the admin container
- [ ] Telemetry process tailing driver files into DuckDB, query API
- [x] Admin UI container (TanStack Start) built once per version, API over mounted Unix socket
- [ ] Port the admin UI from v1 API endpoints to the v2 API
- [ ] Secrets encryption, external secret providers, JWT auth, CLI contexts

### Phase 3: multi-node

- [x] Raft over gRPC (mTLS), join tokens, voter add (learner then promote), leader redirect
- [ ] Voter removal and end-to-end 3-voter join test on VMs
- [x] Agent stream protocol (Attach), hub-aggregated observed state, node expiry and leader startup grace
- [x] Reschedule replicas from lost nodes; Traefik retries, dial timeouts, and active health checks
- [x] WireGuard mesh between nodes, subnet allocation at join, agent-owned NAT
- [ ] Routing deltas instead of full tables (needed before ~100 nodes)
- [ ] Drain, remove, rolling upgrades, NixOS module with slices
- [ ] Scale test: 3 voters + simulated 1000 agents

### Phase 4: remaining features and cutover

- [ ] PR previews (see PR previews section), exec, egress rules, Datadog/S3 sinks, Tailscale/Cloudflare add-ons
- [ ] `maestro migrate v1`: export services, deployments, and secrets from v1 etcd
- [ ] Remove `controller/`

## Open items

- Node instance sizes, to tune the default system reserve.
