# Maestro

Maestro is a lightweight deployment system for running containerized services on
your own machines. It builds images, schedules replicas across nodes, routes traffic
through Traefik, and keeps services healthy, while staying small enough to run on a
single laptop or across hundreds of nodes.

- One `maestro` binary: control plane, node agent, CLI, telemetry, and log driver.
- Highly available control plane: Raft (openraft) over mTLS gRPC with redb storage.
- containerd through its gRPC API, CNI bridge networking, and a WireGuard mesh
  between nodes.
- Per-service CPU, memory, and pids limits, plus a cgroup cap on all workloads so
  they can never starve Maestro, Traefik, or telemetry.
- Zero-downtime rollouts, health checks, automatic restarts, and rescheduling off
  failed nodes.
- Built-in service discovery (`<service>.maestro.internal`), structured log search,
  encrypted secrets, and token-protected API access.

See [MAESTRO_V3.md](MAESTRO_V3.md) for the architecture and roadmap.

## Requirements

- Linux with systemd and cgroup v2, containerd 2.x, BuildKit, CNI plugins,
  `wireguard-tools`, and `nftables` (the NixOS module and the dev VM provide these).
- macOS for development, using [Lima](https://lima-vm.io) (`brew install lima`).
- To build from source: Rust 1.97, `protoc`, and a C/C++ toolchain.

```bash
cargo build --release --locked -p maestro   # -> target/release/maestro
```

## Quick start (local development)

`maestro dev up` creates a Lima VM with containerd, BuildKit, and CNI, builds the
Linux binary inside it from this checkout, initializes a single-node cluster, and
starts the node and telemetry units. It also saves a `dev` CLI context.

```bash
maestro dev up                 # create or start the VM and (re)install maestro
maestro dev status
maestro rollout -f examples/basic/maestro.services.jsonc            # preview
maestro rollout -f examples/basic/maestro.services.jsonc --apply    # deploy
maestro services
maestro replicas
curl -H "Host: web.localhost" http://127.0.0.1:8080/
maestro logs --service web -n 50
maestro dev reload             # rebuild and restart after code changes
maestro dev down
```

The API is forwarded to `127.0.0.1:3000` and ingress to `127.0.0.1:8080`.

## Services file

`maestro.services.jsonc` (JSONC, camelCase) declares every service:

```jsonc
{
  "services": {
    "api": {
      "name": "API",
      // Either a prebuilt image...
      "image": "ghcr.io/acme/api:1.4.0",
      // ...or a build from git (built with BuildKit on the cluster):
      // "build": {
      //   "repo": "https://github.com/acme/api.git",
      //   "branch": "main",
      //   "dockerfile": "Dockerfile",
      //   "registry": "ghcr.io/acme",          // required for multi-node clusters
      //   "env": { "items": { "NODE_ENV": "production" } },   // build args
      //   "secrets": { "items": { "NPM_TOKEN": "..." } }      // BuildKit secrets
      // },
      "ingress": { "host": "api.example.com", "hosts": ["www.example.com"], "port": 8080 },
      "deploy": {
        "replicas": 3,
        "command": { "command": "/app/server", "args": ["--port", "8080"] },
        "exposePorts": [9090],
        "healthcheckPath": "/health",
        "healthcheckInterval": 10,
        "maxRestarts": 10,
        "env": { "items": { "DATABASE_URL": "postgres://..." } },
        "resources": {
          "cpu": 4,              // limit in cores (default 4)
          "memory": "50%",       // limit: bytes, 512Mi, 4Gi, or a % of node memory (default 50%)
          "request": { "cpu": 0.25, "memory": "512Mi" }   // used for scheduling
        },
        "oomPriority": "high"    // low | normal | high: who the kernel kills first
      }
    }
  }
}
```

- Env values and build secrets are encrypted before they reach the cluster store and
  are masked in API responses.
- Every replica gets `MAESTRO_NODE_ID`, `MAESTRO_REPLICA_ID`, `MAESTRO_REPLICA_INDEX`,
  `MAESTRO_AFFINITY_TOKEN`, and, when the service has one ingress host,
  `MAESTRO_INGRESS_HOST` and `MAESTRO_INGRESS_PORT` (a preview gets its preview host).
  Env and secret values may use `${{ MAESTRO_INGRESS_HOST }}` and
  `${{ MAESTRO_INGRESS_PORT }}`, including values loaded from `aws-secret://`; builds
  receive both as build args.
- `maestro rollout` shows created, updated, and unchanged services; `--apply` stores
  the specs and queues one deployment per changed service. Services missing from the
  file are reported but never deleted implicitly.
- A deployment is ready when all replicas pass health checks. The previous
  deployment is then drained and stopped, and the newest 20 deployments are kept.

## Resource isolation

Every node runs three systemd slices:

| Slice | Contains | Policy |
|---|---|---|
| `maestro-system.slice` | maestro, telemetry, Traefik, admin UI | `CPUWeight=1000`, guaranteed memory reserve |
| `maestro-workloads.slice` | every service replica | `CPUWeight=100`, `MemoryMax = total - reserve`, `MemoryHigh` at 90% |
| `maestro-builds.slice` | BuildKit | `CPUWeight=50`, capped memory |

Replicas get hard per-container CPU, memory (no swap), and pids limits. CPU limits
are ceilings, not reservations: idle capacity is shared and the system slice wins
under contention. The scheduler bin-packs on requests, so limits can overcommit
safely. The default system reserve is `max(2 GiB, 10%)` of memory and one CPU.

## Cluster config

Every node reads the same `maestro.jsonc` (a file or an `aws-secret://` source).
Maestro never generates secrets, so these come from the config:

- `cluster.joinSecret` (all nodes) lets nodes join and lets a joining node verify it
  reached the real cluster.
- `cluster.voters` (all nodes) lists the voter addresses. The first one creates the
  cluster; everyone else joins through any of them.
- `encryptionKey` (voters) encrypts env values and secrets in the cluster store
  (AES-256 with an Argon2-derived key).
- `jwtSecretKey` (voters) signs API tokens (HS256). You can mint tokens elsewhere with
  it as long as they carry `sub` and `exp`, and `exp` is at most 24 hours after `iat`
  (or after now when `iat` is absent).

Every other field has a default.

```jsonc
{
  "$extends": "common.jsonc",   // optional base file; objects merge, other values replace
  "encryptionKey": "…",         // required on voters
  "jwtSecretKey": "…",          // required on voters
  "cluster": {
    "name": "prod",
    "cidr": "10.200.0.0/16",
    "joinSecret": "…",
    "voters": ["10.20.0.11", "10.20.0.12", "10.20.0.13"]   // port 7400 unless given
  },
  "node": { "labels": { "zone": "us-west-2a" }, "reserve": { "cpu": 1, "memory": "4Gi" } },
  "ingress": { "ports": [80, 443] },
  "defaults": {
    "resources": { "cpu": 4, "memory": "50%", "request": { "cpu": 0.25, "memory": "512Mi" } }
  },
  "volumeRoots": ["/var/lib/maestro-volumes", "/data"]   // the default
}
```

Each node is allocated a `/24` from `cluster.cidr` when it joins.

A service's `volumes[].hostPath` must be a directory inside one of `volumeRoots`. Each node
checks this after resolving symlinks, so a volume can never reach the node's keys, `/etc`,
or the container runtime.

## Networking

- Replicas run on a per-node CNI bridge (`maestro0`); nodes are connected by a
  WireGuard mesh (`maestro-wg`, UDP 51820), so every replica IP is reachable from
  every node. Traffic to the internet is masqueraded by the node; traffic inside the
  cluster CIDR keeps its source address.
- Traefik runs on each node, routes `Host()` rules to healthy replicas anywhere in the
  cluster, retries failed connections on another replica, and actively health-checks
  backends.
- DNS on the bridge gateway answers `<service>.maestro.internal`,
  `<service>.<cluster>.maestro.internal`, and per-replica names; other names are
  forwarded upstream.

## Logs

Container output goes through `maestro-log-driver` into rotated files and is indexed
by the telemetry process (DuckDB with daily Parquet rollover).

```bash
maestro logs --service api -q 'level:error "upstream timeout"'
maestro logs -q '@http.status_code:[500 TO 599] AND -message:*health*' -n 200
```

Queries support free text, quoted phrases, `service:`, `deployment:`, `replica:`,
`stream:`, `level:`, `@attribute` filters for JSON lines, numeric comparisons and
ranges, `AND`/`OR`/`NOT`/`-`, and `*`/`?` wildcards.

## CLI

```text
maestro dev up|reload|down|status        local cluster in a Lima VM
maestro server provision                 create or join the cluster from the config (NixOS runs this)
maestro server init                      create a single-node cluster on this node
maestro join --config SRC --advertise IP:7400 [--role voter|worker]
maestro agent                            run a node (systemd unit)
maestro telemetry                        run the telemetry process (systemd unit)
maestro token create [--config SRC]      print an API token signed with jwtSecretKey (max 24h)
maestro context set|use|ls|remove        manage API endpoints and tokens
maestro context login [--ttl 24h]        sign a token with $JWT_SECRET_KEY and save it to the active context
maestro cluster upgrade [VERSION]        rolling upgrade of every node, without logging in to them
maestro rollout [-f file] [--apply]
maestro services | replicas | nodes
maestro deploy <service>
maestro logs [--service S] [-q QUERY] [-n N]
```

The CLI reads `~/.config/maestro/contexts.json` (override with
`MAESTRO_CONTEXTS_FILE`); `--api` and `--token` (or `MAESTRO_API`, `MAESTRO_TOKEN`)
override the active context.

## Multi-node clusters

A Maestro cluster has 1, 3, or 5 **voters** and any number of **workers**.

- **Voters** run the control plane (Raft over mTLS gRPC with redb storage, the API,
  the scheduler, the deployer, and the build queue) and also run workloads.
- **Workers** run workloads only. They hold no cluster state and receive exactly the
  replica plans, routes, DNS records, and mesh peers they need from the leader.

Use three voters for production; the cluster stays writable while a majority of
voters is up. Workers can scale to hundreds of nodes.

### Ports

| Port | Protocol | Between | Purpose |
|---|---|---|---|
| 7400 | TCP | all nodes and voters | gRPC: Raft, joins, and the agent control stream (mTLS) |
| 51820 | UDP | all nodes | WireGuard mesh carrying replica traffic |
| 3000 | TCP | localhost on voters | token-protected HTTP API for the CLI |
| 80, 443 | TCP | public or load balancer | Traefik ingress |

Ports are fixed; nothing is randomly assigned. The NixOS module opens 7400, 51820,
and the ingress ports when `openFirewall` is set. On AWS, allow 7400/tcp and
51820/udp from the cluster's security group.

### Forming a cluster

Formation needs no manual steps. Start every node with the same config and its own
`--advertise` address (the NixOS module runs this on first boot):

```bash
sudo maestro server provision --config aws-secret://maestro/prod/config.json \
  --advertise 10.20.0.12:7400 --role voter    # or --role worker
```

- The first address in `cluster.voters` creates the cluster (CA, node certificate,
  one-voter Raft group), unless another listed voter already has one; then it joins.
- Every other node joins through the listed voters, retrying while they come up. It
  first asks a voter for the cluster CA along with an HMAC of it under the join
  secret, so it never trusts a node that doesn't know the secret, then joins over TLS
  pinned to that CA with a short-lived grant signed with the same secret.
- The leader assigns a node id and the next free `/24` from the cluster CIDR, records
  the node, and returns its certificate. Voters also receive the CA key.
- If a node joins at an address another node used before (an instance replaced with
  a fresh disk), the old node is dropped from Raft and retired.
- The leader adds joining voters as Raft learners, waits for them to catch up, and
  promotes them. Run `maestro nodes` to check the cluster.

A node with an existing data dir skips all of this and just starts.

### How the control plane scales

Only desired state goes through Raft: services, deployments, encrypted secrets,
assignments, and node records. Heartbeats, replica health, and status never touch
consensus. Every agent holds one long-lived stream to the leader, sends its replica
statuses (on change and every 5 seconds), and receives plans, routes, DNS records,
and mesh peers only when they change. A follower that receives an agent redirects it
to the leader.

### Failure handling

- **Agent restart:** running containers keep running and are adopted when the agent
  comes back. Container logs keep flowing because the log driver is a child of the
  containerd shim.
- **Node loss:** a node whose stream has been gone for 30 seconds is marked
  unreachable. Its replicas are removed from routing and rescheduled onto healthy
  nodes. When the node returns, its agent stops the stale copies.
- **Leader failover:** a new leader waits the same 30-second grace period before
  judging any node, so failovers never trigger rescheduling. Agents reconnect with
  backoff.
- **Ingress during failures:** Traefik retries failed connections on another replica
  with a 1-second dial timeout and actively health-checks backends, so a hard node
  loss costs at most a handful of requests.

### Builds in multi-node clusters

Set `build.registry` for every service that Maestro builds. The building node pushes
`<registry>/<service>:<deployment>`, and other nodes pull it. Registry credentials
must currently be configured on each host.

### Local multi-node testing

`dev/lima/maestro-node.yaml` creates additional Lima VMs on Lima's shared `user-v2`
network next to the `maestro-dev` VM. Initialize `maestro-dev` with its `user-v2`
address as `--advertise`, copy the Linux binary to the new VM, and join it.

## Deploying on NixOS

The flake exports `nixosModules.default`. Every node can use the same module
settings (plus `role = "worker"` on workers); the cluster forms on its own. `advertise`
defaults to the node's address on the route to `cluster.voters`.

```nix
{
  inputs.maestro.url = "github:poudels14/maestro-cloud/main";

  outputs = { maestro, nixpkgs, ... }: {
    nixosConfigurations.node1 = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        maestro.nixosModules.default
        {
          services.maestro = {
            enable = true;
            configFile = "aws-secret://maestro/prod/config.json";
            # role = "worker";   # voters are the default
          };
        }
      ];
    };
  };
}
```

With `JWT_SECRET_KEY` set to the cluster's `jwtSecretKey` (the CLI prompts for it
otherwise), save a token to a context:

```bash
maestro context set prod --api https://maestro-prod-1.<tailnet>.ts.net
maestro context login        # valid for 24h; run it again when it expires
```

`maestro token create` prints a token instead, signed with `JWT_SECRET_KEY` or with the
key from `--config aws-secret://…`.

The module configures containerd, BuildKit (in the builds slice), the three slices,
the node and telemetry units, WireGuard, IP forwarding, and firewall rules
(`openFirewall`, `ingressPorts`). The admin UI is built from the source tree once per
version and runs on voters. It listens on the node's loopback at port 7490, where
workload containers can't reach it, and is published on the tailnet at a fixed name
per voter: `https://maestro-<cluster>-<n>.<tailnet>.ts.net`, where `<n>` is the voter's
position in `cluster.voters`. Enable HTTPS certificates for the tailnet, and use an
ephemeral, reusable auth key so a rebuilt node takes its old name back. Tailscale runs
in its own network namespace and serves only the panel, so nothing else on the node is
reachable from the tailnet. An SSH or SSM tunnel to port 7490 also works. Anyone who can reach it can operate the cluster, except
that creating or updating services (`PUT /api/services/…`, rollout, `services up`)
needs an API token, because a service config can read any secret the nodes can. Other
host names must be listed in the admin container's `MAESTRO_ADMIN_HOSTS`.

Upgrade the whole cluster from the CLI; no node access is needed:

```bash
maestro cluster upgrade            # every node to the latest commit of its flake input
maestro cluster upgrade 3.1.0+abc1234   # or until every node reports this version
```

Nodes go one at a time (workers, then follower voters, then the leader). Each is
drained, runs `nix flake update` and `nixos-rebuild boot` on `/etc/maestro`, reboots,
and must rejoin before the next one starts; deploys are frozen until the run finishes.
Node versions read `<version>+<git commit>`.

API tokens last 1 hour by default and at most 24 hours. Use one with the CLI over the
tailnet, or through an SSH tunnel to the localhost API:

```bash
TOKEN=$(ssh node1 sudo maestro token create --config aws-secret://maestro/prod/config.json)
maestro context set prod --api https://maestro-prod-1.<tailnet>.ts.net --token "$TOKEN"
maestro context use prod
```

## Status

The roadmap is tracked in [MAESTRO_V3.md](MAESTRO_V3.md#phases). Not yet available:
PR previews, `exec`, egress rules, Datadog and S3 log sinks, `aws-secret://` sources,
private registry credentials, local-directory builds, named image users
(`USER node`), node removal, and voter removal.
