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

`maestro.jsonc` is optional; every field has a default.

```jsonc
{
  "$extends": "common.jsonc",   // optional base file; objects merge, other values replace
  "cluster": { "name": "prod", "cidr": "10.200.0.0/16" },
  "node": { "labels": { "zone": "us-west-2a" }, "reserve": { "cpu": 1, "memory": "4Gi" } },
  "ingress": { "ports": [80, 443] },
  "defaults": {
    "resources": { "cpu": 4, "memory": "50%", "request": { "cpu": 0.25, "memory": "512Mi" } }
  }
}
```

Each node is allocated a `/24` from `cluster.cidr` when it joins.

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
maestro server init                      initialize a cluster on this node
maestro server join-token --role R       print a join token for a voter or worker
maestro join --server … --token … --advertise IP:7400 [--role voter|worker]
maestro agent                            run a node (systemd unit)
maestro telemetry                        run the telemetry process (systemd unit)
maestro token create                     mint an API token (on a voter)
maestro context set|use|ls               manage API endpoints and tokens
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

1. Initialize the first node (done automatically by the NixOS module when
   `join.server` is unset):

   ```bash
   sudo maestro server init --advertise 10.20.0.11:7400 [--config maestro.jsonc]
   sudo systemctl start maestro
   ```

   This creates the cluster CA, the node certificate, the data encryption key, the API
   signing key, and the join secret in the data dir, then bootstraps a one-voter Raft
   group.

2. Print a join token on any voter. A token admits only the role it was minted for
   and expires after `--ttl` (default 5m):

   ```bash
   sudo maestro server join-token --role worker   # or --role voter
   # mj2.<ca-sha256-fingerprint>.<role>-<expiry>-<signature>
   ```

3. Join other nodes:

   ```bash
   sudo maestro join --server 10.20.0.11:7400 --token "$TOKEN" \
     --advertise 10.20.0.12:7400                # the role comes from the token
   sudo systemctl start maestro
   ```

   The joining node pins the CA by the fingerprint in the token, so it never trusts an
   impostor. The leader checks the secret, assigns a node id and the next free `/24`
   from the cluster CIDR, records the node, and returns its certificate. Voters also
   receive the CA key, the encryption and API keys, and the join secret so they can
   lead. A follower that receives a join returns the leader's address and the client
   retries there.

4. The leader adds joining voters as Raft learners, waits for them to catch up, and
   promotes them. Run `maestro nodes` to check the cluster.

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

The flake exports `nixosModules.default`. The first node initializes the cluster;
other nodes join with a token.

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
            advertise = "10.20.0.11:7400";
            # configFile = ./maestro.jsonc;
          };
        }
      ];
    };
  };
}
```

On the first node, mint a join token and an API token:

```bash
sudo maestro server join-token --role worker --ttl 30m
sudo maestro token create --subject laptop
```

Joining nodes set `join.server`, `join.tokenFile` (a token minted for their role; it is
only used on first boot, so it just has to be fresh then), and optionally
`join.role = "voter"`:

```nix
services.maestro = {
  enable = true;
  advertise = "10.20.0.12:7400";
  join = { server = "10.20.0.11:7400"; tokenFile = "/run/secrets/maestro-join-token"; };
};
```

The module configures containerd, BuildKit (in the builds slice), the three slices,
the node and telemetry units, WireGuard, IP forwarding, and firewall rules
(`openFirewall`, `ingressPorts`). The admin UI is built from the source tree once per
version and runs on voters. Upgrade by updating the flake input and running
`nixos-rebuild switch`.

Reach the API through an SSH tunnel or your private network:

```bash
ssh -L 3000:127.0.0.1:3000 node1
maestro context set prod --api http://127.0.0.1:3000 --token <token>
maestro context use prod
```

## Status

The roadmap is tracked in [MAESTRO_V3.md](MAESTRO_V3.md#phases). Not yet available:
PR previews, `exec`, egress rules, Datadog and S3 log sinks, `aws-secret://` sources,
private registry credentials, local-directory builds, named image users
(`USER node`), node removal, and voter removal.
