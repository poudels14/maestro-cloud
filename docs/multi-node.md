# Multi-node operations

Maestro uses the host's private network for control traffic and a self-managed
WireGuard mesh for workload traffic. Tailscale is an optional operator-access
path; it is not used for cluster membership, consensus, scheduling, or
cross-node workload correctness.

## Network model

Keep these address spaces separate:

- node endpoints are stable, private host addresses such as VPC addresses;
- each workload-capable node owns one explicit, private `/24`; and
- workload subnets do not overlap each other or any control network.

Each workload subnet uses its first host address as the `maestro0` bridge and
authoritative DNS resolver. For example, node subnet `172.22.1.0/24` uses
`172.22.1.1`. Maestro reserves `.2` through `.31` for system infrastructure,
with the Admin API and panel fixed at `.5`. Schedulable system-service replicas
use the remaining addresses in that range, while user workload addresses start
at `.32`.

Workload-capable nodes publish a WireGuard public key, private host endpoint,
and workload subnet. Their node agents converge the exact peer and route set.
Cross-node workload packets therefore route directly between node subnets
through WireGuard. The private key never enters the cluster store.

The default `wireguard` port is UDP `51820`. Choose each node subnet once.
Changing node subnet allocations or cluster ports after bootstrap is not an
online operation.

## Configuration contract

Declare every node that may join before bootstrapping the cluster. The shared
document is identical on every host except for the top-level `node` selector.

```jsonc
{
  "jwt-secret-key": "<at-least-32-bytes>",
  "encryption-key": "<at-least-32-bytes>",
  "cluster": {
    "name": "prod",
    "nodes": {
      "node-1": {
        "hostname": "node-1.internal",
        "endpoint": "10.20.0.11:3000",
        "subnet": "172.22.1.0/24",
        "role": "master"
      },
      "node-2": {
        "hostname": "node-2.internal",
        "endpoint": "10.20.0.12:3000",
        "subnet": "172.22.2.0/24",
        "role": "hybrid"
      },
      "node-3": {
        "hostname": "node-3.internal",
        "endpoint": "10.20.0.13:3000",
        "subnet": "172.22.3.0/24",
        "role": "control-plane"
      }
    },
    "control-allow-cidrs": ["10.20.0.0/24"],
    "ports": {
      "gateway": 3001,
      "store-client": 2379,
      "store-peer": 2380,
      "wireguard": 51820
    },
    "join-secret": "<at-least-32-characters>"
  },
  "node": "node-1"
}
```

An endpoint without a port uses TCP `3000`. The four cluster ports shown above
are also the defaults. They must be nonzero and distinct, and no API endpoint
may reuse one. Maestro validates them from the current config on every daemon
start; it does not discover different free ports on each host.

The topology must have:

- exactly one `master`;
- exactly one or three control-plane-capable nodes in total;
- unique private endpoints and workload subnets; and
- no overlap between host control networks, the cluster pool, or workload
  subnets.

Roles have narrow meanings:

- `master` initializes trust and etcd, participates in the control plane, and
  runs workloads;
- `hybrid` participates in the control plane and runs workloads;
- `control-plane` participates in the control plane without running workloads;
  and
- `worker` runs workloads without a local etcd member.

The `master` designation is only bootstrap authority. Normal lease-backed
election chooses the active controller leader after formation.

Use `$extends` to keep the shared topology in one source while selecting a
different local node:

```jsonc
{
  "$extends": "aws-secret://maestro/production/cluster",
  "node": "node-2"
}
```

Local files and AWS Secrets Manager string values are supported config sources.
Validate the fully merged document before making any local state:

```sh
maestro config validate /etc/maestro/maestro.jsonc
```

Top-level `datadog`, `depot`, `log-backup`, `preview`, and `nixos-upgrade`
settings form the production runtime policy. Their credential fields accept
literal values, relative or absolute `file://` sources, and
`aws-secret://` sources. Each daemon resolves the current shared source at
startup. Bootstrap and admission never copy these credentials into per-node
launch documents or join grants. Without a `nixos-upgrade` override, Linux
nodes use the init flake at `/etc/maestro#default` for upgrades.

## Host prerequisites

Every node needs:

- stable private IPv4 addressing and matching forward and reverse routing;
- synchronized time;
- native containerd;
- Linux network administration privileges for a bridge, veth pairs,
  WireGuard, routes, and nftables; and
- durable, owner-only storage for `/var/lib/maestro`.

Control-plane nodes also need the configured etcd executable. Nodes performing
native builds need BuildKit; nodes selected for remote builds need the Depot
CLI. The Maestro NixOS module provides both.

Cloud security groups and upstream firewalls must allow:

- TCP API traffic to each node endpoint, normally port `3000`, from operator
  clients and cluster peers;
- TCP store-client traffic, normally `2379`, from cluster nodes to
  control-plane nodes;
- TCP store-peer traffic, normally `2380`, between control-plane nodes; and
- UDP WireGuard traffic, normally `51820`, between workload-capable nodes.

Do not expose store ports publicly. `control-allow-cidrs` is an additional host
firewall boundary for protected TCP listeners and must contain every configured
node endpoint when nonempty. Maestro converges its own nftables table, but it
does not edit cloud security groups or upstream network ACLs.

## Form the cluster

Bootstrap the declared master once:

```sh
sudo maestro cluster bootstrap \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --etcd-binary /run/current-system/sw/bin/etcd \
  --output /run/maestro/launch.json
```

The command creates the cluster authority, master identity, initial store
membership, and a create-only encrypted bootstrap document. Start the Maestro
daemon with the same config source before admitting another node:

```sh
sudo maestro-daemon start \
  --config /etc/maestro/maestro.jsonc \
  /run/maestro/launch.json
```

Join each configured node through a running control-plane API:

```sh
sudo maestro cluster join https://10.20.0.11:3000 \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --etcd-binary /run/current-system/sw/bin/etcd \
  --output /run/maestro/launch.json
```

Omit `--etcd-binary` for a `worker`. The join command creates a durable private
key, authenticates CA discovery with the shared join secret, and sends a signed
request. The control plane admits only a node declared in the cluster config
whose role, hostname, endpoint, workload subnet, and observed source address
all match that declaration. The first accepted request durably binds the node
ID to its key and exact request for safe retries. The node receives an encrypted
node-bound grant and starts an etcd learner when its role requires one. The
operator signing key and runtime credentials are not part of that grant. Every
node obtains them from the current shared config source when its daemon starts.
A joining control-plane member is promoted only after it catches up. The
cluster CA is persisted in encrypted etcd state; only the first master carries
an encrypted local seed needed to initialize that record.

Bootstrap and join output files are create-only and owner-only. A retry verifies
and reuses matching state; it does not overwrite a conflicting launch document.
Use the NixOS module to supervise the daemon as described in
[nixos.md](nixos.md).

## Verify formation

From an authenticated context:

```sh
maestro cluster info
maestro cluster nodes
maestro cluster config
```

Confirm that:

- the expected one or three control-plane nodes are ready;
- every workload-capable node reports `Ready`;
- each node reports the intended role and host address;
- the masked config reports each intended workload subnet; and
- the masked config reports the fixed ports and address-pool settings.

The config response intentionally omits the join secret and any Tailscale auth
key.

## Inspect node-local logs

Use the local bootstrap document and live config source to inspect one running
node without an operator contexts file:

```sh
sudo maestro-daemon logs --config /etc/maestro/maestro.jsonc \
  /run/maestro/launch.json --tail 100
sudo maestro-daemon logs --config /etc/maestro/maestro.jsonc \
  /run/maestro/launch.json --source daemon
sudo maestro-daemon logs --config /etc/maestro/maestro.jsonc \
  /run/maestro/launch.json \
  --source api/deployment-1/workload-1 --follow
```

The command uses the node certificate and short-lived node authentication over
the local HTTPS API; it never opens the live DuckDB file from a second process.
`--source` accepts either one exact system component or the retained
`service/deployment/workload` spelling. The initial tail is bounded to 1–10,000
records and prints in chronological order before follow mode begins.

## Lifecycle constraints

Cluster topology is initialization-fixed in the current implementation. A join request
must match one declared node's endpoint, hostname, role, subnet, pool, ports,
and cluster identity. An undeclared node cannot join, and changing a launch
document on one host does not mutate the authoritative topology.

Drain a workload node before planned maintenance:

```sh
maestro cluster drain node-2
maestro cluster restore node-2
```

Restart one node through the same drain, quorum, reboot, verification, and
restore state machine used by upgrades. Restart every node serially with the
leader last by passing `--all`:

```sh
maestro cluster restart node-2
maestro cluster restart --local
maestro cluster restart --all
```

With no target, the CLI lists the cluster nodes and prompts for one selection.
`--local` selects the node serving the active API context but still uses the
coordinated workflow; it does not perform the legacy immediate controller stop.
Every form asks for confirmation before creating the maintenance run. Use `-y`
only in operator-controlled automation.

Start an availability-preserving rolling upgrade, or explicitly choose an
all-node batch:

```sh
maestro cluster upgrade
maestro cluster upgrade --batch=all
```

The CLI confirms the exact target version and selection before creating the
upgrade run. The all-node prompt warns that services and the control plane will
be unavailable. Use `-y` only in automation that has already enforced the same
operator approval.

If maintenance is stuck, cancel the current run without copying its generated
identity. Use `--run` only when selecting an exact retained run, or `--all` to
cancel every active and queued run:

```sh
maestro cluster unfreeze
maestro cluster unfreeze --run upgrade-example
maestro cluster unfreeze --all
```

To replace existing maintenance with a newer upgrade in one command, use
`--force`. The normal topology, quorum, version, and confirmation checks still
apply:

```sh
maestro cluster upgrade --force
```

Permanent removal is a separate, irreversible workflow:

```sh
maestro cluster remove-node node-2
```

Removal drains assignments, removes eligible store membership, cleans durable
node state, and writes a tombstone. The removed node ID cannot rejoin or be
reused. Retain enough live control-plane members for quorum throughout a drain,
upgrade, or removal.

For remote tailnet access to workload routes and cluster DNS, continue with
[Tailscale operator access](tailscale.md). For the one-way production
migration, follow [cutover.md](cutover.md).
