# Multi-node Maestro

This guide covers the first production release of Maestro's multi-node mode. It
uses a private host network for consensus, controller traffic, and the workload
gateway path. Tailscale is optional and is not part of cluster correctness.

## Recommended topology

Use three voters in one low-latency private network. Three voters tolerate one
voter failure; a one-voter cluster tolerates none. Do not run two voters: quorum
would require both. The initial configuration accepts one or three voter IPs or
`IP:controller-port` endpoints, and the order must be identical on every original
voter.

`cluster.nodes[0]` is the only host allowed to create the cluster. It becomes the
first Maestro leader while the cluster forms, but it is not a permanent master.
After quorum forms, normal lease-based election determines leadership.

Add workers when more workload capacity is needed without adding etcd members.
Keep the voter count odd and change membership one voter at a time. For most
installations, three scheduling voters plus any number of workers is the simplest
topology.

## Network model

Each node needs:

- One stable, private IPv4 control address assigned to a non-Tailscale host
  interface. The addresses must be mutually routable without NAT. Use DHCP
  reservations if the hosts do not have static addresses.
- One unique private `/24` workload subnet. Workload subnets must not overlap each
  other, the control network, or any other routed network. These subnets remain
  node-local; they do not need routes between hosts.
- Private TCP reachability between every admitted node and every other node's
  gateway port.

Maestro normally derives the local control address. Set `cluster.bind-ip` only on
an intentionally multi-homed host where source-route discovery is ambiguous. It
is a per-host override and does not replace the shared `cluster.nodes` list.

Allow these ports on the private network:

| Port       | Source                                             | Destination      | Purpose                   |
| ---------- | -------------------------------------------------- | ---------------- | ------------------------- |
| `2379/tcp` | admitted nodes and authoritative cluster subnets   | voters           | etcd client mTLS          |
| `2380/tcp` | voters and authoritative cluster subnets           | voters           | etcd peer mTLS            |
| `3001/tcp` | admitted nodes, operators, and prospective joiners | all nodes        | cluster HTTPS API         |
| `3002/tcp` | admitted nodes and authoritative cluster subnets   | scheduling nodes | node ingress gateway mTLS |

With bare-IP nodes, these port values are configurable through the existing
cluster port fields. With `IP:controller-port` nodes, the listed API port is the
base of a four-port block: gateway is `+1`, etcd client is `+2`, and etcd peer is
`+3`. Never expose etcd to a public interface. Maestro
binds clustered etcd and the cluster API to the resolved private host IP rather
than a wildcard address.

Every workload remains addressed by its node-local container IP. In cluster mode,
Maestro starts a private `maestro-gateway` Traefik on each node. Public ingress
selects a healthy node gateway at
`https://<private-host-ip>:<gateway-port>`; that gateway then selects a local
replica by container IP. Both hops are discovered through
etcd, and the host-to-gateway hop requires mutual TLS using the cluster CA. The
gateway is bound to the configured private host address. The etcd and gateway
listeners require cluster-issued mTLS certificates; private addressing is not
authentication. Maestro does not modify the host firewall. Restrict these private
ports with infrastructure-managed firewall or security-group rules when additional
network isolation is desired.

This is one published port per node, not one port per replica. Replica ports are
never exposed on the host. A separate gateway is not started outside cluster mode,
so existing single-node installations keep their current direct Traefik-to-
container path. Tailscale may still be enabled for operator access or additional
advertised routes, but ingress continues working if it is disconnected.

For services with ingress enabled, the stable
`<service>.<cluster>.maestro.internal` name resolves to the local public Traefik
and therefore uses the same node-gateway path. Per-replica DNS records and stable
records for services without ingress still contain node-local container IPs;
arbitrary cross-node TCP/UDP service networking is outside this release.

## Shared configuration

The original voters use the same ordered `cluster.nodes`, `cluster.subnets`,
cluster name, CA fingerprint, join secret, JWT secret, and registry. The top-level
`subnet` differs on each node. With bare-IP nodes the cluster port fields are also
shared. With endpoint nodes, `cluster.api-port` selects the local entry and differs
per node.

```jsonc
{
  "cluster": {
    "name": "prod",
    "nodes": ["10.20.0.11", "10.20.0.12", "10.20.0.13"],
    "control-allow-cidrs": ["10.20.0.0/24"],
    "subnets": ["172.22.1.0/24", "172.22.2.0/24", "172.22.3.0/24"],
    "gateway-port": 3002,
    "role": "voter",
    "scheduling": true,
    "shared-registry": "ghcr.io/acme",
    "ca-sha256": "<64-character fingerprint from cluster init-ca>",
    "join-secret": "<at-least-32-character-join-secret>",
    "labels": { "zone": "us-west-2a" }
  },
  "subnet": "172.22.1.0/24",
  "jwt-secret-key": "<at-least-32-character-jwt-secret>",
  "encryption-key": "<encryption-key>",
  "runtime": "docker"
}
```

Use `172.22.2.0/24` and `172.22.3.0/24` as the top-level subnet on the second and
third voter. Values in angle brackets are placeholders. Keep secrets out of source
control and deliver the complete configuration through a secret-backed config
source such as AWS Secrets Manager.

### Multiple nodes on one host

Use endpoint nodes when running a complete test cluster on one machine:

```jsonc
"cluster": {
  "nodes": [
    "10.20.0.11:3001",
    "10.20.0.11:3101",
    "10.20.0.11:3201"
  ],
  "api-port": 3001
}
```

The first node uses host ports `3001` through `3004`, the second uses `3101`
through `3104`, and the third uses `3201` through `3204`. Set `api-port` to the
matching base in each node's local config. The blocks must not overlap, must fit
below port 65536, and must stay stable for the lifetime of the etcd member.
Maestro derives and persists the advertised etcd topology from these endpoints;
it does not choose a new peer port on restart.

Each local config still needs a different top-level workload `subnet`, ingress
host port, and any explicitly published admin port. Start each process with a
different base data directory. Maestro automatically namespaces its system
containers, default Docker network, certificates, etcd/RBAC identity, and
runtime identity by the controller port. Cluster assignment containers use the
same namespace, so a replacement replica can overlap its old copy during a safe
drain even when both logical nodes share one container daemon. If `--network` is
supplied explicitly, the caller must also give every same-host node a distinct
network name.

The endpoint form is an identity choice, not an in-place port override. Do not
change an established bare-IP etcd member to endpoint form or change an endpoint's
controller port; remove and re-add the node through the normal membership flow.

`cluster.control-allow-cidrs` is an application-level source check for cluster
formation, not a host firewall or a replacement for authentication. Include the
control addresses of original voters and the private addresses from which approved
nodes will join. Make the range no wider than necessary. Join requests must also
pass signature, timestamp, nonce, source-address, and certificate-pinning checks.
Established etcd and gateway connections use cluster-issued mTLS identities, while
operator API requests use JWT authentication. Maestro deliberately leaves host and
cloud firewall policy to the operator instead of requiring `CAP_NET_ADMIN` or
rewriting system packet-filter state.

## Registry requirements

Cluster scheduling requires `cluster.shared-registry`. Every scheduling node must
be able to resolve, authenticate to, push to, and pull from it before the first
rollout. Configure the runtime's registry credentials on every host; Maestro does
not distribute registry passwords.

The registry must provide read-after-write consistency for image manifests. Do not
apply retention rules that can delete images referenced by active or draining
deployments. A private registry should be reachable over the private network or a
reliable external path independent of any one Maestro node.

Before formation, verify from every host:

```bash
nerdctl login ghcr.io
nerdctl pull ghcr.io/acme/maestro-registry-test:latest
```

Use the equivalent `docker` commands when Docker is the configured runtime.

## Form a new cluster

The examples use `/var/lib/maestro` as the base data directory. Maestro stores the
cluster under `/var/lib/maestro/prod` for a cluster named `prod`.

1. Put the shared configuration on all original voters, changing only the local
   top-level `subnet` and host-specific labels. Initially omit `ca-sha256` or leave
   it unset.
2. On `cluster.nodes[0]`, initialize the identity, CA, host certificates, and
   single-use bootstrap permit:

   ```bash
   maestro cluster init-ca --config /etc/maestro/maestro.jsonc \
     --data-dir /var/lib/maestro
   ```

3. Record the printed cluster ID and CA SHA-256 fingerprint in a secure inventory.
   Set the exact fingerprint as `cluster.ca-sha256` on every node.
4. Before starting any daemon, copy the following material over an authenticated
   out-of-band channel:
   - `system/cluster-id` to every original voter.
   - The matching `system/cluster-provision/<node-identity>/` contents to that
     voter's `system/certs/` directory.
   - `system/certs/cluster-ca/` into every original voter's
     `system/certs/cluster-ca/` directory.

   Only voters receive `cluster-ca/`, which contains the CA private key. Never copy
   that directory to a worker. Preserve ownership and mode `0600` for private keys.
   Do not copy `system/etcd-bootstrap-state.json` away from the seed.
   Bare-IP identities are the eight-digit host-IP hex value. Endpoint identities
   append the four-digit hex controller port, as printed in the provision path.

5. Start Maestro on the seed. Confirm that the API becomes healthy and that it is
   the sole voter and current leader.
6. Start the other original voters. They wait for admission, join one at a time as
   learners, catch up, and are promoted. They never fall back to creating a second
   cluster if the seed is unavailable.
7. Wait for all voters and node gateways to become ready:

   ```bash
   maestro cluster info
   maestro cluster nodes
   ```

Do not interrupt formation during the temporary two-voter stage. Until the third
voter is promoted, both existing voters are required for quorum.

### Existing single-node installation

An existing single-node installation migrates automatically on the first start
with a valid multi-node configuration. The local control address must be
`cluster.nodes[0]`, the role must be `voter`, and the existing etcd member data must
still be present. Use bare-IP `cluster.nodes` for this identity-preserving
migration; endpoint form deliberately fails closed. Configure one or three initial voters and omit `ca-sha256` for
this first start; Maestro generates the CA and persists its fingerprint locally.

Before changing identity or certificates, Maestro takes the normal daemon lock,
stops the detached legacy etcd container, and takes an exclusive lock on its
database. It then makes a byte-verified offline copy at
`system/etcd/data.legacy-backup.v1` with a recovery manifest at
`system/etcd/data.legacy-backup.v1.json`, preserves the existing etcd member name,
creates the cluster identity and CA, saves the old certificates under
`system/certs.legacy-backup`, and recreates the now-empty container network with
the configured per-node `/24`. Preparation is resumable across process or host
failure. An incomplete or conflicting state fails closed and never authorizes a
fresh etcd bootstrap.

The first migrated start needs enough free disk for a second copy of the etcd data
directory and can take longer while that copy is written and verified. This is a
restart operation: containers attached to the legacy network are stopped while it
is recreated, and ingress is unavailable until the controller reconciles the
preserved workload definitions from etcd. Keep both legacy backups until the
migrated cluster has been backed up and tested. Once a cluster identity has been
installed, do not remove `cluster.nodes` or attempt to start the data directory
with the old configuration.

When cluster scheduling is enabled, startup also publishes every locally present
legacy service image to `cluster.shared-registry` before starting the scheduler or
assignment executors. Each push is pulled back for verification and the deployment
record is updated with a leadership-fenced write. This step is crash-resumable via
`system/cluster-image-migration-required`; the marker is cleared only after every
runnable image is either published or independently pullable. A missing local image
that is not pullable fails startup without modifying the versioned etcd backup.

The `v1` suffix identifies the backup and manifest format rather than the Maestro
release that created it. The manifest records the creating Maestro version,
creation time, original member identity, target cluster configuration, and a
SHA-256 digest of the backup tree. Maestro verifies that manifest whenever it
resumes an interrupted migration. A finalized backup is never overwritten or
deleted when its digest or configuration differs; startup fails closed so both
recovery points remain available for inspection.

If an older Maestro build already created the unversioned
`system/etcd/data.legacy-backup` during an interrupted migration, the new build
promotes it to the `v1` name only after it can verify the recovery state. A
conflicting unversioned backup is also preserved and causes startup to fail
closed. Keep the `v1` directory and its JSON manifest together when copying or
restoring the recovery point.

`maestro cluster enable` remains available to stage the same operation explicitly
while the daemon is stopped, but it is not required by deployments that cannot run
host-side commands:

```bash
maestro cluster enable --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro
```

After the migrated voter is healthy, obtain the printed/logged public CA
fingerprint for new-node configurations and use the normal approved join flow to
add voters or workers.

## Add a node

Give the joiner a stable private host IP, a new workload `/24`, the public CA
fingerprint, and the shared join secret. Its `cluster.nodes` remains the immutable
original bootstrap list. Its `cluster.subnets` must include its local top-level
subnet so local validation can complete; the join response installs the current
authoritative voter and subnet cache.

### Worker

Set `cluster.role` to `worker`, then run on the new host:

```bash
maestro cluster join 10.20.0.11:3001 \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro
```

The join secret authorizes a worker identity. Start the daemon after the command
installs the node certificate and cluster identity. Confirm `data-plane` readiness
before relying on the worker for placements.

### Voter

A voter changes quorum and requires explicit approval. On the joining host:

```bash
maestro cluster join --prepare \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro
```

The command prints the node ID, host IP, subnet, and public-key fingerprint. From
an authenticated operator context, approve that exact identity:

```bash
maestro cluster approve-node \
  --role voter \
  --node-id <node-id> \
  --host-ip <private-ip> \
  --api-port <controller-port> \
  --subnet <unique-/24> \
  --public-key-sha256 <fingerprint>
```

Then run `maestro cluster join <leader-private-ip>:<controller-port>` on the
joining host. The leader adds it as a learner and promotion occurs only after it
catches up. Never
add a second voter until the first membership change is complete, and retain an odd
final voter count.

Omit `--api-port` for a cluster that still uses legacy bare-IP node identities.

## Scheduling and affinity contract

The scheduler spreads replicas as evenly as possible across ready, schedulable
nodes and preserves a healthy placement when possible. When the replica count is
larger than the eligible node count, it places additional replicas on the nodes
with the fewest replicas of that deployment, then the fewest total assignments.
Assignments carry a placement epoch; a stale node cannot report health for a
replacement assignment after a move.

Only the currently elected Maestro leader computes and writes assignments. Each
node advertises whether its local assignment executor is enabled through
`cluster.scheduling`; nodes with scheduling disabled remain control-plane members
but are never placement targets.

Scaling up preserves healthy placements and starts only the additional replica
slots. Scaling down is traffic-first: the leader removes surplus replicas from the
durable Traefik generation, keeps their assignments running through the drain
deadline, and only then tells their nodes to stop them. A successor leader resumes
that sequence from the stored traffic generation after failover.

Placement affinity under `deploy.node-affinity` is a hard requirement:

```jsonc
"deploy": {
  "node-affinity": {
    "labels": { "disk": "nvme" }
  }
}
```

`node-id` pins to one exact node, and every label must match. Maestro does not
silently violate these constraints. A rollout or drain remains blocked when no
eligible node exists. Writable host volumes require an exact `node-id` because
their data cannot be relocated safely.

Request affinity is separate and soft. By default, sending
`X-Maestro-Affinity: <node-id>` selects that node's ready replicas when available.
The header name can be changed per service without changing this node-ID value
contract:

```jsonc
"ingress": {
  "host": "api.example.com",
  "port": 8080,
  "sessionAffinity": {
    "header": "X-Session-Node"
  }
}
```

With that configuration, clients send `X-Session-Node: <node-id>`. An unknown node
ID falls through to the normal service router. If a known node disappears, its
affinity router is removed and requests also fall through after Traefik receives
the update. The header is a routing preference, not an authentication mechanism.
The public load balancer independently uses `maestro-node-affinity` to keep a
client on a healthy node gateway. That gateway uses `maestro-affinity` to keep the
client on one of its local replicas. These cookies and the configured header are
separate preferences; none is a durability guarantee.

## Ingress traffic and IP blocking

Maestro converts structured Traefik access logs into one-minute summaries on every
node and exposes the aggregated results in a service's **Traffic** tab. The view
groups requests by client IP and normalized path, with a status-code breakdown for
each value. Query strings are excluded so secrets in URLs are not displayed or
used as distinct paths. The summaries are retained for seven days; raw access
records are discarded after aggregation, while non-access ingress diagnostics
remain in the normal log stream.

The same tab shows denied traffic as a separate cluster-wide IP and path
breakdown. Those rows come only from the reserved global blocking routers and are
not attributed to whichever service page happens to be open. The internal policy
router and service are also excluded from normal ingress-route diagnostics and
per-service Prometheus traffic metrics.

Block or unblock exact IPv4 or IPv6 addresses from any service's Traffic tab. The
blocklist is cluster-wide: an address blocked while viewing one service is refused
before it can reach any Maestro ingress route. Maestro stores each address as
independent persistent runtime state in etcd, separate from service manifests and
deployment history, so changes take effect without a redeploy and survive service
rollouts and deletion. The leader reconciles the global Traefik policy on its
five-second control-loop tick.

Blocked requests receive HTTP 403 from Maestro and never reach a workload
container. Matching covers direct client addresses and the common
`CF-Connecting-IP`, `X-Real-IP`, and `X-Forwarded-For` proxy headers. Access-log
attribution uses Traefik's normalized `ClientHost`. If that value is still an
internal proxy address, Maestro accepts a forwarded address; otherwise it keeps
`ClientHost` to prevent a public client from spoofing another address in the
Traffic view. With Cloudflare Tunnel on Maestro's container network, Traefik
normalizes `ClientHost` to `CF-Connecting-IP`, so one-click actions target the
visitor rather than the tunnel peer.

Maestro compiles the cluster list into bounded, generation-qualified Traefik deny
routers. New generations are installed before old generations are removed, so an
existing policy is never removed before its replacement is complete, without
imposing an arbitrary total-address limit. CIDR ranges are intentionally not
accepted by this control.

## Drain, restore, and remove

List node IDs before changing state:

```bash
maestro cluster nodes
maestro cluster drain <node-id>
```

Drain marks the node unschedulable and relocates movable assignments with the same
readiness-gated cutover used by rollouts. It does not violate hard affinity or stop
an unmovable workload implicitly. Resolve placement blockers, then wait until the
node has no assignment manifest.

Return a maintained node to service with:

```bash
maestro cluster restore <node-id>
```

Permanently remove a drained node with:

```bash
maestro cluster remove-node <node-id>
```

Removal transfers Maestro leadership when necessary, removes voter membership,
and cleans cluster-owned state. Never delete a voter's local etcd member directory
as a substitute for `remove-node`; a wiped member deliberately refuses a fresh
bootstrap. Remove only one voter at a time and verify quorum after every change.

For a lost or compromised node, remove it, rotate `cluster.join-secret`, and
revoke its runtime/registry credentials. Cluster CA rotation is a manual maintenance
operation in this release: issue new node identities and migrate every remaining
node in a planned window. Removing a member alone does not revoke a certificate
that was already issued by the old CA.

## Rolling upgrades

Before upgrading, ensure all durable nodes are online, no rollout is active, every
node can fetch the target system source/images, and movable services have enough
spare capacity. Workloads must handle graceful shutdown for zero-request-loss
rolls.

Start a serial, resumable fleet upgrade with a strictly newer semantic version:

```bash
maestro cluster upgrade --version 0.3.0
```

Maestro freezes new deployment mutations, upgrades workers first, then follower
voters, and the current leader last. Each node is drained, upgraded idempotently,
verified against the exact requested version and `/_healthy`, then restored. The
durable run resumes under a new leader or after a controller restart.

An unmovable pinned service blocks the run after the drain deadline instead of
being stopped. A failed run releases the cluster-wide deploy freeze but leaves the
failed node drained for inspection. Fix the cause and start a new run; nodes already
at the target version are skipped.

Manual unfreeze is only for an abandoned run whose orchestrator heartbeat is
stale. It requires the exact run ID and records the run as failed:

```bash
maestro cluster unfreeze --upgrade-run <run-id>
```

Do not use the node-local `maestro cluster upgrade system` command for a fleet
upgrade; it does not coordinate draining or membership order.

## Expected failure behavior

- A Tailscale failure has no effect on cluster ingress, etcd, leader election,
  joins, or controller APIs. Only optional services explicitly using the tailnet
  are affected.
- A daemon or voter failure does not change current Traefik configuration. With
  quorum, the successor leader removes unhealthy endpoints and replaces movable
  replicas after the durable loss grace period.
- Loss of etcd quorum stops scheduling and deploy writes. Existing containers and
  the last applied Traefik configuration continue serving. Restore a majority; do
  not bootstrap a replacement cluster.
- Loss of a node's mTLS gateway marks that node's data plane unready and disables
  its local cloudflared connectors when the runtime supports dynamic network
  attachment. Every public Traefik also health-checks each remote gateway and
  removes an unreachable gateway independently, without waiting for the leader.
  The controller, etcd member, and cluster API remain available over private host
  IPs.
- Loss of the original seed during initial formation has no automatic fallback.
  Recover the seed and its member data. Starting another fresh seed risks split
  brain and is intentionally refused.
- A full-cluster restart uses persisted etcd member state and the cluster-bound
  voter cache. Preserve each voter's complete data directory and start a majority
  before attempting writes.

## Acceptance and chaos checklist

The repository includes opt-in, container-backed distributed control-plane and
data-plane tests. Run all scenarios serially with:

```bash
cargo test-multi-node
```

The command uses a working nerdctl daemon when available and otherwise falls
back to Docker. Set `MAESTRO_TEST_RUNTIME=nerdctl` or
`MAESTRO_TEST_RUNTIME=docker` to require a specific runtime.

Run it on an isolated Linux container host. It creates and destroys three
host-networked etcd containers using separate controller-derived four-port blocks,
runs two real Maestro leader electors, verifies a semantic write before and after
leader failover, rejects the stale leader's write, and confirms writes stop after
two etcd members are removed. It also creates three isolated logical node networks
on one host, starts a private echo replica and node gateway on each, and fronts the
gateways with a health-checked public Traefik instance. That test verifies traffic
distribution, replica address changes, replica and gateway failure, complete
gateway outage, and recovery without publishing replica ports. A scaling scenario
uses the production scheduler and reconciler diff to move a live echo service from
one replica to five and back to two. It verifies even placement, stable surviving
assignment identities, externally reachable routes, and removal of scaled-down
containers. The normal test suite compiles these tests but leaves them ignored
because they are destructive and require a container daemon. They complement
rather than replace the remaining multi-host drills below.

Run this checklist on a real private network before production and after network,
runtime, or membership changes:

1. Form one voter plus two workers, then a three-voter topology. Confirm only the
   configured seed creates a fresh cluster and all later voters pass through learner
   promotion.
2. Deploy a three-replica echo service that returns `MAESTRO_NODE_ID`. Confirm that
   responses reach all eligible nodes. Confirm the public Traefik servers are the
   node gateway addresses, each gateway contains only its local replica IPs, and
   no replica port is published on a host.
3. Send the service's configured affinity header (or `X-Maestro-Affinity` by
   default) for one node, stop that node, and confirm requests fall through to
   ready replicas without an early or partial router cutover.
4. Generate 2xx, 4xx, and 5xx requests from multiple client addresses and paths.
   Confirm the Traffic tab totals and status breakdowns, block one address, and
   confirm it receives 403 without reaching the workload on any node.
5. Stop every Tailscale container and confirm workload ingress, etcd quorum, the
   Maestro leader, registry leases, cluster reads/writes, and join API remain
   stable. Then stop one `maestro-gateway`; confirm public ingress immediately
   avoids it while the control plane stays healthy, and restore it.
6. Kill the current leader during a rollout, drain, membership change, and rolling
   upgrade. Confirm the successor resumes durable work and stale fenced writes are
   rejected.
7. Kill one voter, then restore it. Kill two of three voters and confirm writes
   fail without disturbing already-running workloads; restore quorum and confirm
   automatic recovery.
8. Drain a node under load. Confirm all replacement replicas become ready before
   atomic traffic cutover. Repeat with a hard-pinned service and confirm the drain
   blocks rather than stopping it.
9. Run a fleet upgrade under sustained load for both a single-replica movable
   service and a multi-replica service. Confirm serial ordering, exact-version
   verification, leader-last handoff, and no dropped requests when the workload
   honors graceful shutdown.
10. Restart all nodes gracefully and verify state, voter membership, assignments,
    DNS, and ingress routes survive. On a disposable cluster, verify a copied or
    wiped voter data directory is refused rather than bootstrapped.

Record timestamps, elected leader, voter membership, node readiness, request error
rate, and gateway state for every drill. Treat any correctness dependency on
Tailscale, publicly reachable gateway without mTLS, duplicate etcd member, stale
assignment acceptance, or partial ingress cutover as a release blocker.
