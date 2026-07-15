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

Nodes default to the `hybrid` role: they vote and run workloads. Add workers when
more workload capacity is needed without adding etcd members, or set a node to
`voter` when it should be control-plane-only. Keep the combined hybrid and voter
count odd and change membership one voter at a time. Three hybrid nodes are the
simplest general-purpose topology; larger installations can use three dedicated
voters plus workers.

Role-specific startup is explicit:

- `hybrid` starts the consensus/control services and the workload ingress,
  gateway, and assignment executor.
- `voter` starts etcd, the node API/probe, DNS, and admin UI, but no public
  ingress, Cloudflare connector, node gateway, or assignment executor.
- `worker` starts the node API/probe, DNS, public ingress, node gateway, and
  assignment executor, but no local etcd member or admin UI. The probe and DNS
  remain local because node health, logs, coordinated maintenance, and workload
  name resolution depend on them.

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

| Port       | Source                                             | Destination    | Purpose                   |
| ---------- | -------------------------------------------------- | -------------- | ------------------------- |
| `2379/tcp` | admitted nodes and authoritative cluster subnets   | voters         | etcd client mTLS          |
| `2380/tcp` | voters and authoritative cluster subnets           | voters         | etcd peer mTLS            |
| `3001/tcp` | admitted nodes, operators, and prospective joiners | all nodes      | cluster HTTPS API         |
| `3002/tcp` | admitted nodes and authoritative cluster subnets   | workload nodes | node ingress gateway mTLS |

With bare-IP nodes, these port values are configurable through the existing
cluster port fields. With `IP:controller-port` nodes, the listed API port is the
base of a four-port block: gateway is `+1`, etcd client is `+2`, and etcd peer is
`+3`. Never expose etcd to a public interface. Maestro
binds clustered etcd and the cluster API to the resolved private host IP rather
than a wildcard address.

Every workload remains addressed by its node-local container IP. In cluster mode,
Maestro starts a private `maestro-gateway` Traefik on each hybrid or worker node.
Public ingress
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
cluster name, join secret, JWT secret, and registry. The top-level
`subnet` and optional `node.role` describe the local node and may differ. With
bare-IP nodes the cluster port fields are also shared. With endpoint nodes,
`cluster.api-port` selects the local entry and differs per node.

```jsonc
{
  "cluster": {
    "name": "prod",
    "nodes": ["10.20.0.11", "10.20.0.12", "10.20.0.13"],
    "control-allow-cidrs": ["10.20.0.0/24"],
    "subnets": ["172.22.1.0/24", "172.22.2.0/24", "172.22.3.0/24"],
    "gateway-port": 3002,
    "shared-registry": "ghcr.io/acme",
    "join-secret": "<high-entropy-secret-of-at-least-32-characters>",
    "labels": { "zone": "us-west-2a" }
  },
  "node": { "role": "hybrid" },
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

Multi-node mode requires `cluster.shared-registry`. Every hybrid or worker node
must be able to pull from it, and every voter or hybrid that may lead builds must
be able to push to it. Configure the runtime's registry credentials on every host;
Maestro does not distribute registry passwords.

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

1. Deploy the shared configuration to every original voter, changing only the
   local top-level `subnet`, `cluster.api-port` in endpoint mode, and host-specific
   labels.
2. Start all Maestro daemons. No provisioning command or certificate copy is
   required. On its first start, `cluster.nodes[0]` creates the cluster identity,
   CA, seed certificates, and one-time bootstrap permit under the daemon lock.
3. Every other unprovisioned voter waits for a configured voter API, authenticates
   the advertised CA with an HMAC proof derived from `cluster.join-secret`, and
   joins automatically. Original configured voters are admitted as learners and
   promoted only after catching up. They never fall back to creating another
   cluster when the seed is unavailable.
4. Wait for all voters and workload-node gateways to become ready:

   ```bash
   maestro cluster info
   maestro cluster nodes
   ```

Do not interrupt formation during the temporary two-voter stage. Until the third
voter is promoted, both existing voters are required for quorum.

### Existing single-node installation

An existing single-node installation migrates automatically on the first start
with a valid multi-node configuration. The local control address must be
`cluster.nodes[0]`, `node.role` must be `hybrid` or `voter`, and the existing etcd
member data must still be present. Use bare-IP `cluster.nodes` for this
identity-preserving migration; endpoint form deliberately fails closed. Configure
one or three initial voters; Maestro generates and persists the CA automatically.

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

On the first clustered startup, Maestro also publishes every locally present
legacy service image to `cluster.shared-registry` before starting placement or
assignment execution. Each push is pulled back for verification and the deployment
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

After the migrated voter is healthy, the other configured original voters and any
new workers authenticate and join automatically when their daemons start.

## Add a node

Give the joiner a stable private host IP, a new workload `/24`, and the shared join
secret. Its `cluster.nodes` remains the immutable original bootstrap list. Its
`cluster.subnets` must include its local top-level subnet so local validation can
complete; the join response installs the authenticated CA, node certificates,
cluster identity, and current authoritative voter/subnet cache.

### Worker

Set `node.role` to `worker` and deploy/start the daemon normally. It waits until a
configured voter is reachable and then joins without a host-side command. Confirm
`data-plane` readiness before relying on the worker for placements.

### Voter or hybrid

The one or three voter/hybrid identities in the initial `cluster.nodes` list join
automatically during formation. `voter` is control-plane-only; `hybrid` also
accepts workloads and is the default. Expanding or replacing the immutable voter
set after formation is not an automatic operation in this release; do not change
`cluster.nodes` on a live cluster.

## Scheduling and affinity contract

The scheduler spreads replicas as evenly as possible across ready hybrid and worker
nodes and preserves a healthy placement when possible. A worker wins an otherwise
equal placement choice, so dedicated workload capacity is used first without
weakening load balancing. When the replica count is
larger than the eligible node count, it places additional replicas on the nodes
with the fewest replicas of that deployment, then the fewest total assignments.
Assignments carry a placement epoch; a stale node cannot report health for a
replacement assignment after a move.

Only the currently elected Maestro leader computes and writes assignments. Worker
and hybrid nodes run assignment executors. Explicit voters remain control-plane
members and are never placement targets.

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

Request affinity is separate and soft. Every response includes an opaque, stable
node token in `X-Session-Affinity`. API clients can echo that header on later
requests to select the same node's ready replicas when available. The token is
derived from the cluster and node identities, but it does not reveal the node ID.
The header name can be changed per service:

```jsonc
"ingress": {
  "host": "api.example.com",
  "port": 8080,
  "sessionAffinity": {
    "header": "X-Session-Node"
  }
}
```

With that configuration, Maestro returns `X-Session-Node: <opaque-token>` and
clients may send the same header back. An unknown token falls through to the
normal service router. If its node disappears, the affinity router is removed and
requests also fall through after Traefik receives the update. The header is a
routing preference, not an authentication mechanism. The node gateway overwrites
the header before it reaches the workload and on the response so applications see
the node that actually handled the request.

The public load balancer independently sets `maestro-node-affinity` to keep a
cookie-aware client on a healthy node gateway with no application changes. That
gateway sets the separate `maestro-affinity` cookie to keep the client on one of
its local replicas. These cookies provide out-of-the-box browser affinity; the
header supports non-browser clients and explicit affinity propagation. None is a
durability guarantee.

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

## Rolling restarts

Restart one node through the safe maintenance workflow with either an interactive
picker or an explicit node ID:

```bash
maestro cluster restart
maestro cluster restart <node-id>
```

Restart the entire cluster serially with:

```bash
maestro cluster restart --all
```

Selected-node and all-node runs freeze deployment mutations, require every durable
node to be live, and refuse to start during an active rollout. Maestro drains each
selected node, transfers leadership when necessary, requests the node-local
restart, verifies that a new controller process is healthy, restores placement
eligibility, and then advances. Whole-cluster restarts process workers first,
follower consensus nodes next, and the current leader last. The durable run resumes
after leadership changes.

This is the recommended way to load a changed host configuration. Publish a
compatible config to every selected host before starting the run. Membership,
identity, control-address, and subnet changes still require the corresponding
cluster lifecycle operation; do not use a rolling restart to silently change those
identities.

For break-glass local maintenance only, bypass orchestration explicitly:

```bash
maestro cluster restart --local
```

`--local` stops the containers on whichever node receives the request without
draining assignments, transferring leadership, or verifying recovery. Do not use
it for routine clustered configuration changes.

## Rolling upgrades

Before upgrading, ensure all durable nodes are online, no rollout is active, every
node can fetch the target system source/images, and movable services have enough
spare capacity. Workloads must handle graceful shutdown for zero-request-loss
rolls.

Start a serial, resumable fleet upgrade to the version embedded in the CLI:

```bash
maestro cluster upgrade
```

The CLI sends its own Cargo package version, matching the existing node-local
upgrade behavior. `--version <version>` remains available as an explicit override
for release testing.

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
leader failover, resumes a production assignment-manifest write from its durable
generation, rejects the stale leader's writes, and confirms writes stop after two
etcd members are removed. A formation scenario starts only the designated seed,
admits the other configured voters as learners (including a later-listed voter
while the middle voter is absent), generates their production-shaped join state,
waits for catch-up, and promotes them into one writable three-voter cluster. It
also proves a consumed seed permit cannot authorize fallback bootstrap. A serial
restart scenario stops one logical node at a time, verifies that the remaining
etcd members still accept writes and public ingress keeps serving through the
other gateways, then proves the restarted member, gateway, and workload rejoin
before proceeding. A coordinated restart scenario uses real etcd fencing and two
real electors to exercise all-node and selected-node maintenance runs, including
worker/follower/leader ordering, leadership handoff, new-process verification,
placement restoration, and freeze cleanup. The suite also creates three isolated
logical node networks on one host, starts a private echo replica and node gateway
on each, and fronts the gateways with a health-checked public Traefik instance.
That test verifies traffic distribution, replica address changes, individual
replica and gateway failures, complete gateway outage, and recovery without
publishing replica ports. A scaling scenario uses the production scheduler and
reconciler diff to move a live echo service from one replica to five and back to
two. It verifies even placement,
stable surviving assignment identities, externally reachable routes, and removal
of scaled-down containers. A rollout scenario starts two real workload versions,
holds the new version out of Traefik until every replica is ready, asserts public
traffic has no failed requests during cutover, and verifies SIGTERM waits for an
in-flight request on the old version. An affinity scenario verifies that the first
response contains an opaque node token and both proxy-layer cookies, cookie replay
stays on the same replica, and header replay can explicitly select another node
without exposing its ID. The normal test suite compiles these tests but leaves them
ignored because they are destructive and require a container daemon. They
complement rather than replace the remaining multi-host drills below.

Run this checklist on a real private network before production and after network,
runtime, or membership changes:

1. Form one voter plus two workers, then a three-voter topology. Confirm only the
   configured seed creates a fresh cluster and all later voters pass through learner
   promotion.
2. Deploy a three-replica echo service that returns `MAESTRO_NODE_ID`. Confirm that
   responses reach all eligible nodes. Confirm the public Traefik servers are the
   node gateway addresses, each gateway contains only its local replica IPs, and
   no replica port is published on a host.
3. Capture the service's configured affinity response header (or
   `X-Session-Affinity` by default), echo it on later requests, and confirm they
   select the same node without exposing its node ID. Stop that node and confirm
   requests fall through to ready replicas after the routing cutover.
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
