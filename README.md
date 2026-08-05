# Maestro

Maestro is a multi-node-native workload platform for declarative service
rollouts, zero-downtime ingress cutovers, cluster operations, build and preview
automation, and node-local observability.

This repository contains the one-shot Maestro rewrite described in
[REWRITE.md](REWRITE.md). The active implementation lives under `crates/` and
`ui/apps|packages`; the retired controller, previous UI, DNS proxy, and
container build paths have been removed.

The rewritten system has no single-node mode. A one-node installation is a
normal cluster topology with one master, and it grows through the same declared
join workflow used by larger clusters.

## Architecture

The rewrite is split into explicit layers:

- `crates/kernel` owns resources, storage, controller mechanics, fencing, and
  process supervision contracts.
- `crates/runtime` provides backend-neutral workload, artifact, network, and
  exec contracts with native containerd and Docker API implementations.
- `crates/node` owns assignment reconciliation, adoption, telemetry, DNS,
  firewalling, workload networking, and the node API.
- `crates/operators` owns deployment, scheduling, ingress, DNS, firewall,
  preview, build, upgrade, and webhook reconciliation.
- `crates/observability` owns LogQL, log and metric pipelines, DuckDB/Parquet
  storage, backup, and external sinks.
- `crates/apps` contains the operator CLI, daemon composition root, API server,
  and cutover migrator.
- `ui/apps/panel` composes the generated API client and feature packages under
  `ui/packages`.

Cluster control traffic uses mutually authenticated HTTPS and etcd-backed
coordination. Workload traffic crosses nodes over a self-managed WireGuard
mesh. Tailscale is optional and limited to operator and cross-cluster access;
it is not a cluster correctness dependency.

## Build

The Nix flake is the supported release path:

```sh
nix build
nix flake check
```

`result/bin` contains:

- `maestro` — operator CLI, including the `daemon` compatibility command;
- `maestro-daemon` — host daemon and node-local administration; and
- `maestro-migrate` — reviewed legacy-to-Maestro cutover migration.

For Rust development without packaging:

```sh
cargo build --workspace
cargo run -p maestro-cli --bin maestro -- --help
cargo run -p daemon -- --help
cargo run -p migrate -- --help
```

Cargo, Nix packages, and release bundles all expose the operator CLI as
`maestro`.

The Nix development shell supplies Rust, Protocol Buffers, C/C++ build tools,
Node, pnpm, actionlint, and shellcheck:

```sh
nix develop
```

## Host requirements

A Maestro daemon host requires:

- Linux 6.3 or newer with stable private IPv4 addressing and synchronized
  time;
- containerd 2.0 or newer with an idmapped snapshotter, and runc 1.2 or newer;
- privileges for bridges, veth pairs, WireGuard, routes, cgroups, and
  nftables;
- owner-only durable storage, conventionally `/var/lib/maestro`; and
- BuildKit on nodes that perform native builds, or the Depot CLI on nodes that
  run services whose build selects a Depot project.

Control-plane nodes also require the configured etcd executable. The NixOS
module provisions containerd, BuildKit, Depot, etcd, and the host tools
used by the production adapters.

The module deliberately leaves the NixOS firewall configuration to the host.
With the default Maestro ports, a host that enables that firewall needs:

```nix
networking.firewall.allowedTCPPorts = [ 53 2379 2380 3000 ];
networking.firewall.allowedUDPPorts = [ 53 51820 ];
```

Both TCP and UDP port `53` must be allowed so workloads can reach the DNS
listener on their local workload bridge; TCP is required for DNS fallback and
larger responses. TCP `3000` is the node API, TCP `2379` and `2380` are etcd
client and peer traffic, and UDP `51820` is the workload WireGuard mesh. Use
the configured values instead if those ports are customized. Restrict the API
and etcd ports to the declared node and operator networks in cloud security
groups, and never expose etcd publicly. Port `53` only needs a listener on the
local workload bridge, so it does not need a public security-group rule.

The runtime crate also contains a native Docker API backend for its supported
development capabilities. Production daemon composition selects containerd.
Every production workload receives a durable, exclusive 65,536-ID user
namespace, so container UID 0 is an unprivileged, workload-specific host UID.
Cutover deliberately restarts migrated workloads under that runtime instead of
carrying a legacy Docker adoption path.

## Form a cluster

Generate create-only starter documents:

```sh
maestro config init cluster
maestro config init services
```

Edit `maestro.jsonc` so every future node is declared with its stable private
endpoint, role, and non-overlapping workload subnet. The generated
`join-secret`, `jwt-secret-key`, and `encryption-key` are already strong. The
cluster document contains secrets and must remain owner-only. Keep
`encryption-key` stable: Maestro derives separate node-bootstrap and etcd keys
from it, and changing it without a re-encryption operation makes existing
encrypted state unreadable.

Validate the fully merged sources before creating local state:

```sh
maestro config validate maestro.jsonc
maestro config validate maestro.services.jsonc
maestro config validate aws-secret://maestro/production/cluster
```

Bootstrap the declared master once:

```sh
sudo maestro cluster bootstrap \
  --config /etc/maestro/maestro.jsonc \
  --data-dir /var/lib/maestro \
  --etcd-binary /run/current-system/sw/bin/etcd \
  --output /run/maestro/launch.json
```

The launch document contains only the node's encrypted bootstrap identity and
local runtime paths. It remains an owner-only regular file outside the Nix
store. The resolved cluster config and reusable service credentials are never
cached in it. Start it through the NixOS module, or pass the same config source
directly while developing:

```sh
sudo maestro-daemon start \
  --config aws-secret://maestro/production/cluster \
  /run/maestro/launch.json
```

Optional top-level `datadog`, `depot`, `log-backup`, `preview`, and
`nixos-upgrade` settings are validated with the cluster config. Credential
fields accept literal values, `file://` sources, or `aws-secret://` sources.
Every daemon fetches and resolves that source on every service start, so an AWS
Secrets Manager update takes effect after restarting the node. Node admission
returns only node-bound bootstrap material; it does not copy shared config into
the launch document. The cluster config API never returns secret values. When
`nixos-upgrade` is omitted, Linux nodes upgrade through the standard init flake
at `/etc/maestro#default`; that setting is only needed to override the default.

For multi-node admission, network requirements, verification, drain, restart,
upgrade, and removal procedures, follow
[Multi-node operations](docs/multi-node.md). For NixOS service and
artifact packaging, follow
[NixOS deployment](docs/nixos.md).

## Authenticate an operator CLI

Mint a token from the same protected cluster config used by the nodes:

```sh
maestro auth token \
  --config /etc/maestro/maestro.jsonc \
  --expires-in 1h \
  --access-level read-only
```

Use `--jwt-secret-key "$JWT_SECRET_KEY"` instead of `--config` when supplying
the raw key directly.

This prints only the short-lived JWT. `read-only` tokens can use API and panel
views but cannot mutate cluster state or open exec sessions; `operator` tokens
retain full access. A panel session never outlives the source token and is
always capped at eight hours.

To rotate the signing key, replace `jwt-secret-key` in the protected shared
config and restart the nodes. Maestro currently accepts one HS256 signing key
at a time, so existing tokens stop working once requests reach a restarted
node. Mint replacement tokens after every node is using the new key.

Create a context for one declared HTTPS endpoint. Supply the cluster CA when it
is not already in the workstation trust store:

```sh
maestro contexts set prod https://10.20.0.11:3000 \
  --ca-certificate cluster-ca.pem
maestro contexts use prod
maestro contexts login --days 7
```

`contexts login` reads `JWT_SECRET_KEY` from the environment or a hidden
prompt, signs a short-lived operator-scoped token, and stores it in the active
context. Context files use owner-only permissions; the CLI refuses an existing
symlink, non-regular file, or group/world-readable credential file.

List configured origins without exposing their credentials:

```sh
maestro contexts ls
```

## Deploy and operate services

Preview a declarative rollout before persisting it:

```sh
maestro services rollout
maestro services rollout --apply
```

Limit a rollout to selected services or release one frozen generation without
changing the service's long-lived freeze:

```sh
maestro services rollout --service api --service worker
maestro services rollout --service api --apply --force
```

Package a local build context and upload it without requiring a registry:

```sh
maestro services up api \
  --config maestro.services.jsonc \
  --context .
```

Builds omit `build.registry` by default and are replicated directly between
Maestro nodes. Set it to a registry prefix such as
`registry.example/team` to publish a deployment-unique tag and deploy the
registry's immutable digest; registry credentials remain a node-runtime
responsibility.

To use Depot, configure the cluster-level `depot.token`, then select the remote
builder per service:

```jsonc
{
  "services": {
    "api": {
      "build": {
        "repo": "https://github.com/example/api.git",
        "branch": "main",
        "dockerfile": "Dockerfile",
        "depot": { "project": "your-depot-project" }
      },
      "deploy": { "exposePorts": [8080] }
    }
  }
}
```

The token is passed to the Depot process only through `DEPOT_TOKEN`. Build
arguments and BuildKit-style secrets are preserved, and the single-platform
result is imported into Maestro's artifact store before optional immutable
registry publication.

The CLI also exposes deployment history, redeploy, in-place workload restart,
cancel, remove, service delete, freeze/unfreeze, and replica override commands.
Use `maestro services --help` for their exact optimistic-concurrency and
idempotency options.

Set `deploy.replicaSpread` to `"bestEffort"` when replicas should rebalance
across every eligible node after topology changes. The default `"stable"`
preserves healthy assignments; hard `nodeAffinity` constraints still limit the
nodes available to either policy.

## Exec

Exec selects one running service replica, relays across nodes when necessary,
and supports PTY resize, wait, signal, and pipe modes:

```sh
maestro exec api
maestro exec api --replica 1
maestro exec api --no-tty -- env
maestro exec api -- /bin/sh -c 'id && pwd'
```

Use `~.` at the beginning of a terminal line to force-detach. Set
`deploy.exec` to `false` for services that must reject operator exec. Session
input and output are relayed directly and are not added to the log store.

## Logs

The CLI queries the same normalized node-local records used by the panel and
cluster APIs:

```sh
maestro logs --service api \
  --query '@http.status_code:[500 TO 599] AND -message:*health*'
maestro logs --query 'level:error' --output json | jq
```

JSON output is always the complete normalized record, including node,
sequence, stream, origin, body, and raw attributes. The legacy `--full`
spelling remains accepted with JSON output, and `--include-system` remains
accepted even though all-log queries already include system records.

Time-bounded queries use Unix milliseconds:

```sh
maestro logs --from 1784707200000 --to 1784793600000 --no-follow
```

Inspect one node without an operator context by authenticating with its local
bootstrap identity and current cluster config:

```sh
sudo maestro daemon logs --config aws-secret://maestro/production/cluster \
  /run/maestro/launch.json --tail 100
sudo maestro daemon logs --config aws-secret://maestro/production/cluster \
  /run/maestro/launch.json --source daemon
sudo maestro daemon logs --config aws-secret://maestro/production/cluster \
  /run/maestro/launch.json \
  --source api/deployment-1/workload-1 --follow
```

The command uses the node API instead of opening the live DuckDB store from a
second process.

Node-local sink dead letters are bounded and require explicit export or purge
selection:

```sh
sudo maestro daemon dead-letters /run/maestro/launch.json list
sudo maestro daemon dead-letters /run/maestro/launch.json \
  export --output dead-letters.jsonl
sudo maestro daemon dead-letters /run/maestro/launch.json purge --all
```

## Panel

Install and build the UI workspace with pnpm:

```sh
pnpm install --frozen-lockfile
pnpm --dir ui typecheck
pnpm --dir ui build
```

The release flake exposes the static panel as `.#panel` and includes it
in the default package and daemon image. The daemon serves it from the API
origin. Operators exchange an existing bearer token for a short-lived Secure,
HttpOnly, SameSite=Strict browser cookie; the panel never stores the token.

## Migration and cutover

The migrator captures and fences the legacy etcd snapshot, produces a
secret-free review plan, applies only that reviewed plan, verifies destination
ownership and exact state, and migrates node-local hot/cold telemetry under
manifest control.

Follow [Rewrite cutover migration](docs/cutover.md) for the exact rehearsal,
apply, native-store restore, launch, verification, and evidence-retention
procedure. Record the result in the
[production rehearsal evidence checklist](docs/rehearsal-evidence.md).
Production cutover is not approved merely because the package builds.

The cutover keeps the one-way data migration and does not preserve legacy API
or runtime compatibility after migration. Migrated workloads are recreated
under the Maestro runtime during the planned cutover window.

## Release artifacts

The flake publishes deterministic native packages, static-musl bundles, a
NixOS module, and a minimal daemon image containing the panel. Build the
current architecture's bundles with:

```sh
nix build .#release-bundle
nix build .#daemon-image-bundle
```

See [NixOS deployment](docs/nixos.md) for checksums, archive
contents, image loading, module configuration, and runtime caveats.

## Verification

The repository gates are:

```sh
cargo fmt --all -- --check
cargo deny check
cargo clippy --workspace --all-targets -- -D warnings
cargo clippy --workspace --all-targets --all-features -- -D warnings
INSTA_UPDATE=no cargo nextest run --workspace
INSTA_UPDATE=no cargo nextest run --workspace --all-features
cargo test --doc --workspace --all-features
RUSTDOCFLAGS="-D missing_docs -D rustdoc::broken_intra_doc_links" \
  cargo doc --no-deps \
    -p kernel-api \
    -p kernel-store \
    -p kernel-controller \
    -p supervisor \
    -p runtime \
    -p node-fabric
git diff --check
python3 scripts/check-source-layout.py
python3 scripts/check-crate-boundaries.py
test -z "$(find crates -type f -name '*.snap.new' -print)"
actionlint
shellcheck scripts/*.sh
pnpm exec oxfmt --check ui/apps/panel ui/packages
pnpm -r test
pnpm --dir ui typecheck
pnpm --dir ui build
pnpm check:api-drift
nix flake check
scripts/run-docker-acceptance.sh
scripts/run-containerd-acceptance.sh
scripts/run-real-cluster-acceptance.sh
scripts/run-cutover-acceptance.sh
scripts/run-minio-acceptance.sh
```

The single release workflow runs real Docker and containerd conformance,
one-node/three-node cluster scenarios, an encrypted three-member etcd cutover,
and SSE-KMS uploads against local MinIO/KES services. Default unit tests do not
pretend to replace those cutover gates.

## Operator documentation

- [Multi-node operations](docs/multi-node.md)
- [Rewrite cutover migration](docs/cutover.md)
- [Rewrite parity evidence](docs/parity-evidence.md)
- [Production cutover rehearsal evidence](docs/rehearsal-evidence.md)
- [NixOS deployment](docs/nixos.md)
- [Tailscale operator access](docs/tailscale.md)
- [Pull-request previews](docs/pr-previews.md)
- [Engineering guide](GUIDE.md)
- [Rewrite architecture and parity plan](REWRITE.md)
