# Maestro

A deployment controller that manages containers with zero-downtime redeployments,
Traefik ingress routing, and optional Tailscale networking. Maestro supports both
Docker and containerd/nerdctl runtimes.

## Prerequisites

- Docker, or containerd + nerdctl + BuildKit
- Rust 1.97.0 or newer
- Protocol Buffers compiler (`protoc`)
- A C/C++ toolchain, CMake, and Make (required by native dependencies)

The included Nix development shell provides the build dependencies. On macOS
without Nix, install the platform tools with:

```bash
xcode-select --install
brew install protobuf cmake
```

## Installation

From the repository root, optionally enter the Nix development shell:

```bash
nix develop
```

Then build the CLI:

```bash
cargo build --release --locked --package controller --bin maestro
```

The binary is written to `target/release/maestro`.

## Quick start

### 1. Create the config files

```bash
maestro config init   # choose "cluster" to create maestro.jsonc
maestro config init   # choose "services" to create maestro.services.jsonc
```

Set a strong `encryption-key` in `maestro.jsonc`, then edit
`maestro.services.jsonc` with the services you want to deploy.

Validate both files before starting or deploying. The command reports the exact
path of missing or invalid fields and lists fields that Maestro will ignore:

```bash
maestro config validate maestro.jsonc
maestro config validate maestro.services.jsonc
# Uses the standard AWS credential chain and reads the secret string:
maestro config validate aws-secret://maestro/production/node1
```

### 2. Start the cluster

```bash
maestro daemon start \
  --config maestro.jsonc \
  --admin-port 3001 \
  --data-dir ./data \
  --project-dir .
```

Common startup overrides (cluster settings should normally live in `maestro.jsonc`):

- `--runtime docker|nerdctl` — container runtime (default: docker)
- `--admin-port` — localhost port for the admin UI and API proxy
- `--ingress-port` — host port mapped to ingress (repeat for multiple ports)
- `--subnet` — container network subnet CIDR, e.g. `172.22.0.0/16` (required)
- `--force` — recreate the container network if it already exists or conflicts
- `--encryption-key` — master key for encrypting secrets
- `--datadog-api-key` — Datadog API key (or `DATADOG_API_KEY`)
- `--datadog-site` — Datadog site (e.g. `datadoghq.com`, `us3.datadoghq.com`)
- `--datadog-no-ingress-logs` — exclude Traefik ingress logs from Datadog
- `--datadog-no-tailscale-logs` — exclude Tailscale logs from Datadog

### 3. Deploy services

Set the Maestro API context once:

```bash
maestro contexts set local http://127.0.0.1:3001
maestro contexts use local
```

```bash
maestro services rollout          # preview the diff
maestro services rollout --apply  # apply it
```

### 4. Redeploy a service

```bash
maestro services redeploy my-app
```

### 5. Execute a command in a replica

Interactive exec is available on Linux clusters using the `nerdctl` runtime. Enable it
explicitly in `maestro.jsonc` and keep the operator API reachable only through the private
Tailscale path. When `jwt-secret-key` is configured, the CLI context must provide a valid bearer
token; without a key, the operator API relies on the private network boundary:

```jsonc
{
  "runtime": "nerdctl",
  "allow-exec": true
}
```

Then connect to the active deployment. A single running replica is selected automatically;
multiple replicas open a picker unless `--replica` or `--node` narrows the selection.

```bash
maestro exec my-app
maestro exec my-app --replica 1
maestro exec my-app --no-tty -- env
maestro exec my-app -- /bin/sh -c 'id && pwd'
```

Use `~.` at the start of a terminal line to force-detach. Set `deploy.exec` to `false` on a
service that must never accept CLI exec sessions. Docker-runtime clusters, including Docker
Desktop on macOS, are not supported by this command. Enabling CLI exec grants authenticated
operators command execution inside workload containers; session metadata is logged, but terminal
input and output are never logged.

## Log queries

The log viewer search bar and `maestro logs --query` use the same server-side
query. Press Enter or click the search icon in the UI to apply it. Supported
Datadog-style syntax includes:

- Free text and quoted phrases: `connection refused`, `"upstream timeout"`
- Reserved fields: `level:error`, `status:warn`, `source:maestro-probe`,
  `service:my-app`, and `message:*timeout*`
- Log attributes: `@request_id:abc123` or `@http.url_details.path:/api/*`
- Numeric comparisons and ranges: `@duration:>1000000` and
  `@http.status_code:[400 TO 499]`
- `AND`, `OR`, `NOT`, `-`, parentheses, and `*`/`?` wildcards

`@http.status_code` is a canonical alias that also matches common application
status fields and Traefik's `DownstreamStatus` field. Queries run before limits
and cursors are applied, including against rolled-over Parquet logs.

```bash
maestro logs --service my-app \
  --query '@http.status_code:[500 TO 599] AND -message:*health*'
```

Use newline-delimited JSON while following logs or piping them to tools such as
`jq`. The default JSON shape contains `ts`, `level`, `message`, `source`, and an
`attributes` object:

```bash
maestro logs --query '@http.status_code:404' --output json | jq
```

Add `--full` for the lossless API record, including sequence, stream, origin,
tags, and raw attribute pairs:

```bash
maestro logs --query '@http.status_code:404' --output json --full | jq
```

## Config file

The config file (`maestro.jsonc`) supports:

```jsonc
{
  "cluster": { "name": "my-cluster" },
  "ingress": {
    // single port
    "port": 8080,
    // or multiple ports (both map to the internal ingress port 8888)
    "ports": [80, 443]
  },
  "subnet": "172.22.0.0/16",
  "encryption-key": "your-secret-key",
  "runtime": "nerdctl", // "docker" (default) or "nerdctl"
  "depot": {
    "token": "your-depot-token"
  },
  "homepage": "http://maestro.internal:3001",
  "github": {
    "token": "your-fine-grained-pat",
    "preview-domain": "preview.getbaton.ai",
    "poll-interval-secs": 60,
    "max-concurrent-previews": 10
  },
  "tailscale": { "auth-key": "tskey-auth-..." },
  "datadog": {
    "api-key": "your-dd-api-key",
    "site": "datadoghq.com",
    "include-ingress-logs": true,
    "include-tailscale-logs": true,
    "logs": {
      "include-healthcheck": true // default; set false to exclude successful checks
    },
    "include-metrics": false
  },
  "allow-cli-deployment": false,
  "allow-exec": false
}
```

Daemon startup fields accept both kebab-case and camelCase, so
`encryption-key`/`encryptionKey`, `cluster.join-secret`/`cluster.joinSecret`, and the
other multi-word startup fields are equivalent. Generated templates and masked
config output use kebab-case as the canonical form. The two spellings can be
mixed across `$extends` layers, but the same field cannot be set with both
spellings in one layer. User-defined `cluster.labels` keys are never rewritten.

Pass as `--config maestro.jsonc` or `--config aws-secret://secret-name`.

### Inheriting a shared startup config

Use `$extends` to keep shared cluster settings in one config and store only
node-specific values in each node's config. For example, an AWS secret named
`maestro/production/common` can contain the cluster, ingress, credentials, and
other shared settings. A node secret can then contain:

```jsonc
{
  "$extends": "aws-secret://maestro/production/common",
  "node": "node2"
}
```

Objects merge recursively. Arrays and scalar values in the node config replace
the inherited values. Configs can extend another config, with cycle detection and
a maximum depth of 16. Relative `$extends` paths resolve from a local config's
directory; configs loaded from a remote source must reference another
`aws-secret://` source or an absolute `file://` path. The effective precedence is
defaults, inherited bases, node config, then explicit CLI arguments.

The instance IAM role must allow `secretsmanager:GetSecretValue` for the node
secret and every AWS secret referenced through `$extends`.

To use Depot for a service build, set `depot.token` in `maestro.jsonc` and
`build.depot.project` in that service's `maestro.services.jsonc` entry. If either is
missing, Maestro falls back to the default local builder automatically.

## Per-service egress exceptions

Use `deploy.egress.allow` to exempt only one service from a matching global
`egress.deny` rule. Each rule requires a canonical IPv4 CIDR and can optionally
limit the destination ports:

```jsonc
{
  "services": {
    "api": {
      "name": "API",
      "image": "example/api:latest",
      "deploy": {
        "egress": {
          "allow": [{ "cidr": "10.0.10.0/24", "ports": [5432] }]
        }
      }
    }
  }
}
```

Configured ports apply to both TCP and UDP. When `ports` is empty or omitted,
the service can use any port in that destination CIDR. The exception follows all
replicas of the service across restarts, rolling deployments, and cluster nodes.
It has no effect unless the destination would otherwise be covered by the node's
global egress deny list.

For per-pull-request deployments, see [Pull request previews](docs/pr-previews.md).

## External Secrets

Maestro can load environment variables and secrets from external providers.
Currently supported: **AWS Secrets Manager**.

### `secrets.source`

Loads all key/value pairs from a JSON secret into `secrets.items`. The AWS secret
must be a flat JSON object.

```jsonc
{
  "deploy": {
    "secrets": {
      "mountPath": "/app/.env",
      "source": "aws-secret://prod/my-app-secrets"
    }
  }
}
```

If the AWS secret `prod/my-app-secrets` contains
`{ "DB_PASSWORD": "s3cret", "API_KEY": "key123" }`, both will be mounted in
`/app/.env`.

Explicit `items` take precedence over values loaded from `source`:

```jsonc
{
  "deploy": {
    "secrets": {
      "mountPath": "/app/.env",
      "source": "aws-secret://prod/my-app-secrets",
      "items": { "DB_PASSWORD": "override-value" }
    }
  }
}
```

### `deploy.env.source`

Loads all key/value pairs from a JSON secret into container environment variables.

```jsonc
{
  "deploy": {
    "env": {
      "source": "aws-secret://prod/my-app-env",
      "items": { "EXTRA_VAR": "literal-value" }
    }
  }
}
```

Explicit `items` take precedence over values from `source`.

### `build.env.source`

Same as `deploy.env.source`, but for Docker build args (`--build-arg`).

```jsonc
{
  "build": {
    "repo": "git@github.com:org/repo.git",
    "dockerfilePath": "Dockerfile",
    "env": {
      "source": "aws-secret://ci/build-tokens"
    }
  }
}
```

### `build.secrets`

Passes each item to Docker, nerdctl, or Depot as an environment-backed BuildKit
secret. The secret value is kept out of the build arguments and command line.

```jsonc
{
  "build": {
    "secrets": {
      "items": { "NPM_TOKEN": "${NPM_TOKEN}" }
    }
  }
}
```

Consume it from the Dockerfile with a secret mount:

```dockerfile
RUN --mount=type=secret,id=NPM_TOKEN,env=NPM_TOKEN,required=true pnpm fetch
```

This is equivalent to passing
`--secret id=NPM_TOKEN,env=NPM_TOKEN` to the image builder. The secret is available
only to that `RUN` instruction unless the Dockerfile explicitly persists it.

### Notes

- Secrets are resolved once when a deployment starts building. All replicas use the
  same resolved values, even across restarts.
- Resolved values are encrypted at rest in etcd.
- To add a new provider (e.g. Vault), implement the `SecretProvider` trait in
  `controller/src/utils/secrets.rs`.

## Tailscale setup

Tailscale enables remote access to your containers from any device on your tailnet.
For a routed multi-node cluster, use the topology, bootstrap, security, operations,
and chaos-test guide in [docs/multi-node.md](docs/multi-node.md).

### 1. Start with Tailscale enabled

Add Tailscale to `maestro.jsonc`. Maestro automatically advertises each node's
container subnet so tailnet devices can access its containers; use
`advertise-routes` only for additional networks.

```jsonc
{
  "cluster": { "name": "my-cluster" },
  "ingress": { "ports": [80, 443] },
  "subnet": "172.22.0.0/16",
  "encryption-key": "replace-with-a-strong-secret",
  "tailscale": {
    "auth-key": "tskey-auth-...",
    // Additional private networks such as VPC CIDRs for RDS or Redis.
    // The node's container subnet is advertised automatically.
    "advertise-routes": []
  }
}
```

```bash
maestro daemon start --config maestro.jsonc --data-dir ./data --project-dir .
```

### 2. Approve advertised routes

Approve every route Maestro advertises. For standalone installations this includes
the automatically discovered container subnet. For multi-node clusters, approve
each node's container subnet plus any additional routes listed in
`advertise-routes`.

Go to [admin.tailscale.com](https://admin.tailscale.com) > Machines, find
`maestro-tailscale-my-cluster`, select Edit route settings, and approve the
advertised subnet.

To auto-approve routes for all clusters, add this to your ACL policy under Access
Controls:

```json
{
  "autoApprovers": {
    "routes": {
      "172.16.0.0/12": ["tag:maestro"]
    }
  },
  "tagOwners": {
    "tag:maestro": ["autogroup:admin"]
  }
}
```

`172.16.0.0/12` covers `172.16.x.x` through `172.31.x.x`, so any container network
in that range is auto-approved. Narrow the policy if all clusters use a smaller
range.

Then generate an auth key tagged with `tag:maestro`.

### 3. Configure split DNS

In Tailscale admin > DNS > Add nameserver > Custom:

- Nameserver: the `.254` IP in the subnet's first `/24` (for example,
  `172.22.0.254` for `172.22.0.0/16`), shown in Maestro's log output
- Restrict to domain: `maestro.internal`

You only need **one** split DNS entry. The DNS proxy discovers peer clusters via
Tailscale and forwards queries across clusters.

Maestro preserves the fixed system addresses in that first `/24` for existing
single-node installations. Cluster scheduling assigns `.2` through `.199` to
workloads and reserves `.200` through `.254` for system use. Legacy standalone
runtime allocation continues using its configured subnet; fixed system
containers start before workloads and are reserved as active IPAM leases.

### 4. Access your services

```bash
# Via ingress
curl -H "Host: example.com" http://web.my-cluster.maestro.internal:8888/

# Via container hostname (port 80 is the container's internal port)
curl http://my-app-abc123.my-cluster.maestro.internal/

# Cross-cluster access works automatically
curl http://web.other-cluster.maestro.internal:8888/
```

### Multi-cluster setup

Each cluster needs a unique `cluster.name` and `subnet` to avoid routing conflicts.
For example, use `172.22.0.0/16` for `cluster-1` and `172.23.0.0/16` for
`cluster-2`, then start each from its own configuration and data directory:

```bash
maestro daemon start --config cluster-1.jsonc --data-dir ./data1 --project-dir .
maestro daemon start --config cluster-2.jsonc --data-dir ./data2 --project-dir .
```

Clusters discover each other via Tailscale. DNS queries for
`*.cluster-2.maestro.internal` that reach cluster-1 are forwarded to cluster-2's DNS
proxy.

## Log storage and backups

### Delivery and spool retention

The controller SQLite spool independently delivers each log to the probe and every
configured external sink, including Datadog. A spool row is reclaimed only after
the slowest registered sink has acknowledged it. Datadog failures therefore retain
the backlog locally until delivery recovers.

Retryable, authentication, rate-limit, network, server, and global payload failures
pin the Datadog cursor. Maestro bisects permanently rejected payloads to isolate a
poison entry and preserves that entry in the spool's `sink_dead_letters` table
before advancing the cursor.

Set `datadog.logs.include-healthcheck` to `false` to keep successful (`GET`/`200`)
health-check access logs out of Datadog. Maestro matches the service's configured
`healthcheckPath` against common structured HTTP log attributes. Failures,
non-health-check requests, and unstructured logs are still delivered. Filtered logs
remain available in the probe, UI, and S3 and count as acknowledged by Datadog for
controller spool retention. The dashboard's Datadog delivery row shows whether the
filter is active and how many entries it has excluded since the controller started.

Dead letters are capped at 100,000 rows. Reaching the cap pins the Datadog cursor
instead of growing the quarantine indefinitely. Inspect, export, and explicitly
purge them from the controller spool with:

```bash
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster list
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster export --output dead-letters.jsonl
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster purge --all
```

### Probe storage

The probe stores live logs in DuckDB under `/data/duckdb` and seals completed UTC
days into hive-partitioned Parquet files under `/data/parts`. `maestro daemon logs`
discovers the locally running probe API and reads live data. The controller SQLite
database remains a delivery spool only and continues shipping through `/api/logs`;
it is not queried as the telemetry store.

### S3 backups

Daily S3 backups are enabled through the cluster config:

```jsonc
{
  "log-backup": {
    "bucket": "my-maestro-logs",
    "kms-key-id": "arn:aws:kms:us-west-2:123456789012:key/...",
    "region": "us-west-2",
    // Optional object-key prefix and local retention after verified backup
    "prefix": "clusters/production",
    "retention-days": 30
  }
}
```

If no prefix is set, the generated cluster name is used to prevent different
clusters from writing the same object keys. In cluster mode, each uploaded
Parquet filename and partition manifest is prefixed with the stable node ID so
replicas of the same deployment cannot overwrite another node's log objects.

The probe uses the standard AWS credential-provider chain for authentication and
uploads every object with SSE-KMS and a SHA-256 checksum. Objects at least 100 MiB
use the AWS SDK's multipart API with per-part and composite SHA-256 verification;
failed uploads are explicitly aborted. It verifies object size, checksum, metadata,
and encryption before marking a partition backed up. Local retention is disabled
by default and never removes an unverified partition.

The probe role must allow `s3:AbortMultipartUpload`. Configure the backup bucket
with an `AbortIncompleteMultipartUpload` lifecycle rule as crash cleanup for uploads
that cannot reach the explicit abort path.

## Deploy to AWS (NixOS on EC2)

### Step 1: Generate and store the cluster config

Run `maestro config init` and choose `cluster` to generate `maestro.jsonc`. Update
it for your environment and store it in AWS Secrets Manager:

```bash
aws secretsmanager create-secret \
  --name <your-secret-id> \
  --secret-string file://maestro.jsonc
```

### Step 2: Launch an EC2 instance

- Use a [NixOS AMI](https://nixos.org/download#nixos-amazon)
- Attach an IAM role with `secretsmanager:GetSecretValue` permission for the secret created above
- Set the following user data:

```bash
#!/bin/bash
set -euo pipefail

# amazon-init runs user data at every boot. Track the initial NixOS transition so
# that critical AMI differences can be applied by rebooting without a reboot loop.
bootstrap_state=/var/lib/maestro-bootstrap
pending_system="$bootstrap_state/pending-system"
complete="$bootstrap_state/complete"
mkdir -p "$bootstrap_state"

if [ -s "$pending_system" ]; then
  expected=$(cat "$pending_system")
  current=$(readlink -f /run/current-system)

  if [ "$current" = "$expected" ]; then
    mv "$pending_system" "$complete"
    exit 0
  fi

  echo "Maestro target generation did not boot: $expected" >&2
  exit 1
fi

[ -e "$complete" ] && exit 0

# Enable flakes for this bootstrap without writing through NixOS's immutable
# /etc/nix/nix.conf symlink. The setting is declared persistently below.
export NIX_CONFIG='experimental-features = nix-command flakes'

mkdir -p /etc/maestro
cat > /etc/maestro/flake.nix << 'EOF'
{
  inputs.maestro.url = "github:poudels14/maestro-cloud/release";
  inputs.nixpkgs.follows = "maestro/nixpkgs";

  outputs = { maestro, nixpkgs, ... }: {
    nixosConfigurations.default = nixpkgs.lib.nixosSystem {
      system = "x86_64-linux";
      modules = [
        maestro.nixosModules.default
        ({ modulesPath, ... }: {
          imports = [ "${modulesPath}/virtualisation/amazon-image.nix" ];
          nix.settings.experimental-features = [ "nix-command" "flakes" ];
          services.maestro = {
            enable = true;
            config = "aws-secret://<your-secret-id>";
            runtime = "nerdctl";  # or "docker"
            extraArgs = [ "--admin-port" "3001" ];
          };
          services.amazon-ssm-agent.enable = true;
          networking.firewall.allowedTCPPorts = [ 80 443 22 ];
          system.stateVersion = "25.05";
        })
      ];
    };
  };
}
EOF

nixos-rebuild boot --flake /etc/maestro#default
readlink -f /nix/var/nix/profiles/system > "$pending_system"
systemctl reboot
```

Replace `<your-secret-id>` with the secret name from step 1.

The initial configuration is installed as the next boot generation instead of
being switched into the running AMI. This supports AMIs whose critical system
services differ from the pinned Maestro configuration. After the target generation
boots successfully, the persistent marker prevents `amazon-init` from rebooting the
instance again.

Forward local port 3001 to the instance with SSH or Session Manager, configure a
context for it, then trigger updates remotely:

```bash
maestro contexts set prod http://127.0.0.1:3001
maestro contexts use prod
maestro cluster upgrade
```

The `maestro cluster upgrade system` spelling is retained as an alias. Both forms
detect the installation topology: multi-node installations are drained and upgraded
serially, while a single-node installation upgrades that node directly. The CLI
sends its Cargo package version with the upgrade request. The controller accepts
the request only when that semantic version is newer than the version currently
running on the cluster. `maestro cluster info` and the UI info page show the running
version after the cluster comes back online. Bump
`controller/Cargo.toml` for every release; the Nix package reads the same version
automatically. On NixOS, the running controller resolves the updated flake source
and pre-builds the new system container images before rebooting. The old controller
and probe remain online during that build; startup after reboot reuses the completed
BuildKit cache instead of compiling the probe while the cluster is unavailable.
Maestro-owned images use exact release tags such as `maestro-probe:<version>`; upgrade
prebuilds use the version read from the updated source and never overwrite `:latest`.

Use `maestro cluster upgrade --batch=all` to make every node unschedulable and send
all node-local upgrade requests as one batch. The coordinator persists all request
watermarks before dispatch and sends its own request last, so the run resumes after
the cluster returns. This mode intentionally takes down services and the control
plane while the nodes restart; use the default rolling strategy when availability
must be preserved.

For coordinated upgrades, the elected leader owns the run in shared cluster state;
the CLI only streams that state and can disconnect without canceling the upgrade.
Each node reports its source update, validation, system rebuild, image pre-build,
restart, and failure stages. While a node reports active upgrade work, the leader
does not resend the node-local request. The entire node upgrade, including build,
installation, restart, and version-and-health verification, has one six-hour
deadline from the initial node-local request. Entering the restart stage does not
shorten or reset that deadline. Node-reported failures terminate the run immediately
and unfreeze the cluster. Coordinated restart operations retain their existing
two-minute timeout.

### Step 3: Manage the service

```bash
journalctl -u maestro        # view logs
systemctl restart maestro     # restart
systemctl status maestro      # check status
```
