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
maestro config init   # choose "services" to create maestro.cluster.jsonc
```

Set a strong `encryption-key` in `maestro.jsonc`, then edit
`maestro.cluster.jsonc` with the services you want to deploy.

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
  "allow-cli-deployment": false
}
```

Pass as `--config maestro.jsonc` or `--config aws-secret://secret-name`.

To use Depot for a service build, set `depot.token` in `maestro.jsonc` and
`build.depot.project` in that service's `maestro.cluster.jsonc` entry. If either is
missing, Maestro falls back to the default local builder automatically.

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

Add Tailscale to `maestro.jsonc`. Maestro automatically advertises the cluster's
container subnet; use `advertise-routes` only for additional networks.

```jsonc
{
  "cluster": { "name": "my-cluster" },
  "ingress": { "ports": [80, 443] },
  "subnet": "172.22.0.0/16",
  "encryption-key": "replace-with-a-strong-secret",
  "tailscale": {
    "auth-key": "tskey-auth-...",
    "advertise-routes": []
  }
}
```

```bash
maestro daemon start --config maestro.jsonc --data-dir ./data --project-dir .
```

### 2. Approve the subnet route

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

- Nameserver: the `.255` IP of your subnet (for example, `172.22.0.255` for
  `172.22.0.0/16`), shown in Maestro's log output
- Restrict to domain: `maestro.internal`

You only need **one** split DNS entry. The DNS proxy discovers peer clusters via
Tailscale and forwards queries across clusters.

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
controller spool retention.

Dead letters are capped at 100,000 rows. Reaching the cap pins the Datadog cursor
instead of growing the quarantine indefinitely. Inspect, export, and explicitly
purge them from the controller spool with:

```bash
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster list
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster export --output dead-letters.jsonl
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster purge --all
```

### Probe storage and migration

The probe stores live logs in DuckDB under `/data/duckdb` and seals completed UTC
days into hive-partitioned Parquet files under `/data/parts`. `maestro daemon logs`
discovers the locally running probe API and reads live data rather than opening the
retired controller SQLite database.

On first DuckDB startup, the probe automatically imports its retired `/data/logs.db`
archive. Active controller spool databases are never migration inputs; they continue
shipping through `/api/logs`.

Set `MAESTRO_DUCKDB=false` on the controller only as a temporary rollback switch
during the migration window. It changes the probe storage backend; it does not
change controller spool delivery.

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
echo "experimental-features = nix-command flakes" >> /etc/nix/nix.conf

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

nixos-rebuild switch --flake /etc/maestro#default
```

Replace `<your-secret-id>` with the secret name from step 1.

Forward local port 3001 to the instance with SSH or Session Manager, configure a
context for it, then trigger updates remotely:

```bash
maestro contexts set prod http://127.0.0.1:3001
maestro contexts use prod
maestro cluster upgrade system
```

The CLI sends its Cargo package version with the upgrade request. The controller
accepts the request only when that semantic version is newer than the version
currently running on the cluster. `maestro cluster info` and the UI info page show
the running version after the cluster comes back online. Bump
`controller/Cargo.toml` for every release; the Nix package reads the same version
automatically. On NixOS, the running controller resolves the updated flake source
and pre-builds the new system container images before rebooting. The old controller
and probe remain online during that build; startup after reboot reuses the completed
BuildKit cache instead of compiling the probe while the cluster is unavailable.
Maestro-owned images use exact release tags such as `maestro-probe:<version>`; upgrade
prebuilds use the version read from the updated source and never overwrite `:latest`.

### Step 3: Manage the service

```bash
journalctl -u maestro        # view logs
systemctl restart maestro     # restart
systemctl status maestro      # check status
```
