# Maestro

A deployment controller that manages containers with zero-downtime redeployments, ingress routing via Traefik, and optional Tailscale networking for remote access. Supports both Docker and containerd/nerdctl runtimes.

## Prerequisites

- Docker **or** containerd + nerdctl + buildkit (for nerdctl runtime)
- Rust toolchain (for building from source)

## Installation

```bash
cd controller
cargo build --release
```

## Quick start

### 1. Create the config files

```bash
maestro config init   # choose "cluster" to create maestro.jsonc
maestro config init   # choose "services" to create maestro.cluster.jsonc
```

Set a strong `encryption-key` in `maestro.jsonc`, then edit `maestro.cluster.jsonc` with the services you want to deploy.

### 2. Start the cluster

```bash
maestro daemon start \
  --config maestro.jsonc \
  --admin-port 3001 \
  --data-dir ./data \
  --project-dir .
```

Flags:

- `--runtime docker|nerdctl` — container runtime (default: docker)
- `--admin-port` — localhost port for the admin UI and API proxy
- `--ingress-port` — host port(s) mapped to the ingress (can be repeated for multiple ports)
- `--subnet` — container network subnet CIDR, e.g. `172.22.0.0/16` (required)
- `--force` — recreate the container network if it already exists or conflicts
- `--encryption-key` — master key for encrypting secrets
- `--datadog-api-key` — Datadog API key for log forwarding (or `DATADOG_API_KEY` env var)
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
    "include-metrics": false
  },
  "allow-cli-deployment": false
}
```

Pass as `--config maestro.jsonc` or `--config aws-secret://secret-name`.

To use Depot for a service build, set `depot.token` in `maestro.jsonc` and `build.depot.project` in that service's `maestro.cluster.jsonc` entry. If either is missing, Maestro falls back to the default local builder automatically.

## External Secrets

Maestro can load env vars and secrets from external providers. Currently supported: **AWS Secrets Manager**.

### `secrets.source`

Loads all key/value pairs from a JSON secret into `secrets.items`. The AWS secret must be a flat JSON object.

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

If the AWS secret `prod/my-app-secrets` contains `{ "DB_PASSWORD": "s3cret", "API_KEY": "key123" }`, both will be mounted in `/app/.env`.

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

- Secrets are resolved once when a deployment starts building. All replicas use the same resolved values, even across restarts.
- Resolved values are encrypted at rest in etcd.
- To add a new provider (e.g. Vault), implement the `SecretProvider` trait in `controller/src/utils/secrets.rs`.

## Tailscale setup

Tailscale enables remote access to your containers from any device on your tailnet.

### 1. Start with Tailscale enabled

```bash
export TS_AUTHKEY=tskey-auth-...
export MAESTRO_ENCRYPTION_KEY=replace-with-a-strong-secret
maestro daemon start \
  --cluster-name my-cluster \
  --ingress-port 80 --ingress-port 443 \
  --data-dir ./data \
  --subnet 172.22.0.0/16 \
  --enable-tailscale \
  --project-dir .
```

### 2. Approve the subnet route

Go to [admin.tailscale.com](https://admin.tailscale.com) > Machines > find `maestro-tailscale-my-cluster` > Edit route settings > approve the advertised subnet.

To auto-approve routes for all clusters, add to your ACL policy (Access Controls in Tailscale admin):

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

`172.16.0.0/12` covers `172.16.x.x` through `172.31.x.x`, so any Docker network subnet is auto-approved. If you use a specific subnet (e.g., `--subnet 172.22.0.0/16`), you can narrow it down.

Then generate an auth key tagged with `tag:maestro`.

### 3. Configure split DNS

In Tailscale admin > DNS > Add nameserver > Custom:

- Nameserver: the `.255` IP of your subnet (e.g., `172.22.0.255` for `172.22.0.0/16`), shown in maestro's log output
- Restrict to domain: `maestro.internal`

You only need **one** split DNS entry — the DNS proxy auto-discovers peer clusters via Tailscale and forwards queries across clusters.

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

Each cluster needs its own subnet to avoid IP conflicts:

```bash
export TS_AUTHKEY=tskey-auth-...
export MAESTRO_ENCRYPTION_KEY=replace-with-a-strong-secret

# Cluster 1
maestro daemon start --cluster-name cluster-1 --ingress-port 8888 --data-dir ./data1 --subnet 172.22.0.0/16 --enable-tailscale --project-dir .

# Cluster 2
maestro daemon start --cluster-name cluster-2 --ingress-port 8889 --data-dir ./data2 --subnet 172.23.0.0/16 --enable-tailscale --project-dir .
```

Clusters auto-discover each other via Tailscale. DNS queries for `*.cluster-2.maestro.internal` hitting cluster-1's DNS are automatically forwarded to cluster-2's DNS proxy.

## Log storage and backups

The probe stores live logs in DuckDB under `/data/duckdb` and seals completed UTC
days into hive-partitioned Parquet files under `/data/parts`. Set
`MAESTRO_DUCKDB=false` on the controller only as a temporary rollback switch during
the migration window; this changes only the probe storage backend.
`maestro daemon logs` discovers the locally running probe API and reads live data
rather than opening the retired controller SQLite database.

The controller SQLite spool independently delivers each log to the probe and every
configured external sink, including Datadog. A spool row is reclaimed only after
the slowest registered sink has acknowledged it. Datadog failures therefore retain
the backlog locally until delivery recovers. Datadog payloads are kept below the
uncompressed intake limit and `400`/`413` responses are bisected to isolate poison
entries. An isolated `400` is quarantined only when a sibling payload succeeds;
if every subdivision receives `400`, the response is treated as global and the
cursor remains pinned. An irreducible rejected entry is preserved in the spool
database's `sink_dead_letters` table before its cursor advances. Authentication,
rate-limit, network, and server failures also pin the cursor.

Dead letters are capped at 100,000 rows. Reaching the cap pins the Datadog cursor
instead of growing the quarantine indefinitely. Inspect, export, and explicitly
purge them from the controller spool with:

```bash
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster list
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster export --output dead-letters.jsonl
maestro daemon dead-letters --data-dir ./data --cluster-name my-cluster purge --all
```

On first DuckDB startup, the probe automatically imports its retired `/data/logs.db`
archive. Active controller spool databases are never migration inputs; they continue
shipping through `/api/logs`.

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
clusters from writing the same object keys.

The probe uses the standard AWS credential-provider chain for authentication and
uploads every object with SSE-KMS and a SHA-256 checksum. Objects at least 100 MiB
use the AWS SDK's multipart API with per-part and composite SHA-256 verification;
failed uploads are explicitly aborted. It verifies object size, checksum, metadata,
and encryption before marking a partition backed up. Local retention is disabled
by default and never removes an unverified partition.

The probe role must allow `s3:AbortMultipartUpload`. Configure the backup bucket
with an `AbortIncompleteMultipartUpload` lifecycle rule as crash cleanup for uploads
that cannot reach the explicit abort path. The ignored real-AWS integration test can
be run manually with `MAESTRO_TEST_S3_BUCKET`, `MAESTRO_TEST_S3_KMS_KEY_ID`, and
`MAESTRO_TEST_S3_REGION`; the CI workflow runs it on manual dispatch using secrets
with the same names plus `MAESTRO_TEST_AWS_ROLE_ARN` for GitHub OIDC credentials.

## Deploy to AWS (NixOS on EC2)

### Step 1: Generate and store the cluster config

Run `maestro config init` and choose `cluster` to generate a `maestro.jsonc` config, then update it for your environment and store it in AWS Secrets Manager:

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
  inputs.maestro.url = "github:poudels14/maestro-cloud";
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

Forward local port 3001 to the instance with SSH or Session Manager, configure a context for it, then trigger updates remotely:

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
automatically.

### Step 3: Manage the service

```bash
journalctl -u maestro        # view logs
systemctl restart maestro     # restart
systemctl status maestro      # check status
```
