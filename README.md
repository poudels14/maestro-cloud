# Maestro

A deployment controller that manages Docker containers with zero-downtime redeployments, ingress routing via Traefik, and optional Tailscale networking for remote access.

## Prerequisites

- Docker
- Rust toolchain (for building from source)

## Installation

```bash
cd controller
cargo build --release
```

## Quick start

### 1. Create a config file

```bash
maestro init
```

This generates a `maestro.jsonc` file in your working directory with a sample service configuration.

### 2. Start the cluster

```bash
maestro start --cluster-name my-cluster --port 8888 --data-dir ./data
```

### 3. Deploy services

```bash
maestro rollout
```

### 4. Redeploy a service

```bash
maestro redeploy my-app
```

## Tailscale setup

Tailscale enables remote access to your containers from any device on your tailnet.

### 1. Start with Tailscale enabled

```bash
export TS_AUTHKEY=tskey-auth-...
maestro start \
  --cluster-name my-cluster \
  --port 8888 \
  --data-dir ./data \
  --subnet 172.22.0.0/16 \
  --enable-tailscale
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
# Cluster 1
maestro start --cluster-name cluster-1 --port 8888 --data-dir ./data1 --subnet 172.22.0.0/16 --enable-tailscale

# Cluster 2
maestro start --cluster-name cluster-2 --port 8889 --data-dir ./data2 --subnet 172.23.0.0/16 --enable-tailscale
```

Clusters auto-discover each other via Tailscale. DNS queries for `*.cluster-2.maestro.internal` hitting cluster-1's DNS are automatically forwarded to cluster-2's DNS proxy.

## Deploy to AWS (NixOS on EC2)

### Step 1: Generate and store the cluster config

Run `maestro init` to generate a `maestro.jsonc` config, then update it for your environment and store it in AWS Secrets Manager:

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

Use `maestro upgrade system` to trigger updates remotely.

### Step 3: Manage the service

```bash
journalctl -u maestro        # view logs
systemctl restart maestro     # restart
systemctl status maestro      # check status
```
