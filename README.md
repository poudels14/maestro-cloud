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

Create `maestro.jsonc` in your working directory:

```jsonc
{
  "$schema": "./maestro.schema.json",
  "services": {
    "my-app": {
      "name": "My App",
      "image": "traefik/whoami",
      "ingress": {
        "host": "example.com",
        "port": 80
      },
      "deploy": {
        "replicas": 1,
        "healthcheckPath": "/health"
      }
    }
  }
}
```

### 2. Start the cluster

```bash
maestro start --cluster-name my-cluster --data-dir ./data
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
  --data-dir ./data \
  --enable-tailscale \
  --nameserver-ip 172.22.0.4 # (optional) assigns a fixed IP to the DNS container so it doesn't change across restarts
```

### 2. Approve the subnet route

Go to [admin.tailscale.com](https://admin.tailscale.com) > Machines > find `maestro-my-cluster` > Edit route settings > approve the advertised subnet.

To auto-approve routes, add to your ACL policy:

```json
{
  "autoApprovers": {
    "routes": {
      "172.22.0.0/16": ["tag:maestro"]
    }
  },
  "tagOwners": {
    "tag:maestro": ["autogroup:admin"]
  }
}
```

Then generate an auth key tagged with `tag:maestro`.

### 3. Configure split DNS

In Tailscale admin > DNS > Add nameserver > Custom:

- Nameserver: the IP from maestro's log output (or your `--nameserver-ip` value)
- Restrict to domain: `my-cluster.maestro.internal`

### 4. Access your services

```bash
# Via container hostname
curl http://my-app-abc123.my-cluster.maestro.internal/

# Via ingress
curl -H "Host: example.com" http://web.my-cluster.maestro.internal:8888/
```
