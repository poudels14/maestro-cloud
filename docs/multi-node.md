# Multi-node clusters

A Maestro cluster has 1, 3, or 5 **voters** and any number of **agents**.

- **Voters** run the control plane (Raft over mTLS gRPC with redb storage, the API,
  the scheduler, the deployer, and the build queue) and also run workloads.
- **Agents** run workloads only. They hold no cluster state and receive exactly the
  replica plans, routes, DNS records, and mesh peers they need from the leader.

Use three voters for production; the cluster stays writable while a majority of
voters is up. Agents can scale to hundreds of nodes.

## Ports

| Port | Protocol | Between | Purpose |
|---|---|---|---|
| 7400 | TCP | all nodes and voters | gRPC: Raft, joins, and the agent control stream (mTLS) |
| 51820 | UDP | all nodes | WireGuard mesh carrying replica traffic |
| 3000 | TCP | localhost on voters | token-protected HTTP API for the CLI |
| 80, 443 | TCP | public or load balancer | Traefik ingress |

Ports are fixed; nothing is randomly assigned. The NixOS module opens 7400, 51820,
and the ingress ports when `openFirewall` is set. On AWS, allow 7400/tcp and
51820/udp from the cluster's security group.

## Forming a cluster

1. Initialize the first node (done automatically by the NixOS module when
   `join.server` is unset):

   ```bash
   sudo maestro server init --advertise 10.20.0.11:7400 [--config maestro.jsonc]
   sudo systemctl start maestro
   ```

   This creates the cluster CA, the node certificate, the data encryption key, the API
   signing key, and the join secret in the data dir, then bootstraps a one-voter Raft
   group.

2. Print a join token on any voter:

   ```bash
   sudo maestro server join-token
   # mj1.<ca-sha256-fingerprint>.<secret>
   ```

3. Join other nodes:

   ```bash
   sudo maestro join --server 10.20.0.11:7400 --token "$TOKEN" \
     --advertise 10.20.0.12:7400 --role voter   # or agent (default)
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

## How the control plane scales

Only desired state goes through Raft: services, deployments, encrypted secrets,
assignments, and node records. Heartbeats, replica health, and status never touch
consensus. Every agent holds one long-lived stream to the leader, sends its replica
statuses (on change and every 5 seconds), and receives plans, routes, DNS records,
and mesh peers only when they change. A follower that receives an agent redirects it
to the leader.

## Failure handling

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

## Networking

Each node owns a `/24` (for example `10.200.2.0/24`) on its `maestro0` bridge.
The WireGuard interface `maestro-wg` carries each peer's `/24`, so any replica can
reach any other replica by IP and DNS returns replica IPs directly. Container traffic
to the internet is masqueraded by the node; traffic inside the cluster CIDR keeps its
source address.

## Builds in multi-node clusters

Set `build.registry` for every service that Maestro builds. The building node pushes
`<registry>/<service>:<deployment>`, and other nodes pull it. Registry credentials
must currently be configured on each host.

## Local multi-node testing

`dev/lima/maestro-node.yaml` creates additional Lima VMs on Lima's shared `user-v2`
network next to the `maestro-dev` VM. Initialize `maestro-dev` with its `user-v2`
address as `--advertise`, copy the Linux binary to the new VM, and join it.
