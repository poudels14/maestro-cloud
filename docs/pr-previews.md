# Pull request previews

Maestro can create an ephemeral derived service for every open, non-draft pull request on a
preview-enabled service. PR `123` for service `app` becomes `app-pr-123`, runs one replica, and is
served at `app-pr-123.<preview-domain>`.

## Cluster configuration

Add the preview integration to the protected cluster config:

```jsonc
{
  "preview": {
    "domain": "preview.getbaton.ai",
    "github-token": "replace-with-a-fine-grained-token",
    "max-concurrent-previews": 10
  }
}
```

Use a fine-grained personal access token scoped to every preview-enabled repository with:

- Pull requests: read, for listing PRs and resolving their head commits.
- Deployments: read and write, for creating the native GitHub deployment card, publishing its
  lifecycle status, and attaching the ready preview URL.
- Contents: read if the same token is also used as the service's `build.secrets.items.GH_TOKEN` for
  a private repository checkout.

The integration token is not injected into builds. Private repositories must already build
successfully from the base service; previews inherit its build environment and secrets.

Preview discovery remains disabled until the cluster has a `preview` integration.

For an existing cluster, update the shared config source and restart each Maestro daemon. Every
node fetches the current source during startup; no preview token is copied into its launch document.

```sh
aws secretsmanager put-secret-value \
  --secret-id maestro/production/config.json \
  --secret-string file://maestro.jsonc
sudo systemctl restart maestro
```

## Service configuration

Add a `preview` block to a repository-backed service in `maestro.services.jsonc`:

```jsonc
{
  "services": {
    "app": {
      "name": "app",
      "build": {
        "repo": "git@github.com:Baton-AI/baton.git",
        "branch": "main",
        "dockerfile": "packages/app/Dockerfile",
        "watch": true,
        "secrets": {
          "items": { "GH_TOKEN": "$GH_TOKEN" }
        }
      },
      "deploy": {
        "replicas": 2,
        "env": {
          "items": {
            "LOG_FORMAT": "json",
            "FEATURE_FLAGS": "stable"
          }
        }
      },
      "ingress": { "host": "app.getbaton.ai", "port": 3000 },
      "preview": {
        "enabled": true,
        "closeGracePeriod": "1d",
        "replicas": 1,
        "env": {
          "items": {
            "FEATURE_FLAGS": "preview",
            "ANALYTICS_DISABLED": "1",
            "PREVIEW_URL": "https://${{ MAESTRO_INGRESS_HOST }}",
            "INGRESS_PORT": "${{ MAESTRO_INGRESS_PORT }}"
          }
        }
      }
    }
  }
}
```

`closeGracePeriod` accepts a positive integer followed by `s`, `m`, `h`, or `d`. It defaults to
`1d`. Preview environment items override base deployment environment items. Base environment
sources, build settings, Depot project, health checks, deploy secrets, node affinity, and egress
rules are inherited. Volumes are deliberately removed and replicas are fixed at one.

Runtime environment and mounted-secret values support `${{ MAESTRO_INGRESS_HOST }}` and
`${{ MAESTRO_INGRESS_PORT }}` for both normal and preview deployments. This includes values loaded
from external sources such as AWS Secrets Manager. Maestro resolves them to the service's one
concrete ingress hostname and configured ingress target port. A template may appear inside a larger
value, as shown above. Resolution fails when a referenced value is unavailable or ambiguous;
Maestro never guesses which ingress endpoint the application should use.

Preview-enabled services must use a `github.com` repository, define ingress, and have a lowercase
DNS-label service ID. The `-pr-` infix is reserved for Maestro's derived services. Fork PRs and
draft PRs are not deployed.

## Lifecycle

- Opening a PR queues its first native GitHub deployment.
- Pushing a commit queues a replacement deployment at the same URL.
- Editing the base service configuration re-derives and redeploys every open preview.
- Freezing the base service prevents preview creation and redeployment.
- Closing or merging a PR freezes its preview and starts `closeGracePeriod`.
- Reopening during the grace period cancels teardown and retains the existing preview.
- After the grace period, Maestro deletes the derived service, containers, routes, and eventually
  unused images. Reopening afterward creates it again.
- Disabling previews or deleting the base service removes its previews immediately.

The cluster-wide concurrency limit includes previews waiting in the close grace period. When the
limit is reached, Maestro publishes a queued deployment and admits the oldest open PR first when a
slot opens.

## DNS, tunnel, and access setup

The public preview hostname requires one-time infrastructure configuration outside Maestro:

1. Create a wildcard DNS record such as `*.preview.getbaton.ai` pointing at the existing tunnel.
2. Add a wildcard Cloudflare Tunnel public-hostname rule whose service URL is
   `http://web:8888`.
3. Optionally protect the wildcard with a Cloudflare Access policy.

`web` is Maestro's stable ingress alias. Maestro publishes it through the workload resolver with
every ready Traefik replica, so tunnel connectors retain a memorable, highly available origin as
workloads move between nodes. Port `8888` is the dedicated tunnel entrypoint; port `80` remains
available for direct cluster ingress. Do not configure a particular workload IP.

The DNS wildcard and tunnel rule must exist before public preview links can serve traffic. Internal
Maestro DNS continues to expose derived services through the cluster's canonical internal domain.

## Verification checklist

1. Open a same-repository, non-draft PR and wait for its GitHub deployment to become ready.
2. Open the deployment's environment URL and verify preview environment overrides.
3. Push a commit and verify the same URL receives a new deployment.
4. Close the PR and verify the deployment becomes inactive.
5. Reopen during the grace period and verify teardown is canceled.
6. Close it again and verify the service and route disappear after the grace period.
7. Exercise quota, base freeze, base configuration changes, and controller leadership failover.
