# Pull request previews

Maestro can create an ephemeral derived service for every open, non-draft pull request on a
preview-enabled service. PR `123` for service `app` becomes `app-pr-123`, runs one replica, and is
served at `app-pr-123.<preview-domain>`.

## Cluster configuration

Add the GitHub integration to `maestro.jsonc`:

```jsonc
{
  "homepage": "http://maestro.internal:3001",
  "github": {
    "token": "replace-with-a-fine-grained-token",
    "preview-domain": "preview.getbaton.ai",
    "poll-interval-secs": 60,
    "max-concurrent-previews": 10
  }
}
```

`homepage` is optional. When configured, failed-preview comments link directly to the build logs in
the Maestro UI. It must be an absolute `http://` or `https://` URL.

Use a fine-grained personal access token scoped to every preview-enabled repository with:

- Pull requests: read and write, for listing PRs and maintaining the sticky preview comment.
- Contents: read if the same token is also used as the service's `build.secrets.items.GH_TOKEN` for
  a private repository checkout.

The integration token is not injected into builds. Private repositories must already build
successfully from the base service; previews inherit its build environment and secrets.

Maestro rejects preview-enabled service rollouts when the cluster has no `github` configuration.

## Service configuration

Add a `preview` block to a repository-backed service in `maestro.cluster.jsonc`:

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
            "ANALYTICS_DISABLED": "1"
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

Preview-enabled services must use a `github.com` repository, define ingress, and have a lowercase
DNS-label service ID. The `-pr-` infix is reserved for Maestro's derived services. Fork PRs and
draft PRs are not deployed.

## Lifecycle

- Opening a PR queues its first deployment and creates a sticky GitHub comment.
- Pushing a commit queues a replacement deployment at the same URL.
- Editing the base service configuration re-derives and redeploys every open preview.
- Freezing the base service prevents preview creation and redeployment.
- Closing or merging a PR freezes its preview and starts `closeGracePeriod`.
- Reopening during the grace period cancels teardown and retains the existing preview.
- After the grace period, Maestro deletes the derived service, containers, routes, and eventually
  unused images. Reopening afterward creates it again.
- Disabling previews or deleting the base service removes its previews immediately.

The cluster-wide concurrency limit includes previews waiting in the close grace period. When the
limit is reached, Maestro posts a quota state and admits the oldest open PR first when a slot opens.

## DNS, tunnel, and access setup

The public preview hostname requires one-time infrastructure configuration outside Maestro:

1. Create a wildcard DNS record such as `*.preview.getbaton.ai` pointing at the existing tunnel.
2. Add a wildcard Cloudflare Tunnel public-hostname rule forwarding to the Maestro ingress port.
3. Optionally protect the wildcard with a Cloudflare Access policy.

The DNS wildcard and tunnel rule must exist before public preview links can serve traffic. Internal
Maestro DNS continues to expose derived services through the cluster's canonical internal domain.

## Verification checklist

1. Open a same-repository, non-draft PR and wait for the sticky comment to become ready.
2. Open its preview URL and verify preview environment overrides.
3. Push a commit and verify the same URL receives a new deployment.
4. Close the PR and verify the scheduled removal time in the comment.
5. Reopen during the grace period and verify teardown is canceled.
6. Close it again and verify the service and route disappear after the grace period.
7. Exercise quota, base freeze, base configuration changes, and controller leadership failover.
