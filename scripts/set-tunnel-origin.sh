#!/usr/bin/env bash
# Point a Cloudflare tunnel's public hostnames at a new origin service.
#
# The dashboard rejects multi-level wildcard hostnames such as *.preview.sandbox.example.com,
# so this edits the tunnel's remote configuration through the Cloudflare API instead.
#
# Usage:
#   CF_API_TOKEN=... TUNNEL_TOKEN=... scripts/set-tunnel-origin.sh [--origin URL] [--from URL] [--yes]
#
#   CF_API_TOKEN   API token with Account > Cloudflare Tunnel > Edit
#   TUNNEL_TOKEN   the tunnel's connector token (cloudflare.tunnel.token in the cluster config);
#                  the account and tunnel ids are read from it
#   --origin URL   new service for the hostname rules (default: http://localhost:8000)
#   --from URL     only rewrite rules whose service is exactly URL (default: every hostname rule)
#   --yes          apply without asking
set -euo pipefail

origin="http://localhost:8000"
from=""
assume_yes=false
while [ $# -gt 0 ]; do
  case "$1" in
    --origin) origin="$2"; shift 2 ;;
    --from) from="$2"; shift 2 ;;
    --yes) assume_yes=true; shift ;;
    -h|--help) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for tool in curl jq base64; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
: "${CF_API_TOKEN:?set CF_API_TOKEN to an API token with Cloudflare Tunnel edit access}"
: "${TUNNEL_TOKEN:?set TUNNEL_TOKEN to the tunnel connector token}"

ids=$(printf '%s' "$TUNNEL_TOKEN" | base64 -d 2>/dev/null | jq -er '"\(.a) \(.t)"') || {
  echo "TUNNEL_TOKEN is not a valid tunnel token" >&2
  exit 1
}
read -r account tunnel <<<"$ids"
endpoint="https://api.cloudflare.com/client/v4/accounts/$account/cfd_tunnel/$tunnel/configurations"

current=$(curl -fsS -H "Authorization: Bearer $CF_API_TOKEN" "$endpoint")
if [ "$(jq -r '.success' <<<"$current")" != "true" ]; then
  echo "could not read the tunnel configuration:" >&2
  jq '.errors' <<<"$current" >&2
  exit 1
fi

rules() { jq -r '.config.ingress[] | "  \(.hostname // "(catch-all)") -> \(.service)"'; }

echo "tunnel $tunnel, current rules:"
jq '.result' <<<"$current" | rules

updated=$(jq --arg origin "$origin" --arg from "$from" '
  {config: (.result.config
    | .ingress |= map(
        if .hostname and ($from == "" or .service == $from)
        then .service = $origin
        else .
        end))}' <<<"$current")

if [ "$(jq -c '.config' <<<"$updated")" = "$(jq -c '.result.config' <<<"$current")" ]; then
  echo "nothing to change"
  exit 0
fi

echo
echo "new rules:"
rules <<<"$updated"

if [ "$assume_yes" != true ]; then
  read -r -p "apply? [y/N] " answer
  [ "$answer" = "y" ] || [ "$answer" = "Y" ] || { echo "not applied"; exit 1; }
fi

result=$(curl -fsS -X PUT -H "Authorization: Bearer $CF_API_TOKEN" -H "Content-Type: application/json" \
  --data "$updated" "$endpoint")
if [ "$(jq -r '.success' <<<"$result")" = "true" ]; then
  echo "applied; connectors pick it up within a few seconds"
else
  echo "Cloudflare rejected the update:" >&2
  jq '.errors' <<<"$result" >&2
  exit 1
fi
