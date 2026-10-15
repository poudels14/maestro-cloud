#!/usr/bin/env bash
# Point a Cloudflare tunnel's public hostnames at a new origin service.
#
# The dashboard rejects multi-level wildcard hostnames such as *.preview.sandbox.example.com,
# so this edits the tunnel's remote configuration through the Cloudflare API instead.
#
# Usage:
#   CF_API_TOKEN=... scripts/set-tunnel-origin.sh [--tunnel NAME] [--account ID] [--origin URL] [--from URL] [--yes]
#
#   CF_API_TOKEN   API token with Account > Cloudflare Tunnel > Edit
#   --tunnel NAME  tunnel to change, by name or id (default: the account's only tunnel)
#   --account ID   account to use (default: the only account the API token can access)
#   --origin URL   new service for the hostname rules (default: http://localhost:8000)
#   --from URL     only rewrite rules whose service is exactly URL (default: every hostname rule)
#   --yes          apply without asking
set -euo pipefail

origin="http://localhost:8000"
from=""
tunnel_name=""
account=""
assume_yes=false
while [ $# -gt 0 ]; do
  case "$1" in
    --tunnel) tunnel_name="$2"; shift 2 ;;
    --account) account="$2"; shift 2 ;;
    --origin) origin="$2"; shift 2 ;;
    --from) from="$2"; shift 2 ;;
    --yes) assume_yes=true; shift ;;
    -h|--help) sed -n '2,15p' "$0" | sed 's/^# \{0,1\}//'; exit 0 ;;
    *) echo "unknown argument: $1" >&2; exit 2 ;;
  esac
done

for tool in curl jq; do
  command -v "$tool" >/dev/null || { echo "$tool is required" >&2; exit 1; }
done
: "${CF_API_TOKEN:?set CF_API_TOKEN to an API token with Cloudflare Tunnel edit access}"

api="https://api.cloudflare.com/client/v4"
cf() { curl -fsS -H "Authorization: Bearer $CF_API_TOKEN" "$@"; }

if [ -z "$account" ]; then
  accounts=$(cf "$api/accounts" | jq -r '.result[] | "\(.id)\t\(.name)"')
  if [ "$(grep -c . <<<"$accounts")" != 1 ]; then
    echo "the API token can access several accounts (or none); pass --account ID:" >&2
    printf '%s\n' "$accounts" >&2
    exit 1
  fi
  account=$(cut -f1 <<<"$accounts")
fi

tunnels=$(cf "$api/accounts/$account/cfd_tunnel?is_deleted=false&per_page=100" \
  | jq -r '.result[] | "\(.id)\t\(.name)"')
if [ -n "$tunnel_name" ]; then
  match=$(awk -F'\t' -v wanted="$tunnel_name" '$1 == wanted || $2 == wanted' <<<"$tunnels")
  tunnel=$(cut -f1 <<<"$match")
  tunnel_name=$(cut -f2 <<<"$match")
elif [ "$(grep -c . <<<"$tunnels")" = 1 ]; then
  tunnel=$(cut -f1 <<<"$tunnels")
  tunnel_name=$(cut -f2 <<<"$tunnels")
else
  tunnel=""
fi
if [ -z "$tunnel" ]; then
  echo "pick a tunnel with --tunnel NAME; tunnels in this account:" >&2
  cut -f2 <<<"$tunnels" | sed 's/^/  /' >&2
  exit 1
fi
endpoint="$api/accounts/$account/cfd_tunnel/$tunnel/configurations"

current=$(cf "$endpoint")
if [ "$(jq -r '.success' <<<"$current")" != "true" ]; then
  echo "could not read the tunnel configuration:" >&2
  jq '.errors' <<<"$current" >&2
  exit 1
fi

rules() { jq -r '.config.ingress[] | "  \(.hostname // "(catch-all)") -> \(.service)"'; }

echo "tunnel $tunnel_name ($tunnel), current rules:"
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

result=$(cf -X PUT -H "Content-Type: application/json" --data "$updated" "$endpoint")
if [ "$(jq -r '.success' <<<"$result")" = "true" ]; then
  echo "applied; connectors pick it up within a few seconds"
else
  echo "Cloudflare rejected the update:" >&2
  jq '.errors' <<<"$result" >&2
  exit 1
fi
