#!/bin/sh
CLUSTER_NAME="$1"
CLUSTER_ALIAS="${2:-$CLUSTER_NAME}"

if [ -f /run/secrets/ts-authkey ]; then
    export TS_AUTHKEY="$(cat /run/secrets/ts-authkey)"
fi

python3 /dns-proxy.py "$CLUSTER_NAME" "$CLUSTER_ALIAS" &
DNS_PID=$!

exec /usr/local/bin/containerboot
