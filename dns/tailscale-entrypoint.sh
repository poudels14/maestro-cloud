#!/bin/sh
CLUSTER_NAME="$1"
CLUSTER_ALIAS="${2:-$CLUSTER_NAME}"

if [ -f /run/secrets/ts-authkey ]; then
    export TS_AUTHKEY="$(cat /run/secrets/ts-authkey)"
fi

if [ -f /data/dns/Corefile ]; then
    /usr/local/bin/coredns -conf /data/dns/Corefile &
    COREDNS_PID=$!
    echo "coredns started (pid=$COREDNS_PID)" >&2
fi

python3 /dns-proxy.py "$CLUSTER_NAME" "$CLUSTER_ALIAS" &
DNS_PID=$!

# Watchdog: if tailscale goes offline (e.g. node identity dropped after sleep/wake),
# wipe stale node state and kill containerboot so the supervisor restarts with a fresh registration.
(
    sleep 60
    FAIL_COUNT=0
    while sleep 15; do
        ONLINE=$(tailscale status --json 2>/dev/null \
            | python3 -c "import json,sys; print(json.load(sys.stdin).get('Self',{}).get('Online',False))" 2>/dev/null)
        if [ "$ONLINE" = "True" ]; then
            FAIL_COUNT=0
        else
            FAIL_COUNT=$((FAIL_COUNT + 1))
            if [ "$FAIL_COUNT" -ge 4 ]; then
                echo "tailscale offline for 60s+, clearing state for re-auth..." >&2
                rm -rf /var/lib/tailscale/*
                kill 1
            fi
        fi
    done
) &

exec /usr/local/bin/containerboot
