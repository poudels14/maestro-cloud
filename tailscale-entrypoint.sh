#!/bin/sh
CLUSTER_NAME="$1"

python3 /dns-proxy.py "$CLUSTER_NAME" &
DNS_PID=$!

exec /usr/local/bin/containerboot
