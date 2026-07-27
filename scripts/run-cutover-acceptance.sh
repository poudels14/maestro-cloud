#!/usr/bin/env bash

set -euo pipefail

for command in cargo etcd etcdutl ip sudo; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "cutover acceptance requires Linux" >&2
  exit 1
fi

etcd_binary=$(command -v etcd)
etcdutl_binary=$(command -v etcdutl)
ip_binary=$(command -v ip)
interface="mcut${$}"
interface=${interface:0:15}

cleanup() {
  status=$?
  trap - EXIT INT TERM
  sudo "$ip_binary" link delete "$interface" >/dev/null 2>&1 || true
  exit "$status"
}
trap cleanup EXIT INT TERM

# These addresses exist only on this disposable dummy interface. The real
# migration test needs three distinct non-loopback endpoints to prove restored
# peer URLs and quorum membership.
sudo "$ip_binary" link add "$interface" type dummy
sudo "$ip_binary" address add 10.254.253.11/32 dev "$interface"
sudo "$ip_binary" address add 10.254.253.12/32 dev "$interface"
sudo "$ip_binary" address add 10.254.253.13/32 dev "$interface"
sudo "$ip_binary" link set "$interface" up

MAESTRO_ETCD_BIN="$etcd_binary" \
  MAESTRO_ETCDUTL_BIN="$etcdutl_binary" \
  MAESTRO_ETCD_TEST_IPS=10.254.253.11,10.254.253.12,10.254.253.13 \
  cargo test \
    -p migrate \
    real_etcd_tests::real_cutover_restores_three_member_encrypted_store \
    -- \
    --ignored \
    --nocapture
