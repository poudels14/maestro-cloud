#!/usr/bin/env bash

set -euo pipefail

for command in cargo containerd ctr etcd ip jq modprobe nft ping sudo sysctl; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "real cluster acceptance requires Linux" >&2
  exit 1
fi

acceptance_root=$(mktemp -d /tmp/maestro-real-cluster.XXXXXX)
containerd_socket="$acceptance_root/containerd.sock"
containerd_pid=
wireguard_probe="mrw${$}"
wireguard_probe=${wireguard_probe:0:15}

cleanup() {
  status=$?
  trap - EXIT INT TERM

  sudo ip link delete "$wireguard_probe" >/dev/null 2>&1 || true
  if [[ -n "$containerd_pid" ]]; then
    sudo kill "$containerd_pid" >/dev/null 2>&1 || true
    wait "$containerd_pid" 2>/dev/null || true
  fi
  if [[ "$status" -ne 0 && -f "$acceptance_root/containerd.log" ]]; then
    echo "===== containerd.log =====" >&2
    tail -n 200 "$acceptance_root/containerd.log" >&2 || true
  fi

  case "$acceptance_root" in
    /tmp/maestro-real-cluster.*)
      sudo rm -rf -- "$acceptance_root"
      ;;
    *)
      echo "refusing to remove unexpected acceptance root: $acceptance_root" >&2
      status=1
      ;;
  esac

  exit "$status"
}
trap cleanup EXIT INT TERM

sudo modprobe wireguard
sudo ip link add "$wireguard_probe" type wireguard
sudo ip link delete "$wireguard_probe"

sudo containerd \
  --log-level warn \
  --address "$containerd_socket" \
  --root "$acceptance_root/containerd-root" \
  --state "$acceptance_root/containerd-state" \
  >"$acceptance_root/containerd.log" 2>&1 &
containerd_pid=$!

for attempt in {1..30}; do
  if [[ -S "$containerd_socket" ]] \
    && sudo ctr --address "$containerd_socket" version >/dev/null 2>&1; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "containerd did not become ready" >&2
    exit 1
  fi
  sleep 1
done

CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2} \
  cargo test \
    -p daemon \
    --test real_cluster \
    --no-run
CARGO_BUILD_JOBS=${CARGO_BUILD_JOBS:-2} \
  cargo test \
    -p daemon \
    --test real_cluster \
    --no-run \
    --message-format=json \
    >"$acceptance_root/cargo-metadata.json"
test_binary=$(jq -r \
  'select(.reason == "compiler-artifact" and .target.name == "real_cluster" and .profile.test == true) | .executable // empty' \
  "$acceptance_root/cargo-metadata.json" \
  | tail -n 1)
if [[ -z "$test_binary" || ! -x "$test_binary" ]]; then
  echo "Cargo did not report an executable real-cluster test binary" >&2
  exit 1
fi

test_environment=(
  env
  PATH=/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
  "MAESTRO_CONTAINERD_SOCKET=$containerd_socket"
  "MAESTRO_ETCD_BIN=$(command -v etcd)"
  "RUST_BACKTRACE=${RUST_BACKTRACE:-1}"
)
if [[ "$(uname -m)" == "aarch64" || "$(uname -m)" == "arm64" ]]; then
  test_environment+=(ETCD_UNSUPPORTED_ARCH=arm64)
fi

sudo "${test_environment[@]}" \
  "$test_binary" \
  "$@" \
  --ignored \
  --nocapture \
  --test-threads=1
