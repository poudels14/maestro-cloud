#!/usr/bin/env bash

set -euo pipefail

for command in bash cargo containerd ctr curl dig env etcd ip jq modprobe mount nft nsenter ping runc sudo sysctl unshare; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "real cluster acceptance requires Linux" >&2
  exit 1
fi

containerd_binary=$(command -v containerd)
ctr_binary=$(command -v ctr)
curl_binary=$(command -v curl)
dig_binary=$(command -v dig)
env_binary=$(command -v env)
etcd_binary=$(command -v etcd)
ip_binary=$(command -v ip)
mount_binary=$(command -v mount)
modprobe_binary=$(command -v modprobe)
nft_binary=$(command -v nft)
nsenter_binary=$(command -v nsenter)
ping_binary=$(command -v ping)
runc_binary=$(command -v runc)
sysctl_binary=$(command -v sysctl)
unshare_binary=$(command -v unshare)
containerd_directory=$(dirname "$containerd_binary")
curl_directory=$(dirname "$curl_binary")
dig_directory=$(dirname "$dig_binary")
ip_directory=$(dirname "$ip_binary")
mount_directory=$(dirname "$mount_binary")
nft_directory=$(dirname "$nft_binary")
nsenter_directory=$(dirname "$nsenter_binary")
ping_directory=$(dirname "$ping_binary")
runc_directory=$(dirname "$runc_binary")
sysctl_directory=$(dirname "$sysctl_binary")
unshare_directory=$(dirname "$unshare_binary")
runtime_path="${containerd_directory}:${curl_directory}:${dig_directory}:${ip_directory}:${mount_directory}:${nft_directory}:${nsenter_directory}:${ping_directory}:${runc_directory}:${sysctl_directory}:${unshare_directory}:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin"

acceptance_root=$(mktemp -d /tmp/maestro-real-cluster.XXXXXX)
containerd_socket="$acceptance_root/containerd.sock"
containerd_pid=
wireguard_probe="mrw${$}"
wireguard_probe=${wireguard_probe:0:15}

cleanup() {
  status=$?
  trap - EXIT INT TERM

  sudo "$ip_binary" link delete "$wireguard_probe" >/dev/null 2>&1 || true
  if [[ -S "$containerd_socket" ]] \
    && sudo "$ctr_binary" --address "$containerd_socket" version >/dev/null 2>&1; then
    while IFS= read -r namespace; do
      if [[ -z "$namespace" ]]; then
        continue
      fi
      while IFS= read -r task_id; do
        if [[ -n "$task_id" ]]; then
          sudo "$ctr_binary" --address "$containerd_socket" --namespace "$namespace" \
            tasks delete --force "$task_id" >/dev/null 2>&1 || true
        fi
      done < <(
        sudo "$ctr_binary" --address "$containerd_socket" --namespace "$namespace" \
          tasks list --quiet 2>/dev/null || true
      )
      while IFS= read -r container_id; do
        if [[ -n "$container_id" ]]; then
          sudo "$ctr_binary" --address "$containerd_socket" --namespace "$namespace" \
            containers delete "$container_id" >/dev/null 2>&1 || true
        fi
      done < <(
        sudo "$ctr_binary" --address "$containerd_socket" --namespace "$namespace" \
          containers list --quiet 2>/dev/null || true
      )
    done < <(
      sudo "$ctr_binary" --address "$containerd_socket" namespaces list --quiet 2>/dev/null || true
    )
  fi
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

sudo "$modprobe_binary" wireguard
sudo "$ip_binary" link add "$wireguard_probe" type wireguard
sudo "$ip_binary" link delete "$wireguard_probe"

# The caller owns the isolated log directory used by this redirection.
# shellcheck disable=SC2024
sudo "$env_binary" PATH="$runtime_path" "$containerd_binary" \
  --log-level warn \
  --address "$containerd_socket" \
  --root "$acceptance_root/containerd-root" \
  --state "$acceptance_root/containerd-state" \
  >"$acceptance_root/containerd.log" 2>&1 &
containerd_pid=$!

for attempt in {1..30}; do
  if [[ -S "$containerd_socket" ]] \
    && sudo "$ctr_binary" --address "$containerd_socket" version >/dev/null 2>&1; then
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
  "$env_binary"
  "PATH=$runtime_path"
  "MAESTRO_CONTAINERD_SOCKET=$containerd_socket"
  "MAESTRO_ETCD_BIN=$etcd_binary"
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
