#!/usr/bin/env bash

set -euo pipefail

for command in buildctl buildkitd cargo containerd ctr sudo; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "containerd acceptance requires Linux" >&2
  exit 1
fi

case "$(uname -m)" in
  x86_64)
    platform=linux/amd64
    ;;
  aarch64 | arm64)
    platform=linux/arm64
    ;;
  *)
    echo "unsupported containerd acceptance architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

acceptance_root=$(mktemp -d /tmp/maestro-containerd-acceptance.XXXXXX)
containerd_socket="$acceptance_root/containerd.sock"
buildkit_socket="$acceptance_root/buildkitd.sock"
containerd_pid=
buildkit_pid=

cleanup() {
  status=$?
  trap - EXIT INT TERM

  if [[ "$status" -ne 0 ]]; then
    for log in containerd.log containerd-probe.log image-pull.log buildkitd.log buildkit-probe.log; do
      if [[ -f "$acceptance_root/$log" ]]; then
        echo "===== $log =====" >&2
        tail -n 200 "$acceptance_root/$log" >&2 || true
      fi
    done
  fi

  if [[ -S "$containerd_socket" ]] \
    && sudo ctr --address "$containerd_socket" version >/dev/null 2>&1; then
    while IFS= read -r task_id; do
      if [[ -n "$task_id" ]]; then
        sudo ctr --address "$containerd_socket" --namespace maestro-test \
          tasks delete --force "$task_id" >/dev/null 2>&1 || true
      fi
    done < <(
      sudo ctr --address "$containerd_socket" --namespace maestro-test \
        tasks list --quiet 2>/dev/null || true
    )
    while IFS= read -r container_id; do
      if [[ -n "$container_id" ]]; then
        sudo ctr --address "$containerd_socket" --namespace maestro-test \
          containers delete "$container_id" >/dev/null 2>&1 || true
      fi
    done < <(
      sudo ctr --address "$containerd_socket" --namespace maestro-test \
        containers list --quiet 2>/dev/null || true
    )
  fi

  for pid in "$buildkit_pid" "$containerd_pid"; do
    if [[ -n "$pid" ]]; then
      sudo kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  case "$acceptance_root" in
    /tmp/maestro-containerd-acceptance.*)
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

# The caller owns the isolated log directory used by this redirection.
# shellcheck disable=SC2024
sudo containerd \
  --log-level warn \
  --address "$containerd_socket" \
  --root "$acceptance_root/containerd-root" \
  --state "$acceptance_root/containerd-state" \
  >"$acceptance_root/containerd.log" 2>&1 &
containerd_pid=$!

# The caller owns the isolated log directory used by this redirection.
# shellcheck disable=SC2024
sudo buildkitd \
  --root "$acceptance_root/buildkit-root" \
  --addr "unix://$buildkit_socket" \
  --containerd-worker=false \
  --oci-worker=true \
  --oci-worker-net=host \
  >"$acceptance_root/buildkitd.log" 2>&1 &
buildkit_pid=$!

for attempt in {1..30}; do
  if [[ -S "$containerd_socket" && -S "$buildkit_socket" ]]; then
    sudo chmod 0666 "$containerd_socket" "$buildkit_socket"
    if ctr --address "$containerd_socket" version \
      >"$acceptance_root/containerd-probe.log" 2>&1 \
      && buildctl --addr "unix://$buildkit_socket" debug workers \
        >"$acceptance_root/buildkit-probe.log" 2>&1; then
      break
    fi
  fi

  if [[ "$attempt" == 30 ]]; then
    echo "containerd or BuildKit did not become ready" >&2
    exit 1
  fi
  sleep 1
done

image=mirror.gcr.io/library/busybox:1.36.1
if ! ctr \
  --address "$containerd_socket" \
  --namespace maestro-test \
  images pull \
  --platform "$platform" \
  "$image" \
  >"$acceptance_root/image-pull.log" 2>&1; then
  echo "failed to pull the containerd conformance image" >&2
  exit 1
fi

MAESTRO_CONTAINERD_SOCKET="$containerd_socket" \
  MAESTRO_CONTAINERD_NAMESPACE=maestro-test \
  MAESTRO_CONTAINERD_SNAPSHOTTER=overlayfs \
  MAESTRO_CONTAINERD_TEST_IMAGE="$image" \
  cargo test \
    -p runtime \
    --no-default-features \
    --features containerd,test-util \
    --test containerd_conformance \
    -- \
    --ignored \
    --nocapture

MAESTRO_CONTAINERD_SOCKET="$containerd_socket" \
  MAESTRO_CONTAINERD_SNAPSHOTTER=overlayfs \
  MAESTRO_CONTAINERD_ARTIFACT_TEST_IMAGE="$image" \
  MAESTRO_BUILDCTL="$(command -v buildctl)" \
  MAESTRO_BUILDKIT_ADDRESS="unix://$buildkit_socket" \
  cargo test \
    -p runtime \
    --no-default-features \
    --features containerd \
    --test containerd_artifact \
    -- \
    --ignored \
    --nocapture \
    --test-threads=1
