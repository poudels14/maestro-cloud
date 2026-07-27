#!/usr/bin/env bash

set -euo pipefail

for command in cargo docker; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "Docker acceptance requires Linux" >&2
  exit 1
fi

docker info >/dev/null

MAESTRO_DOCKER_TEST_IMAGE="${MAESTRO_DOCKER_TEST_IMAGE:-mirror.gcr.io/library/busybox:1.36.1}" \
  cargo test \
    -p runtime \
    --no-default-features \
    --features docker,test-util \
    --test docker_conformance \
    -- \
    --ignored \
    --nocapture

cargo test \
  -p runtime \
  --no-default-features \
  --features docker \
  --test docker_artifact \
  -- \
  --ignored \
  --nocapture
