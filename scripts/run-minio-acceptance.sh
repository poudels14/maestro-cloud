#!/usr/bin/env bash

set -euo pipefail

for command in cargo curl openssl sha256sum; do
  if ! command -v "$command" >/dev/null; then
    echo "required command is unavailable: $command" >&2
    exit 1
  fi
done

if [[ "$(uname -s)" != "Linux" ]]; then
  echo "MinIO acceptance requires Linux" >&2
  exit 1
fi

case "$(uname -m)" in
  x86_64)
    backend_arch=amd64
    kes_sha256=9f07258d121a69594125c6d2b569145c9b75ce80d20eaf27ab76863b689558ef
    minio_sha256=7c5bd8512c6e966455b1d198209358b2d191c77a83ab377c4073281065fb855f
    ;;
  aarch64 | arm64)
    backend_arch=arm64
    kes_sha256=3277419d221591043d7f99b0816d530f139a337cf09680f7cc7bf19f6126df0e
    minio_sha256=5c83cd2cf151717ba0243f73e1c7802ff36e272b67144bdd7f1f7d684fd6f03d
    ;;
  *)
    echo "unsupported MinIO acceptance architecture: $(uname -m)" >&2
    exit 1
    ;;
esac

backend_root=$(mktemp -d /tmp/maestro-minio-acceptance.XXXXXX)
kes_pid=
minio_pid=

redact_log() {
  sed -E \
    -e 's/(API[[:space:]]+Key[[:space:]]+).*/\1[redacted]/' \
    -e 's/(MINIO_KMS_KES_API_KEY=).*/\1[redacted]/'
}

cleanup() {
  status=$?
  trap - EXIT INT TERM

  if [[ "$status" -ne 0 ]]; then
    for log in kes.log minio.log; do
      if [[ -f "$backend_root/$log" ]]; then
        echo "===== $log =====" >&2
        tail -n 200 "$backend_root/$log" | redact_log >&2 || true
      fi
    done
  fi

  for pid in "$minio_pid" "$kes_pid"; do
    if [[ -n "$pid" ]]; then
      kill "$pid" >/dev/null 2>&1 || true
      wait "$pid" 2>/dev/null || true
    fi
  done

  case "$backend_root" in
    /tmp/maestro-minio-acceptance.*)
      rm -rf -- "$backend_root"
      ;;
    *)
      echo "refusing to remove unexpected acceptance root: $backend_root" >&2
      status=1
      ;;
  esac

  exit "$status"
}
trap cleanup EXIT INT TERM

mkdir -p "$backend_root/minio-data"

curl --fail --location --silent --show-error \
  --output "$backend_root/kes" \
  "https://github.com/minio/kes/releases/download/2025-03-12T09-35-18Z/kes-linux-$backend_arch"
echo \
  "$kes_sha256  $backend_root/kes" \
  | sha256sum --check
chmod +x "$backend_root/kes"
"$backend_root/kes" server --dev --addr 127.0.0.1:7373 \
  >"$backend_root/kes.log" 2>&1 &
kes_pid=$!

for attempt in {1..30}; do
  if grep --quiet "Server is up and running" "$backend_root/kes.log"; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "KES did not become ready" >&2
    exit 1
  fi
  sleep 1
done

kes_api_key=$(awk '$1 == "API" && $2 == "Key" { print $3 }' "$backend_root/kes.log")
if [[ -z "$kes_api_key" ]]; then
  echo "KES did not report a development API key" >&2
  exit 1
fi

MINIO_KES_SERVER=https://127.0.0.1:7373 \
  MINIO_KES_API_KEY="$kes_api_key" \
  "$backend_root/kes" key create --insecure maestro-backup
{
  # The development KES endpoint requires client authentication and closes
  # this certificate-only probe after sending its self-signed server chain.
  openssl s_client -connect 127.0.0.1:7373 -showcerts </dev/null 2>/dev/null || true
} | openssl x509 -out "$backend_root/kes.crt"

curl --fail --location --silent --show-error \
  --output "$backend_root/minio" \
  "https://dl.min.io/server/minio/release/linux-$backend_arch/archive/minio.RELEASE.2025-09-07T16-13-09Z"
echo \
  "$minio_sha256  $backend_root/minio" \
  | sha256sum --check
chmod +x "$backend_root/minio"
MINIO_ROOT_USER=maestro-test \
  MINIO_ROOT_PASSWORD=maestro-test-secret \
  MINIO_KMS_KES_ENDPOINT=https://127.0.0.1:7373 \
  MINIO_KMS_KES_API_KEY="$kes_api_key" \
  MINIO_KMS_KES_CAPATH="$backend_root/kes.crt" \
  MINIO_KMS_KES_KEY_NAME=maestro-backup \
  "$backend_root/minio" server "$backend_root/minio-data" \
    --address 127.0.0.1:9000 \
    --console-address 127.0.0.1:9001 \
    >"$backend_root/minio.log" 2>&1 &
minio_pid=$!

for attempt in {1..30}; do
  if curl --fail --silent http://127.0.0.1:9000/minio/health/ready >/dev/null; then
    break
  fi
  if [[ "$attempt" == 30 ]]; then
    echo "MinIO did not become ready" >&2
    exit 1
  fi
  sleep 1
done

MAESTRO_MINIO_ENDPOINT=http://127.0.0.1:9000 \
  MAESTRO_MINIO_ACCESS_KEY=maestro-test \
  MAESTRO_MINIO_SECRET_KEY=maestro-test-secret \
  MAESTRO_MINIO_KMS_KEY_ID=maestro-backup \
  cargo test \
    -p daemon \
    --test minio_s3_backup \
    -- \
    --ignored \
    --nocapture
