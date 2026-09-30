#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://localhost:8080/v1/info}"
RETRIES="${2:-60}"

for i in $(seq 1 "$RETRIES"); do
  if curl -fsS "$URL" >/dev/null 2>&1; then
    echo "Trino is ready: $URL"
    exit 0
  fi
  sleep 2
done

echo "Trino did not become ready: $URL" >&2
exit 1
