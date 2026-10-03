#!/usr/bin/env bash
set -euo pipefail

URL="${1:-http://localhost:8080/v1/info}"
RETRIES="${2:-60}"

for i in $(seq 1 "$RETRIES"); do
  if curl -fsS "$URL" >/dev/null 2>&1; then
    echo "Trino is ready: $URL"
    exit 0
  fi
  # Host port forwarding (e.g. WSL/Docker Desktop) can lag or break while the container is
  # healthy; the Makefile talks to Trino through `docker exec`, so accept that check too.
  if command -v docker >/dev/null 2>&1 \
     && docker exec trino curl -fsS http://localhost:8080/v1/info >/dev/null 2>&1; then
    echo "Trino is ready inside the container (host port $URL not reachable from this shell)"
    exit 0
  fi
  sleep 2
done

echo "Trino did not become ready: $URL" >&2
exit 1
