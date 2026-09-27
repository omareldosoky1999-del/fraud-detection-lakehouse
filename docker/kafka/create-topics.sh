#!/usr/bin/env bash
# Run once after `docker compose --profile core up -d`:
#   docker exec kafka bash /kafka-scripts/create-topics.sh
# (or: bash docker/kafka/create-topics.sh localhost:29092 from the host)
# Inside the broker container use: docker exec kafka bash /kafka-scripts/create-topics.sh kafka:9092
set -euo pipefail

BOOTSTRAP="${1:-${KAFKA_BOOTSTRAP_INTERNAL:-localhost:29092}}"

create() {
  local topic="$1" partitions="$2"
  kafka-topics --bootstrap-server "$BOOTSTRAP" \
    --create --if-not-exists \
    --topic "$topic" --partitions "$partitions" --replication-factor 1
}

create transactions 3        # raw producer -> Spark source
create transactions.dlq 1    # producer-side: records that failed Avro serialization
create fraud.alerts 1        # Spark -> anything that wants to consume FLAG/BLOCK decisions live

kafka-topics --bootstrap-server "$BOOTSTRAP" --list
