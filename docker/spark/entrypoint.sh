#!/usr/bin/env bash
set -euo pipefail

ROLE="${SPARK_ROLE:-worker}"

case "${ROLE}" in
  master)
    exec /opt/spark/bin/spark-class \
      org.apache.spark.deploy.master.Master \
      --host "${SPARK_MASTER_HOST:-0.0.0.0}" \
      --port "${SPARK_MASTER_PORT:-7077}" \
      --webui-port "${SPARK_MASTER_WEBUI_PORT:-8080}"
    ;;
  worker)
    exec /opt/spark/bin/spark-class \
      org.apache.spark.deploy.worker.Worker \
      "${SPARK_MASTER_URL:-spark://spark-master:7077}" \
      --host "${SPARK_LOCAL_IP:-0.0.0.0}" \
      --webui-port "${SPARK_WORKER_WEBUI_PORT:-8081}"
    ;;
  *)
    echo "ERROR: unsupported SPARK_ROLE=${ROLE}. Expected master or worker." >&2
    exit 64
    ;;
esac
