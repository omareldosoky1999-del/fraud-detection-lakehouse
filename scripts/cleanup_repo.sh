#!/usr/bin/env bash
# One-time repo cleanup. Safe to re-run. Nothing is deleted from disk or history:
#  1) runtime artifacts are un-tracked (they stay on your disk, .gitignore keeps them out)
#  2) code that is not part of the MVP is MOVED to archive/ (git mv -> history preserved)
set -euo pipefail
cd "$(git rev-parse --show-toplevel)"

echo ">> un-tracking runtime artifacts"
for p in checkpoints output airflow-db docker/airflow-db ingestion/data/checkpoint.json .vscode; do
  git rm -r --cached --ignore-unmatch -q "$p"
done

echo ">> archiving non-MVP code"
move() {
  if [ -e "$1" ]; then
    mkdir -p "archive/$(dirname "$1")"
    git mv "$1" "archive/$1"
    echo "   $1 -> archive/$1"
  fi
}
for f in \
  processing/streaming/finguard_master_pipeline.py \
  processing/streaming/spark_streaming.py \
  processing/streaming/kafka_to_bronze.py \
  processing/streaming/write_bronze.py \
  processing/streaming/write_silver.py \
  processing/batch/write_gold.py \
  orchestration/dags/fraud_pipeline_dag.py \
  fraud_detection \
  data_quality \
  ml \
  k8s \
  etc \
  config \
  docker/marquez \
  docker/trino \
  docker/minio \
  ingestion/data/Kafka_producer.py
do
  move "$f"
done

cat > archive/README.md <<'EOT'
# archive/

Earlier experiments that are NOT part of the delivered pipeline (MinIO/Iceberg medallion
prototype, FinGuard master pipeline, k8s manifest, ML scripts, old DAG...). Kept for
reference and as ideas for future work; they are not maintained and were never wired into
the working flow. See the top-level README for what actually runs.
EOT
git add archive/README.md

echo ">> done. Review with: git status"
