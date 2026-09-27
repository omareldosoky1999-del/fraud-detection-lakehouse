# Real-Time Fraud Detection Data Platform

Kafka + Schema Registry + Spark Structured Streaming + HDFS/Hive + HBase + Airflow + Prometheus/Grafana, with optional Spark MLlib ensemble scoring.

## Pipeline

```text
Synthetic Generator
  -> Kafka/Schema Registry
  -> Spark Structured Streaming
       -> Bronze (raw decoded events)
       -> DQ / Quarantine
       -> Silver (Decimal money + FX)
       -> Config-driven Rules + optional MLlib ensemble
       -> Fraud Decisions
            -> HBase current-risk API
            -> Kafka fraud.alerts (deterministic alert_id)
  -> Airflow -> rolling 2-day Gold rebuild

Prometheus/Grafana <- Kafka Exporter + Spark driver metrics
```

## Restart and idempotency

Spark checkpoints provide source progress. Each micro-batch also receives a deterministic token derived from Kafka partition/offset ranges. Bronze/Silver/quarantine/decision outputs are written with `batch_token` partitions and dynamic overwrite, and a `COMMITTED` marker is written after the sinks complete. A retry of an uncommitted batch therefore replaces only its own output. Transaction IDs are also filtered against previously committed Silver data, protecting against manual checkpoint resets.

Kafka alert publication remains at-least-once. `alert_id = batch_token:transaction_id` lets the included SQLite-backed alert consumer de-duplicate repeated deliveries.

## Rules

Business thresholds are in `config/fraud_rules.yml`, not hard-coded in the rules module.

## MLlib

`processing/ml/train.py` trains Logistic Regression, Random Forest and GBT pipelines. Pass `--ml-model-dir models/fraud_ensemble` to the streaming job to enable ensemble scoring; without a model directory, the rules-only path remains the default.

## Serving

The optional `serving-api` exposes:

- `GET /health`
- `GET /v1/clients/{client_id}`

The endpoint reads the HBase `fraud:client_risk` serving table.

## DLQ replay

`processing/dlq/replay.py` replays manually corrected JSONL records to the Avro transactions topic. Automatic repair is intentionally not performed because changing financial records without an explicit correction policy would be unsafe.

## Monitoring

Prometheus scrapes Kafka Exporter and the Spark driver metrics endpoint. Grafana is provisioned with a Fraud Platform dashboard. Airflow/HBase/Hive remain log/health monitored until dedicated exporters are introduced.

## Known environment limitations

The bundled deployment is single-host and academic: one Kafka broker, one HDFS datanode, one HBase node, SQLite Airflow metadata, plaintext internal networking, and an Airflow Docker-socket mount. The Docker Spark cluster remains on the existing Spark 3.0.0 image; local CI can use a newer PySpark for API-compatible tests, but a containerized integration test on the exact image should be run before release.

## Start

```bash
cd docker
docker compose up -d --build
docker compose --profile serving --profile warehouse --profile orchestration --profile monitoring up -d --build
```
