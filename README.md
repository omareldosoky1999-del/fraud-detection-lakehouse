# Real-Time Fraud Detection Data Platform

Kafka + Schema Registry + Spark 3.5.9 Structured Streaming + HDFS/Hive + HBase + Airflow + Prometheus/Grafana, with Spark MLlib ensemble scoring.

## Pipeline

```text
Synthetic Generator
  -> Kafka/Schema Registry
  -> Spark Structured Streaming
       -> Bronze (raw decoded events)
       -> DQ / Quarantine
       -> Silver (Decimal money + FX)
       -> Config-driven Rules + MLlib ensemble (core decision signal)
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

`processing/ml/train.py` trains Logistic Regression, Random Forest and GBT pipelines with a time-based validation split. The current artifact format is file-based; the MLOps phase will move tracking, versioning and promotion into MLflow Model Registry.


## MLOps / MLflow

The fraud models are managed as a first-class MLOps lifecycle:

```text
Spark MLlib training
  -> MLflow Tracking
  -> PostgreSQL metadata store
  -> RustFS S3-compatible artifact store
  -> Model Registry
  -> production alias
  -> Spark Structured Streaming inference
```

The registry contract is in `config/ml_models.yml`. Logistic Regression, Random Forest and GBT are registered as separate ensemble members. Streaming inference resolves their current `production` versions through MLflow aliases instead of hard-coded model files. MLflow's Tracking Server supports PostgreSQL as a backend store and remote object storage for artifacts; aliases are designed to decouple deployed inference code from a specific model version.

### Start the Lakehouse + MLOps stack

```bash
docker compose \\
  -f docker/docker-compose.yml \\
  -f docker/docker-compose.lakehouse.yml \\
  -f docker/docker-compose.mlflow.yml \\
  --profile lakehouse --profile mlops up -d --build
```

Open MLflow at `http://localhost:5000`.

### Train and register the fraud ensemble

```bash
docker exec spark-master \\
  /opt/spark/bin/spark-submit \\
  --master spark://spark-master:7077 \\
  /app/processing/ml/train.py \\
  --n 20000 \\
  --tracking-uri http://mlflow:5000 \\
  --promote-alias production
```

Training uses a time-based validation split and registers each validated Spark MLlib model under the configured registry name. The production streaming job requires all three registered ensemble members and loads them from the `production` alias. `--rules-only` exists only as an explicit test/legacy escape hatch.

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

The bundled deployment is single-host and academic: one Kafka broker, one HDFS datanode, one HBase node, SQLite Airflow metadata, plaintext internal networking, and an Airflow Docker-socket mount. Phase 1 standardizes Docker and CI on Spark 3.5.9 with Java 17 before the Iceberg/MinIO/Polaris/Trino migration.

## Start

```bash
cd docker
docker compose up -d --build
docker compose --profile serving --profile warehouse --profile orchestration --profile monitoring up -d --build
```
