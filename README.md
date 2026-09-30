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
       -> Point-in-time Feature Layer
       -> Config-driven Rules + MLlib ensemble (core decision signal)
       -> Fraud Decisions
            -> HBase current-risk API
            -> Kafka fraud.alerts (deterministic alert_id)
  -> Airflow -> rolling 2-day Gold rebuild

Prometheus/Grafana <- Kafka Exporter + Spark driver metrics
```

## Restart and idempotency

Spark checkpoints provide source progress. Each micro-batch also receives a deterministic token derived from Kafka partition/offset ranges. Legacy HDFS Bronze/Silver/quarantine/decision outputs use `batch_token` partitions, while Iceberg uses time-based partitions and row-level `batch_token` replacement. A `COMMITTED` marker is written after the sinks complete, so an uncommitted retry can safely replace its own logical batch without making every micro-batch a physical Iceberg partition. Transaction IDs are also filtered against previously committed Silver data, protecting against manual checkpoint resets.

Kafka alert publication remains at-least-once. `alert_id = batch_token:transaction_id` lets the included SQLite-backed alert consumer de-duplicate repeated deliveries.

## Rules

Business thresholds are in `config/fraud_rules.yml`, not hard-coded in the rules module.

## MLlib

`processing/ml/train.py` trains Logistic Regression, Random Forest and GBT pipelines with a time-based validation split and class weighting, evaluates ROC-AUC/AUPRC/precision/recall/F1, tracks the run in MLflow, and registers all three models. Training defaults to the `candidate` alias; production promotion is a separate controlled operation.


## Feature engineering

The shared feature builder lives in `processing/features/fraud_features.py` and is used by both ML training and streaming inference. Features are causal: each row uses only the current transaction and data that occurred before it. The current feature contract includes 5-minute transaction velocity, 1-hour amount sum, prior-30-transaction amount statistics, time since previous transaction, 30-day new-device detection, country changes and amount-to-prior-average ratio.

Streaming feature batches are persisted in the Iceberg table `features.transaction_features`, making model inputs auditable and queryable through Trino.


## Data Quality / Great Expectations

The streaming path still performs immediate schema/data-quality checks and quarantines invalid events. Great Expectations adds an independent batch-level contract on the persisted Silver partition before the daily Gold build. This keeps the low-latency fraud decision path lightweight while giving downstream analytics a declarative validation gate.

The Silver contract currently checks required identifiers/timestamps/categorical fields and prevents negative USD amounts. It is implemented in `processing/quality/gx_validation.py`, executed by `scripts/validate_silver_quality.py`, and enforced by the `fraud_gold_daily` Airflow DAG.

Great Expectations is integrated directly with the existing Spark DataFrame validation boundary, so the financial dataset does not need to be converted to pandas.
## MLOps / MLflow

The fraud models are managed as a first-class MLOps lifecycle with candidate-first promotion:

```text
Spark MLlib training
  -> MLflow Tracking
  -> PostgreSQL metadata store
  -> RustFS S3-compatible artifact store
  -> Model Registry
  -> candidate alias
  -> controlled production promotion
  -> production alias
  -> Spark Structured Streaming inference
```

The registry contract is in `config/ml_models.yml`. Logistic Regression, Random Forest and GBT are registered as separate ensemble members. Training promotes the validated ensemble to the `candidate` alias by default; production is not changed automatically. `scripts/promote_ensemble.py` verifies that all candidate members belong to the same training run and then performs the controlled candidate-to-production promotion. Streaming inference resolves only the current `production` alias instead of hard-coded model files. MLflow's Tracking Server supports PostgreSQL as a backend store and remote object storage for artifacts; aliases decouple deployed inference code from a specific model version.

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

## Local deployment profile

The bundled Docker environment is intentionally single-host and development-oriented: one Kafka broker, one HDFS datanode, one HBase node, SQLite Airflow metadata, plaintext internal networking, and an Airflow Docker-socket mount. The cloud deployment boundary is separated into Terraform + Helm so the core Spark processing code remains unchanged.

## Start

```bash
cd docker
docker compose up -d --build
docker compose --profile serving --profile warehouse --profile orchestration --profile monitoring up -d --build
```


### Cloud image lifecycle

The Spark application image is published to GHCR from `main` with both `latest` and an immutable `sha-<commit>` tag. Cloud deployments should use the immutable SHA tag through Helm (`--set image.immutableTag=sha-<commit>`) rather than relying on `latest`.

## Cloud deployment contract

The platform keeps the fraud-processing logic cloud-agnostic. `config/storage_profiles.yml` defines the storage contract for Local/RustFS, AWS/S3, Azure/ADLS Gen2, and GCP/GCS, while Terraform owns cloud-specific infrastructure.

For Kubernetes deployment, the runtime target is EKS, AKS, or GKE with Spark on Kubernetes. The Spark Operator API used by the deployment manifests is `sparkoperator.k8s.io/v1beta2`.

See `docs/cloud-migration.md` and `infrastructure/terraform/README.md` for the migration boundary and infrastructure layout.


### Promote a validated ensemble to production

Training writes a candidate release first. Production promotion is an explicit control-plane action:

```bash
docker exec spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /app/scripts/promote_ensemble.py \
  --tracking-uri http://mlflow:5000 \
  --expected-run-id <MLFLOW_RUN_ID>
```

The promotion command refuses mixed candidate releases and refuses models without `validation_status=PASSED`.
