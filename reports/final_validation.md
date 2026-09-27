# Fraud Detection Platform — Final Validation Report

## Scope of this revision

The project was revised to address the major architecture and reliability issues found in the baseline review.

## Implemented fixes

1. Docker Compose profile dependency issue: foundational services are now always active; optional profiles depend on default-active services or share a profile.
2. Kafka/ZooKeeper/HBase persistence: named volumes were added.
3. False exactly-once claim: streaming documentation now distinguishes Spark source progress from `foreachBatch` side effects. Deterministic batch tokens + commit markers + dynamic partition overwrite were added for restart-safe batch outputs.
4. Cross-checkpoint duplication: Silver filters transaction IDs already committed in other batch tokens.
5. Bronze layer: raw decoded Kafka events are now persisted before DQ.
6. Configurable fraud rules: thresholds moved to `config/fraud_rules.yml`.
7. Financial precision: Silver monetary fields use Decimal types instead of floating-point money storage.
8. Late data handling: Gold rebuilds a rolling two-day lookback by default.
9. Alert reliability: alerts contain deterministic `alert_id`; a SQLite-backed deduplicating consumer was added.
10. DLQ recovery: a replay utility for manually corrected JSONL records was added.
11. Serving layer: a lightweight HBase client API was added (`/health`, `/v1/clients/{client_id}`).
12. MLlib path: optional Logistic Regression + Random Forest + GBT training/inference modules were added; rules-only mode remains the default unless a model directory is supplied.
13. Monitoring: Spark driver metrics, Prometheus scraping, Grafana provisioning, and a starter dashboard were added.
14. Image reproducibility: `:latest` tags were removed from monitoring/HBase images in favor of pinned tags.
15. CI/static validation: compile/config checks and `scripts/validate_stack.py` were added.

## Checks executed in this environment

| Check | Result |
|---|---|
| `python -m compileall -q .` | PASS |
| Compose YAML parse | PASS |
| Compose profile dependency validation | PASS |
| No `:latest` image tags | PASS |
| Persistent volume checks | PASS |
| Fraud-rules YAML parse | PASS |
| Prometheus/Grafana YAML parse | PASS |
| Hive DDL presence/partition checks | PASS |
| `scripts/validate_stack.py` | PASS |
| `pytest -q tests/test_compose_config.py` | PASS (1 test) |

## Tests blocked by the review environment

The full pytest suite could not be executed because this environment does not contain `fastavro` or PySpark, and package installation from the network is unavailable. The earlier full-suite collection stopped on missing `fastavro`; Spark-backed tests were skipped because PySpark was absent.

The environment also has no Docker/Compose binary, so the Kafka -> Spark -> HDFS/Hive -> HBase -> Airflow end-to-end deployment could not be launched here.

Therefore this revision is **static/config validated, not end-to-end runtime validated in this environment**. The project intentionally does not claim that the full distributed stack was executed here.

## Recommended runtime verification command set on a Docker-enabled machine

```bash
cd docker
docker compose up -d --build
docker compose --profile serving --profile warehouse --profile orchestration --profile monitoring up -d --build
bash kafka/create-topics.sh localhost:29092

docker exec hive beeline -u jdbc:hive2://localhost:10000 -f /app/warehouse/hive/ddl.sql

# start stream
# run producer
# verify Bronze/Silver/decisions/HBase/alerts
# trigger Airflow Gold DAG
# open Prometheus/Grafana/serving API
```

After dependencies are installed locally:

```bash
pip install -r requirements-dev.txt
pip install pyspark==3.5.1
pytest -q
```
