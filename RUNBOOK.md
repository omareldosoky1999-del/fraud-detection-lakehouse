# Runbook

How to run, verify and troubleshoot the platform. The Docker sequence below is the same one
`.github/workflows/full-stack-e2e.yml` uses; `make` wraps it.

## 0. Prerequisites
Docker (>= 16 GB RAM allotted; the full stack is ~30 services), Python 3.11, Java 17+ for local Spark,
`pip install -r requirements-dev.txt`. For the host-side producer: `pip install 'confluent-kafka[avro,schemaregistry]>=2.3' fastavro PyYAML`.

## 1. Fast confidence check (no Docker)
```bash
make test         # unit tests
make local-e2e    # train -> promote -> load registered ensemble -> run process_batch -> assert
```
`local-e2e` validates the decision path (features, rules, ML ensemble, Silver/decisions writes) using a
SQLite MLflow backend. It does **not** cover Kafka, Schema Registry, Iceberg/Polaris, HBase or Airflow.

## 2. Full stack (Docker)
```bash
make e2e          # = up init train promote stream produce verify
```
or step by step:

| Step | Command | What it does |
|---|---|---|
| 1 | `make up` | Build and start base + lakehouse + mlops + serving profiles |
| 2 | `make init` | HDFS dirs, Kafka topics, waits for HBase and serving API |
| 3 | `make train` | Trains the 3-model ensemble, registers it in MLflow with alias `candidate` |
| 4 | `make promote` | Moves the trained run from `candidate` to `production` (the streaming job loads `production`) |
| 5 | `make stream` | Starts the Spark Structured Streaming job (`bronze_silver_job.py`) in `spark-master` |
| 6 | `make produce` | Sends labelled Avro transactions to Kafka (`N_EVENTS=1500` by default) |
| 7 | `make verify` | Polls Trino until `polaris.gold.fraud_decisions` has rows |

Order matters: **train and promote before starting the stream**. The job requires a production
ensemble (`ml_required` defaults to true) and exits if none exists.

Useful endpoints: Spark UI `:8083`, MLflow `:5000`, Trino `:8080`, Serving API `:8095/health`
(`/v1/clients/<id>`), Airflow `:8088`, Grafana `:3000`, Prometheus `:9091`, Schema Registry `:8081`, Polaris `:8181`.

## 3. Daily Gold build
Airflow DAG `fraud_gold_daily` (02:00): validates Silver quality, then builds Gold tables. Maintenance and
retraining DAGs are separate. All three currently shell out with `docker exec spark-master`, so they work on
the Docker stack only (see Known limitations).

## 4. Troubleshooting
| Symptom | Check |
|---|---|
| `Failed to find data source: kafka` / `from_avro` not found | Spark image must contain the Kafka/Avro jars (`docker/spark*/Dockerfile`); rebuild with `make up` |
| Stream exits immediately with "ML ensemble required" | Run `make train promote` first |
| `make verify` times out | `make stream-log`; confirm topic `transactions` exists and the producer ran |
| `UNRESOLVED_COLUMN ... label` in training | Fixed in `build_labeled_features`; make sure you are on the current code |
| Stream restarts but state errors after upgrading | The dedup operator changed; clear `STREAMING_CHECKPOINT` once |
| `FileNotFoundError: ingestion/data/cards.avro` / `config/ml_models.yml` | Fixed: paths are anchored to the repo root; also `make` runs `docker exec -w /app` |
| Executors die, `Connection reset`, executor ids keep growing | The worker used all host cores on a 2 GB executor. Use `SPARK_RES` (defaults bound cores/memory) or set `SPARK_WORKER_CORES` / `SPARK_WORKER_MEMORY` |
| `Not enough fraud examples ... validation_positives` | Training set too small; fraud campaigns are clustered in time. Use `N_TRAIN=5000` or more |
| `AccessControlException: user=spark, access=WRITE, inode="/"` during `train` | HDFS `/user/spark` is missing (`make init` did not run/failed). `make train` now creates it; if the script itself fails run `sed -i 's/\r$//' scripts/*.sh` (CRLF from Windows) |
| datanode restarting forever, log says `Incompatible clusterIDs` | The namenode was re-formatted (new clusterID) while the datanode kept the old one. Reset the datanode: `docker rm -f datanode && make up`. The compose now formats the namenode only once (it used to check a path that was never populated) |
| Spark tests hang on a small machine | `export SPARK_LOCAL_IP=127.0.0.1` and lower `spark.sql.shuffle.partitions` |

## 5. Known limitations (read before calling this production)
- Models are trained on synthetic data from `ingestion/generator`, whose fraud patterns match the rules.
  Metrics validate plumbing, not real-world detection quality. There are no real labels or analyst feedback loop.
- Cloud (Helm/Terraform) deploys only the streaming SparkApplication. Kafka, Schema Registry, Polaris, MLflow,
  Trino, Airflow and HBase must be provided separately. Azure/GCP checkpoints need extra Hadoop connector jars;
  AWS S3A with EKS Pod Identity needs a newer AWS SDK or an IRSA credentials provider.
- The serving API uses `http.server` without auth/TLS and is for local use only.
- Avro schema uses `int` for amounts and account numbers; real card/account identifiers will overflow.
