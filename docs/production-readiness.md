# Production Readiness

## Platform flow

Kafka + Schema Registry -> Spark Structured Streaming -> Silver/DQ -> Point-in-time Features -> Rules + MLlib -> Iceberg -> Trino / HBase / Alerts

Control planes:

- Airflow: scheduled Gold builds and Iceberg maintenance
- MLflow: model tracking, registry, candidate and production aliases
- OpenLineage + Marquez: data lineage
- Terraform: cloud infrastructure
- Helm + Spark Operator: Kubernetes workload deployment
- Prometheus + Grafana + Alertmanager: operational monitoring
- CodeQL + CI: source and configuration validation

## Data correctness controls

1. Schema Registry enforces backward compatibility.
2. Spark performs immediate record validation and quarantine.
3. Great Expectations validates persisted Silver before Gold.
4. Deterministic batch tokens support retry-safe micro-batch replacement.
5. Iceberg partitions by event/ingestion date, not operational batch IDs.
6. HBase is explicitly a serving cache; Iceberg remains the analytical system of record.

## ML controls

Training uses a time-based validation split and class weighting. Validation records ROC-AUC, AUPRC, precision, recall and F1 at the configured decision threshold.

Every ensemble member carries validation metadata and an ensemble training-run ID in MLflow.

New models are promoted to the candidate alias first. Production promotion requires the explicit GitHub `production` environment workflow and a matching expected MLflow run ID. Streaming inference rejects mixed ensemble releases.

## Security boundary

Local development uses non-production credentials only. Cloud deployments inject runtime secrets through Kubernetes Secret / External Secrets mechanisms and use cloud-native workload identity wherever possible.

Long-lived cloud access keys are not stored in the repository.

## Availability and recovery

- Kafka producer checkpoint advances only after broker acknowledgements.
- Spark checkpoints provide source progress.
- External batch commit markers provide idempotent retry state.
- Alert consumer commits Kafka offsets only after SQLite persistence.
- Alertmanager and serving alert storage are persisted locally.
- Iceberg maintenance is scheduled outside the latency-sensitive fraud stream.

## Cloud boundary

Business logic remains unchanged across environments.

| Layer | Local | AWS | Azure | GCP |
| --- | --- | --- | --- | --- |
| Object storage | RustFS | S3 | ADLS Gen2 | GCS |
| Iceberg FileIO | S3FileIO | S3FileIO | ADLSFileIO | GCSFileIO |
| Catalog | Polaris | Polaris | Polaris | Polaris |
| Compute | Docker Spark | EKS | AKS | GKE |
| Workload identity | local secret | EKS Pod Identity | AKS Workload Identity | GKE Workload Identity Federation |

## Known limitations

- The local stack intentionally uses development credentials and plaintext Kafka/HTTP endpoints. Production environments must add TLS, authentication and network policy.
- HDFS/Hive remain during the migration window for compatibility. New analytical development should target Iceberg + Trino.
- The current E2E workflows create real local infrastructure and are therefore slower than unit tests.
- Multi-table streaming writes are not a single distributed transaction. Deterministic batch idempotency makes partial writes retryable.
- HBase provides current-state serving, not historical audit storage.
- Cloud Terraform modules are validated in CI but are not auto-applied.

## Release checklist

- CI green
- CodeQL green
- Container build green
- Schema Registry E2E green
- Quality E2E green
- MLOps E2E green
- Lakehouse/Trino E2E green
- Serving/Monitoring validation green
- Production model promotion performed only through the approved workflow
- Immutable Spark image tag recorded for the release
