# ADR-0008: Periodic Iceberg table maintenance

## Status

Accepted.

## Decision

Iceberg maintenance runs outside the real-time fraud streaming job.

The weekly maintenance task:

1. rewrites data files toward a target file size
2. expires old snapshots
3. runs as an Airflow batch task with retries
4. never executes inside a streaming micro-batch

Default policy:

- snapshot retention: 168 hours (7 days)
- retain at least 20 snapshots
- target data-file size: 512 MiB

## Why

The fraud stream must prioritize predictable latency and correctness. File compaction and snapshot cleanup are metadata/data-layout operations and should not compete with the streaming workload.

Time-based Iceberg partitioning plus periodic compaction keeps small-file growth under control while preserving the logical batch idempotency contract.

## Failure semantics

Maintenance failure does not stop the streaming application. Airflow retries the maintenance task independently.

Snapshot expiration is bounded by the retention window and retain_last=20 so recent snapshots remain available for operational rollback and investigation.

## Scope

Current tables:

- bronze.transactions
- quality.quarantine
- silver.transactions
- features.transaction_features
- gold.fraud_decisions

Gold aggregate tables can be added to the maintenance set after their write frequency and file-size characteristics are measured.

## Cloud path

The same Spark maintenance job can run against Polaris-backed Iceberg tables on AWS S3, Azure ADLS Gen2 or GCP GCS. Only the catalog/storage configuration changes.
