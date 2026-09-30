# ADR-0003: OpenLineage + Marquez as the lineage control plane

## Status
Accepted.

## Decision

Use OpenLineage as the vendor-neutral lineage event standard and Marquez as the local reference backend/UI.

The local flow is:

```text
Spark jobs ────────────────┐
                           ├──> OpenLineage HTTP API ──> Marquez
Airflow DAGs ──────────────┘
```

Spark uses the OpenLineage Spark listener and HTTP transport. Airflow uses the official OpenLineage provider. The local backend is Marquez 0.51.1.

## Runtime contract

- OpenLineage Spark: 1.53.0
- Airflow: 2.8.1
- Airflow OpenLineage provider: 1.14.0
- Marquez: 0.51.1
- Marquez API: internal port 5000
- Local Marquez UI: host port 3001

The Airflow provider version is intentionally pinned to the latest compatible provider line for Airflow 2.8.x rather than using a newer provider that requires a newer Airflow release.

## Why

The project has multiple producers and consumers: Kafka, Spark, Iceberg, HDFS, Trino, Airflow, MLflow and serving sinks. Lineage must therefore live outside any single processing framework. OpenLineage provides the event contract, while Marquez gives the local platform a concrete storage and visualization target.

## Cloud path

The application keeps the OpenLineage event contract unchanged when moving to cloud. Only transport/backend configuration changes: the same Spark/Airflow jobs can emit to a managed or self-hosted OpenLineage-compatible backend.

## Validation

`.github/workflows/lineage-e2e.yml` starts the local stack, executes a real Spark workload, and verifies that the workload is visible through the Marquez API.
