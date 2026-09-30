# ADR-0001: Standardize the Spark runtime before Lakehouse migration

## Status
Accepted.

## Decision
The project baseline is Apache Spark 3.5.9 with Java 17 for local Docker and CI.

## Why
Iceberg maintains a dedicated Spark 3.5 integration line, while Spark 3.0 is end-of-life. Keeping Docker and CI on different Spark generations creates compatibility drift where the next phases add Iceberg, Trino interoperability and ML lifecycle tooling.

## Consequences
- The custom Spark image uses the official `apache/spark:3.5.9-java17-python3` base.
- Master and worker are started by a role-specific entrypoint.
- CI pins `pyspark==3.5.9`.
- HDFS remains temporarily supported during migration.
- Iceberg/MinIO/Polaris/Trino are introduced after this runtime baseline.

## Validation target
Every future Spark-related dependency must be validated against this single runtime contract.
