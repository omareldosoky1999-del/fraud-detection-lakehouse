"""Iceberg table lifecycle and idempotent micro-batch writes."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


TABLES = {
    "bronze": "bronze.transactions",
    "quarantine": "quality.quarantine",
    "silver": "silver.transactions",
    "decisions": "gold.fraud_decisions",
}


def _qualified(catalog: str, logical_name: str) -> str:
    return f"{catalog}.{logical_name}"


def _ensure_namespace(spark: SparkSession, catalog: str, namespace: str) -> None:
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")


def _ensure_table(
    spark: SparkSession,
    table: str,
    frame: DataFrame,
    partition_columns: list[str],
    temp_view: str,
) -> None:
    try:
        spark.table(table).limit(0).collect()
        return
    except Exception:
        pass

    frame.createOrReplaceTempView(temp_view)
    partition_sql = ", ".join(partition_columns)
    spark.sql(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"USING iceberg PARTITIONED BY ({partition_sql}) "
        f"AS SELECT * FROM {temp_view} WHERE 1=0"
    )


def _overwrite_partitions(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    partition_columns: list[str],
    temp_view: str,
) -> None:
    # The micro-batch token is part of the partition key. A retry of an
    # uncommitted batch replaces exactly that batch's partitions.
    namespace = table.split(".")[-2]
    catalog = table.split(".")[0]
    _ensure_namespace(spark, catalog, namespace)
    _ensure_table(spark, table, frame, partition_columns, temp_view)

    frame.writeTo(table).overwritePartitions()


def write_micro_batch(
    spark: SparkSession,
    *,
    catalog: str,
    batch_token: str,
    bronze: DataFrame,
    quarantine: DataFrame | None,
    silver: DataFrame,
    decisions: DataFrame,
) -> None:
    """Persist one micro-batch to Iceberg.

    HDFS remains the migration-era compatibility sink. This function runs
    before the COMMITTED marker, so Iceberg failures remain retryable.
    """
    bronze_frame = (
        bronze
        .withColumn("ingest_date", F.to_date("kafka_timestamp"))
        .withColumn("batch_token", F.lit(batch_token))
    )
    _overwrite_partitions(
        spark,
        bronze_frame,
        _qualified(catalog, TABLES["bronze"]),
        ["batch_token", "ingest_date"],
        "__iceberg_bronze_schema",
    )

    if quarantine is not None and not quarantine.rdd.isEmpty():
        quarantine_frame = (
            quarantine
            .withColumn("quarantine_date", F.to_date("quarantined_at"))
            .withColumn("batch_token", F.lit(batch_token))
        )
        _overwrite_partitions(
            spark,
            quarantine_frame,
            _qualified(catalog, TABLES["quarantine"]),
            ["batch_token", "quarantine_date"],
            "__iceberg_quarantine_schema",
        )

    _overwrite_partitions(
        spark,
        silver,
        _qualified(catalog, TABLES["silver"]),
        ["batch_token", "event_date"],
        "__iceberg_silver_schema",
    )

    decisions_frame = decisions
    if "ml_probability" not in decisions_frame.columns:
        decisions_frame = decisions_frame.withColumn(
            "ml_probability", F.lit(None).cast("double")
        )

    _overwrite_partitions(
        spark,
        decisions_frame,
        _qualified(catalog, TABLES["decisions"]),
        ["batch_token", "event_date"],
        "__iceberg_decisions_schema",
    )
