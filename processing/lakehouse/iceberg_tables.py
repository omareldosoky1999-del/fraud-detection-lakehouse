"""Iceberg table lifecycle and idempotent micro-batch writes."""
from __future__ import annotations

from pyspark.sql import DataFrame, SparkSession, functions as F


TABLES = {
    "bronze": "bronze.transactions",
    "quarantine": "quality.quarantine",
    "silver": "silver.transactions",
    "features": "features.transaction_features",
    "decisions": "gold.fraud_decisions",
    "customer_daily_risk": "gold.customer_daily_risk",
    "daily_kpis": "gold.daily_kpis",
}

PARTITIONS = {
    "bronze": ["ingest_date"],
    "quarantine": ["quarantine_date"],
    "silver": ["event_date"],
    "features": ["event_date"],
    "decisions": ["event_date"],
    "customer_daily_risk": ["event_date"],
    "daily_kpis": ["event_date"],
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

    # In foreachBatch the batch DataFrame belongs to a *cloned* session, so the temp
    # view must be registered and queried on that same session (not the outer one).
    session = frame.sparkSession
    frame.createOrReplaceTempView(temp_view)
    partition_sql = ", ".join(partition_columns)
    session.sql(
        f"CREATE TABLE IF NOT EXISTS {table} "
        f"USING iceberg PARTITIONED BY ({partition_sql}) "
        f"AS SELECT * FROM {temp_view} WHERE 1=0"
    )


_PARTIAL_FILE_DELETE = "some, but not all, rows match filter"


def _delete_batch_rows(spark: SparkSession, table: str, escaped_token: str, partition_predicate: str) -> None:
    """Remove the rows of one batch (idempotent retry) from ``table``.

    A plain DELETE can be planned by Iceberg as a metadata-only delete, which it
    refuses when it cannot prove that *every* row of a data file matches the filter
    (ValidationException: "Cannot delete file where some, but not all, rows match
    filter"). A MERGE is always planned at row level, so use it as the fallback.
    """
    condition = f"batch_token = '{escaped_token}' AND ({partition_predicate})"
    try:
        spark.sql(f"DELETE FROM {table} WHERE {condition}")
    except Exception as exc:
        if _PARTIAL_FILE_DELETE not in str(exc):
            raise
        spark.sql(
            f"MERGE INTO {table} AS t USING (SELECT 1 AS _k) AS s "
            f"ON {condition} WHEN MATCHED THEN DELETE"
        )


def _replace_batch(
    spark: SparkSession,
    frame: DataFrame,
    table: str,
    partition_columns: list[str],
    temp_view: str,
    batch_token: str,
) -> None:
    """Replace exactly one micro-batch without making batch_token a partition.

    Iceberg partitioning remains time-oriented for manageable file/partition
    counts. The deterministic batch_token is retained as a row-level idempotency
    key: retrying a batch deletes rows from that batch first, then appends the
    complete batch atomically.
    """
    namespace = table.split(".")[-2]
    catalog = table.split(".")[0]
    _ensure_namespace(spark, catalog, namespace)
    _ensure_table(spark, table, frame, partition_columns, temp_view)

    escaped = batch_token.replace("'", "''")
    partition_rows = frame.select(*partition_columns).distinct().collect()
    if partition_rows:
        predicates = []
        for row in partition_rows:
            parts = []
            for column in partition_columns:
                value = row[column]
                if value is None:
                    parts.append(f"{column} IS NULL")
                else:
                    literal = str(value).replace("'", "''")
                    parts.append(f"{column} = DATE '{literal}'" if "date" in column else f"{column} = '{literal}'")
            predicates.append("(" + " AND ".join(parts) + ")")
        partition_predicate = " OR ".join(predicates)
        _delete_batch_rows(spark, table, escaped, partition_predicate)

    frame.writeTo(table).append()


def write_micro_batch(
    spark: SparkSession,
    *,
    catalog: str,
    batch_token: str,
    bronze: DataFrame,
    quarantine: DataFrame | None,
    silver: DataFrame | None,
    decisions: DataFrame | None,
    features: DataFrame | None = None,
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
    _replace_batch(
        spark,
        bronze_frame,
        _qualified(catalog, TABLES["bronze"]),
        PARTITIONS["bronze"],
        "__iceberg_bronze_schema",
        batch_token,
    )

    if quarantine is not None:
        quarantine_frame = (
            quarantine
            .withColumn("quarantine_date", F.to_date("quarantined_at"))
            .withColumn("batch_token", F.lit(batch_token))
        )
        _replace_batch(
            spark,
            quarantine_frame,
            _qualified(catalog, TABLES["quarantine"]),
            PARTITIONS["quarantine"],
            "__iceberg_quarantine_schema",
            batch_token,
        )

    if silver is not None:
        _replace_batch(
            spark,
            silver,
            _qualified(catalog, TABLES["silver"]),
            PARTITIONS["silver"],
            "__iceberg_silver_schema",
            batch_token,
        )

    if features is not None:
        _replace_batch(
            spark,
            features,
            _qualified(catalog, TABLES["features"]),
            PARTITIONS["features"],
            "__iceberg_features_schema",
            batch_token,
        )

    if decisions is not None:
        decisions_frame = decisions
        if "ml_probability" not in decisions_frame.columns:
            decisions_frame = decisions_frame.withColumn(
                "ml_probability", F.lit(None).cast("double")
            )
        _replace_batch(
            spark,
            decisions_frame,
            _qualified(catalog, TABLES["decisions"]),
            PARTITIONS["decisions"],
            "__iceberg_decisions_schema",
            batch_token,
        )


def write_gold_tables(
    spark: SparkSession,
    *,
    catalog: str,
    customer_daily_risk: DataFrame,
    daily_kpis: DataFrame,
) -> None:
    """Write batch Gold aggregates as event-date partitioned Iceberg tables."""
    for logical_name, frame, temp_view in (
        (
            "customer_daily_risk",
            customer_daily_risk,
            "__iceberg_customer_daily_risk_schema",
        ),
        ("daily_kpis", daily_kpis, "__iceberg_daily_kpis_schema"),
    ):
        table = _qualified(catalog, TABLES[logical_name])
        _ensure_namespace(spark, catalog, "gold")
        _ensure_table(
            spark,
            table,
            frame,
            PARTITIONS[logical_name],
            temp_view,
        )
        frame.writeTo(table).overwritePartitions()
