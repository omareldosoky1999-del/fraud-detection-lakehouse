"""Restart-safety helpers for Spark foreachBatch.

When the migration-era HDFS sink is enabled, commit markers remain HDFS files.
For Iceberg-only cloud deployments, commit state is stored in a small Iceberg
control table so no HDFS dependency remains in the streaming path.
"""
from __future__ import annotations

import hashlib
from datetime import datetime, timezone

from pyspark.sql import DataFrame, functions as F


DEFAULT_ICEBERG_COMMIT_TABLE = "control.streaming_batch_commits"


def build_batch_token(batch_df: DataFrame, batch_id: int) -> str:
    """Return a deterministic token for the Kafka records in this batch."""
    required = {"kafka_partition", "kafka_offset"}
    if not required.issubset(set(batch_df.columns)):
        return f"spark-{batch_id}"

    summary = (
        batch_df.groupBy("kafka_partition")
        .agg(
            F.min("kafka_offset").alias("min_offset"),
            F.max("kafka_offset").alias("max_offset"),
            F.count("*").alias("row_count"),
        )
        .orderBy("kafka_partition")
        .collect()
    )
    payload = "|".join(
        f"{int(r.kafka_partition)}:{int(r.min_offset)}:{int(r.max_offset)}:{int(r.row_count)}"
        for r in summary
    )
    if not payload:
        return f"spark-{batch_id}"
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()[:20]


def batch_path(base_path: str, batch_token: str) -> str:
    return f"{base_path.rstrip('/')}/batch_token={batch_token}"


def _fs(spark, path: str):
    jvm = spark.sparkContext._jvm
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    return jvm.org.apache.hadoop.fs.FileSystem.get(
        jvm.org.apache.hadoop.fs.Path(path).toUri(),
        hadoop_conf,
    )


def path_exists(spark, path: str) -> bool:
    fs = _fs(spark, path)
    return bool(
        fs.exists(
            spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
        )
    )


def delete_path(spark, path: str) -> None:
    fs = _fs(spark, path)
    jpath = spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)
    if fs.exists(jpath):
        fs.delete(jpath, True)


def commit_marker_path(commit_root: str, batch_token: str) -> str:
    return f"{commit_root.rstrip('/')}/batch_token={batch_token}/COMMITTED"


def is_committed(spark, commit_root: str, batch_token: str) -> bool:
    return path_exists(spark, commit_marker_path(commit_root, batch_token))


def mark_committed(spark, commit_root: str, batch_token: str) -> None:
    fs = _fs(spark, commit_root)
    jvm = spark.sparkContext._jvm
    marker = jvm.org.apache.hadoop.fs.Path(
        commit_marker_path(commit_root, batch_token)
    )
    fs.mkdirs(marker.getParent())
    out = fs.create(marker, True)
    try:
        out.write(b"committed\n")
    finally:
        out.close()


def _ensure_iceberg_commit_table(spark, table: str) -> None:
    catalog, namespace, name = table.split(".", 2)
    spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {catalog}.{namespace}")
    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table} (
            batch_token STRING,
            committed_at TIMESTAMP
        )
        USING iceberg
        """
    )


def is_committed_iceberg(
    spark,
    catalog: str,
    batch_token: str,
    table: str = DEFAULT_ICEBERG_COMMIT_TABLE,
) -> bool:
    qualified = table if table.count(".") == 2 else f"{catalog}.{table}"
    _ensure_iceberg_commit_table(spark, qualified)
    return (
        spark.table(qualified)
        .filter(F.col("batch_token") == F.lit(batch_token))
        .limit(1)
        .count()
        > 0
    )


def mark_committed_iceberg(
    spark,
    catalog: str,
    batch_token: str,
    table: str = DEFAULT_ICEBERG_COMMIT_TABLE,
) -> None:
    qualified = table if table.count(".") == 2 else f"{catalog}.{table}"
    _ensure_iceberg_commit_table(spark, qualified)
    committed_at = datetime.now(timezone.utc).replace(tzinfo=None)
    frame = spark.createDataFrame(
        [(batch_token, committed_at)],
        ["batch_token", "committed_at"],
    )
    frame.createOrReplaceTempView("__streaming_commit_marker")
    spark.sql(
        f"""
        MERGE INTO {qualified} AS target
        USING __streaming_commit_marker AS source
        ON target.batch_token = source.batch_token
        WHEN NOT MATCHED THEN
          INSERT (batch_token, committed_at)
          VALUES (source.batch_token, source.committed_at)
        """
    )
