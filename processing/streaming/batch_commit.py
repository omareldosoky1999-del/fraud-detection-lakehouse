"""Restart-safety helpers for foreachBatch.

A Spark checkpoint tracks source progress, but foreachBatch side effects are
external to that checkpoint. These helpers make HDFS batch outputs idempotent
for the same Kafka input set by deriving a deterministic batch token from
partition/offset ranges and recording a final commit marker after all sinks
finish.
"""
from __future__ import annotations

import hashlib
from typing import Optional

from pyspark.sql import DataFrame, functions as F


def build_batch_token(batch_df: DataFrame, batch_id: int) -> str:
    """Return a deterministic token for the Kafka records in this micro-batch.

    Tests may pass DataFrames without Kafka metadata; those fall back to the
    Spark batch_id so the helper remains easy to unit-test.
    """
    required = {"kafka_partition", "kafka_offset"}
    if not required.issubset(set(batch_df.columns)):
        return f"spark-{batch_id}"

    summary = (batch_df
               .groupBy("kafka_partition")
               .agg(F.min("kafka_offset").alias("min_offset"),
                    F.max("kafka_offset").alias("max_offset"),
                    F.count("*").alias("row_count"))
               .orderBy("kafka_partition")
               .collect())
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
    return jvm.org.apache.hadoop.fs.FileSystem.get(jvm.org.apache.hadoop.fs.Path(path).toUri(), hadoop_conf)


def path_exists(spark, path: str) -> bool:
    fs = _fs(spark, path)
    return bool(fs.exists(spark.sparkContext._jvm.org.apache.hadoop.fs.Path(path)))


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
    marker = jvm.org.apache.hadoop.fs.Path(commit_marker_path(commit_root, batch_token))
    fs.mkdirs(marker.getParent())
    out = fs.create(marker, True)
    try:
        out.write(b"committed\n")
    finally:
        out.close()
