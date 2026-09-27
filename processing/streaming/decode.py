"""Decode Confluent-wire-format Avro records read from Kafka.

Why this file exists
---------------------
Vanilla Spark's `from_avro` (org.apache.spark.sql.avro) decodes RAW Avro
binary. Confluent's serializer (what the producer and the Schema Registry
use) prepends a 5-byte header before the Avro payload:

    byte 0       magic byte, always 0x0
    bytes 1-4    big-endian int32 schema id (registered in the Schema Registry)
    bytes 5..    the actual Avro binary payload

The old `kafka_to_bronze.py` in this repo ignored this and did
`CAST(value AS STRING)`, which produces garbage for every record. The fix is
two steps: (1) strip the first 5 bytes, (2) hand the remainder to `from_avro`
with the schema we already control (ingestion/schema/transaction.avsc).

This project does not evolve the schema at runtime, so decoding against a
fixed local copy of the schema (rather than resolving the schema id against
the registry on every record) is enough and much simpler. If the team adds
schema evolution later, swap this for a per-record registry lookup keyed by
the embedded schema id.

Requires the job to be submitted with Spark Avro + Kafka connector packages, e.g.:
    --packages org.apache.spark:spark-avro_2.12:3.0.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.0.0
(match the Scala/Spark version actually running in the cluster; see
docker/spark/Dockerfile / docker-compose.yml for the pinned version.)
"""
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.functions import col, expr

CONFLUENT_HEADER_BYTES = 5


def strip_confluent_header(df: DataFrame, value_col: str = "value") -> DataFrame:
    """Return `df` with `value_col` sliced to drop the 5-byte Confluent header.

    `substring` is 1-indexed, so byte 6 onward is the Avro payload.
    """
    return df.withColumn(
        value_col,
        expr(f"substring({value_col}, {CONFLUENT_HEADER_BYTES + 1}, length({value_col}) - {CONFLUENT_HEADER_BYTES})"),
    )


def decode_transactions(df: DataFrame, avro_schema_json: str, value_col: str = "value") -> DataFrame:
    """Kafka raw DataFrame (key/value bytes + kafka metadata) -> one column
    per transaction field, plus the original Kafka metadata columns kept for
    lineage/debugging (kafka_offset, kafka_partition, kafka_timestamp).
    """
    stripped = strip_confluent_header(df, value_col)
    decoded = stripped.withColumn("_record", from_avro(col(value_col), avro_schema_json))
    return (
        decoded
        .select(
            "_record.*",
            col("partition").alias("kafka_partition"),
            col("offset").alias("kafka_offset"),
            col("timestamp").alias("kafka_timestamp"),
        )
    )
