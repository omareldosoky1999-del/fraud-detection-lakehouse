"""Decode Confluent-wire-format Avro records with schema lineage metadata.

Confluent serialization prepends:
- byte 0: magic byte 0
- bytes 1-4: big-endian schema ID
- remaining bytes: Avro payload

The decoder keeps the schema ID as kafka_schema_id so Bronze/Silver records
retain the exact registry version used at ingestion time.
"""
from pyspark.sql import DataFrame
from pyspark.sql.avro.functions import from_avro
from pyspark.sql import functions as F


CONFLUENT_HEADER_BYTES = 5


def extract_confluent_metadata(
    df: DataFrame,
    value_col: str = "value",
) -> DataFrame:
    """Add schema ID and magic-byte metadata before stripping the header."""
    return (
        df.withColumn(
            "kafka_confluent_magic",
            F.hex(F.substring(F.col(value_col), 1, 1)),
        )
        .withColumn(
            "kafka_schema_id",
            F.conv(
                F.hex(F.substring(F.col(value_col), 2, 4)),
                16,
                10,
            ).cast("int"),
        )
    )


def strip_confluent_header(
    df: DataFrame,
    value_col: str = "value",
) -> DataFrame:
    """Drop the 5-byte Confluent header and leave only Avro payload bytes."""
    return df.withColumn(
        value_col,
        F.expr(
            f"substring({value_col}, {CONFLUENT_HEADER_BYTES + 1}, "
            f"length({value_col}) - {CONFLUENT_HEADER_BYTES})"
        ),
    )


def decode_transactions(
    df: DataFrame,
    avro_schema_json: str,
    value_col: str = "value",
) -> DataFrame:
    """Decode Kafka records while retaining Kafka and schema lineage metadata."""
    enriched = extract_confluent_metadata(df, value_col)
    stripped = strip_confluent_header(enriched, value_col)
    decoded = stripped.withColumn(
        "_record",
        from_avro(F.col(value_col), avro_schema_json),
    )

    return decoded.select(
        "_record.*",
        F.col("partition").alias("kafka_partition"),
        F.col("offset").alias("kafka_offset"),
        F.col("timestamp").alias("kafka_timestamp"),
        "kafka_confluent_magic",
        "kafka_schema_id",
    )
