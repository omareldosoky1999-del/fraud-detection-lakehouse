"""Pure, Spark-batch-DataFrame transforms shared by the streaming job and
the tests.

Kept free of any I/O (no Kafka, no HDFS, no HBase) on purpose: everything
here takes a DataFrame and returns a DataFrame, so it can be unit-tested with
a small in-memory DataFrame under `local[*]`, and reused unchanged inside a
structured-streaming `foreachBatch` (which already hands you a plain static
DataFrame per micro-batch).
"""
from typing import Tuple

from pyspark.sql import DataFrame, functions as F
from pyspark.sql.types import DecimalType

from common.fx import FX_TO_USD
from processing.common.dq_rules import (
    REQUIRED_NOT_NULL, TRANS_DATE_FORMAT, VALID_CURRENCY, VALID_TRANS_STATUS, VALID_TRANS_TYPE)


def add_dq_errors(df: DataFrame) -> DataFrame:
    """Add a `dq_errors` array<string> column: empty array = record is valid."""
    errors = []

    for c in REQUIRED_NOT_NULL:
        errors.append(F.when(F.col(c).isNull(), F.lit(f"null:{c}")))

    errors.append(F.when(~F.col("Trans_status").isin(*VALID_TRANS_STATUS),
                         F.lit("invalid:Trans_status")))
    errors.append(F.when(~F.col("Trans_type").isin(*VALID_TRANS_TYPE),
                         F.lit("invalid:Trans_type")))
    errors.append(F.when(~F.col("Currency").isin(*VALID_CURRENCY),
                         F.lit("invalid:Currency")))
    errors.append(F.when(F.col("Trans_amount") <= 0, F.lit("invalid:Trans_amount<=0")))
    errors.append(F.when(
        F.to_timestamp(F.col("Trans_date"), TRANS_DATE_FORMAT).isNull(),
        F.lit("invalid:Trans_date_unparseable")))

    return df.withColumn(
        "dq_errors",
        F.array_except(F.array(*errors), F.array(F.lit(None).cast("string"))),
    )


def split_valid_invalid(df: DataFrame) -> Tuple[DataFrame, DataFrame]:
    """DQ-check `df` (must have the raw transaction columns) and split it.

    Returns (valid_df, quarantine_df). `quarantine_df` keeps `dq_errors` and
    a `quarantined_at` timestamp for the DLQ/quarantine table; `valid_df` has
    `dq_errors` dropped since it is (by construction) always empty there.
    """
    checked = add_dq_errors(df)
    valid = checked.filter(F.size("dq_errors") == 0).drop("dq_errors")
    invalid = (checked.filter(F.size("dq_errors") > 0)
              .withColumn("quarantined_at", F.current_timestamp()))
    return valid, invalid


def dedup_batch(df: DataFrame, key: str = "Trans_id") -> DataFrame:
    """Drop duplicate keys within a single (already-collected) batch.

    This is the batch-level half of de-duplication. The streaming half is
    `readStream...dropDuplicatesWithinWatermark`/`dropDuplicates` applied to
    the streaming DataFrame *before* foreachBatch (see bronze_silver_job.py) --
    that catches duplicate deliveries across micro-batches within the
    watermark window; this catches any leftovers inside one micro-batch.
    """
    return df.dropDuplicates([key])


def _fx_map_column():
    """`create_map` literal built FROM common.fx.FX_TO_USD -- one source of
    truth shared with the generator, evaluated at plan time (no UDF, no
    per-row python call)."""
    items = []
    for k, v in FX_TO_USD.items():
        items += [F.lit(k), F.lit(v)]
    return F.create_map(*items)


def to_silver(df: DataFrame) -> DataFrame:
    """Valid raw transaction rows -> the Silver schema.

    - renames to snake_case
    - parses Trans_date into a real `event_time` timestamp (+ `event_date`
      partition column)
    - converts the amount to USD using the shared FX table (unknown currency
      falls back to 1.0) and stores financial values as Decimal(18,4)
    - adds `ingest_time` for lineage/debugging
    """
    fx = _fx_map_column()
    event_time = F.to_timestamp(F.col("Trans_date"), TRANS_DATE_FORMAT)
    return (
        df
        .withColumn("event_time", event_time)
        .withColumn("event_date", F.to_date("event_time"))
        .withColumn("fx_rate", F.coalesce(fx[F.col("Currency")], F.lit(1.0)).cast(DecimalType(18,8)))
        .withColumn("amount", F.col("Trans_amount").cast(DecimalType(18,4)))
        .withColumn("amount_usd", F.round(F.col("Trans_amount").cast(DecimalType(18,4)) * F.col("fx_rate"), 2).cast(DecimalType(18,4)))
        .withColumn("ingest_time", F.current_timestamp())
        .select(
            F.col("Trans_id").alias("transaction_id"),
            F.col("Clt_id").alias("client_id"),
            F.col("Card_id").alias("card_id"),
            F.col("Dev_id").alias("device_id"),
            "fx_rate",
            "amount_usd",
            F.col("Currency").alias("currency"),
            F.col("Trans_type").alias("txn_type"),
            F.col("Trans_status").alias("status"),
            F.col("Trans_destination").alias("destination_bank"),
            F.col("Dev_Ip_Location").alias("device_location"),
            F.col("Trans_Ref_No").alias("ref_no"),
            F.col("Trans_Reason").alias("reason"),
            F.col("Dest_account_No").alias("dest_account_no"),
            F.col("Country_Src").alias("country_src"),
            F.col("Country_Dest").alias("country_dest"),
            "event_time",
            "event_date",
            "ingest_time",
        )
    )
