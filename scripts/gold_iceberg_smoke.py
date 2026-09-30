"""Prepare Iceberg fraud decisions for the Gold Iceberg E2E test."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import functions as F
from processing.common.spark_session import get_spark

CATALOG = "polaris"
DECISIONS = f"{CATALOG}.gold.fraud_decisions"
TEST_DATE = "2026-09-01"


def main():
    spark = get_spark("gold-iceberg-smoke", enable_hive=False)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.gold")
        spark.sql(f"DROP TABLE IF EXISTS {DECISIONS}")

        frame = spark.createDataFrame(
            [
                (1, 77, 100.0, "PASS", 5, "Egypt", 10, "b1"),
                (2, 77, 150.0, "FLAG", 70, "Egypt", 10, "b1"),
                (3, 77, 200.0, "BLOCK", 95, "France", 20, "b1"),
            ],
            [
                "transaction_id",
                "client_id",
                "amount_usd",
                "decision",
                "risk_score",
                "country_src",
                "device_id",
                "batch_token",
            ],
        ).withColumn(
            "event_time",
            F.lit(datetime(2026, 9, 1, 10, 0, 0)).cast("timestamp"),
        ).withColumn(
            "event_date",
            F.to_date("event_time"),
        ).withColumn(
            "ml_probability",
            F.lit(None).cast("double"),
        )

        (
            frame.writeTo(DECISIONS)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .partitionedBy("event_date")
            .createOrReplace()
        )

        count = spark.table(DECISIONS).count()
        assert count == 3
        print(f"[GOLD_ICEBERG_SMOKE] prepared {DECISIONS} with {count} decisions")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
