"""Create a minimal valid Silver partition for the Great Expectations E2E gate."""
from __future__ import annotations

from datetime import datetime
from pyspark.sql import functions as F
from processing.common.spark_session import get_spark

OUTPUT = "hdfs:///warehouse/silver/transactions"
DATE = "2026-09-01"


def main():
    spark = get_spark("great-expectations-silver-smoke", enable_hive=False)
    try:
        frame = spark.createDataFrame(
            [
                (
                    1001,
                    77,
                    1,
                    5,
                    1.0,
                    250.00,
                    "USD",
                    "Withdrawal",
                    "Successful",
                    "CIB",
                    "Cairo",
                    "R1001",
                    "Shopping",
                    1,
                    "Egypt",
                    "Egypt",
                    datetime(2026, 9, 1, 10, 0, 0),
                )
            ],
            [
                "transaction_id",
                "client_id",
                "card_id",
                "device_id",
                "fx_rate",
                "amount_usd",
                "currency",
                "txn_type",
                "status",
                "destination_bank",
                "device_location",
                "ref_no",
                "reason",
                "dest_account_no",
                "country_src",
                "country_dest",
                "event_time",
            ],
        ).withColumn("event_date", F.to_date("event_time"))          .withColumn("ingest_time", F.col("event_time"))

        frame.write.mode("overwrite").partitionBy("event_date").parquet(OUTPUT)
        print(f"[GX_SMOKE] wrote valid Silver partition: {OUTPUT}/event_date={DATE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
