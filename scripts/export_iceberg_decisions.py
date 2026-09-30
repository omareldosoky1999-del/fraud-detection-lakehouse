"""Export an Iceberg fraud-decision partition to Parquet for offline evaluation."""
from __future__ import annotations

import argparse
from datetime import datetime

from pyspark.sql import functions as F

from processing.common.spark_session import get_spark


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--catalog", default="polaris")
    parser.add_argument("--table", default="gold.fraud_decisions")
    parser.add_argument("--date")
    parser.add_argument("--output", required=True)
    args = parser.parse_args()

    if args.date:
        datetime.strptime(args.date, "%Y-%m-%d")

    spark = get_spark("iceberg-decision-export", enable_hive=False)
    try:
        table = f"{args.catalog}.{args.table}"
        df = spark.table(table)
        if args.date:
            df = df.filter(F.col("event_date") == F.lit(args.date))
        if df.rdd.isEmpty():
            raise ValueError(f"No decisions found in {table} for date={args.date}")
        df.write.mode("overwrite").parquet(args.output)
        print(f"[ICEBERG_EXPORT] {table} -> {args.output} rows={df.count()}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
