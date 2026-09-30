"""Validate a persisted Silver partition with Great Expectations."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pyspark.sql import functions as F

from processing.common.spark_session import get_spark
from processing.quality.gx_validation import assert_silver_dataframe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", help="Silver event_date in YYYY-MM-DD format.")
    parser.add_argument(
        "--path",
        default="hdfs://namenode:8020/warehouse/silver/transactions",
        help="Legacy HDFS Silver path when --iceberg-catalog is not supplied.",
    )
    parser.add_argument(
        "--iceberg-catalog",
        default=None,
        help="Iceberg catalog name, e.g. polaris.",
    )
    parser.add_argument(
        "--iceberg-table",
        default="silver.transactions",
        help="Iceberg logical Silver table.",
    )
    parser.add_argument(
        "--report",
        default="reports/dq/silver_validation.json",
    )
    args = parser.parse_args()

    if args.date:
        datetime.strptime(args.date, "%Y-%m-%d")

    spark = get_spark("silver-gx-validation", enable_hive=False)
    try:
        if args.iceberg_catalog:
            qualified = f"{args.iceberg_catalog}.{args.iceberg_table}"
            if not spark.catalog.tableExists(qualified):
                raise ValueError(f"Silver Iceberg table does not exist: {qualified}")
            df = spark.table(qualified)
            if args.date:
                df = df.filter(F.col("event_date") == F.lit(args.date))
            source = "iceberg"
            source_ref = qualified
        else:
            input_path = args.path.rstrip("/")
            if args.date:
                input_path = f"{input_path}/event_date={args.date}"
            df = spark.read.parquet(input_path)
            source = "hdfs"
            source_ref = input_path

        if df.rdd.isEmpty():
            raise ValueError(f"Silver source is empty: {source_ref}")

        report = assert_silver_dataframe(df)
        report["source_type"] = source
        report["source"] = source_ref
        report["event_date"] = args.date
        report["validated_at_utc"] = datetime.utcnow().isoformat() + "Z"

        output = Path(args.report)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(
            json.dumps(report, indent=2, default=str),
            encoding="utf-8",
        )
        print(json.dumps(report, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
