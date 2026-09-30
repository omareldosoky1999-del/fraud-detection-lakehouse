"""Validate a persisted Silver partition with Great Expectations."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from processing.common.spark_session import get_spark
from processing.quality.gx_validation import assert_silver_dataframe


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--date",
        help="Silver event_date partition in YYYY-MM-DD format.",
    )
    parser.add_argument(
        "--path",
        default="hdfs://namenode:8020/warehouse/silver/transactions",
    )
    parser.add_argument(
        "--report",
        default="reports/dq/silver_validation.json",
    )
    args = parser.parse_args()

    if args.date:
        datetime.strptime(args.date, "%Y-%m-%d")
        input_path = f"{args.path.rstrip('/')}/event_date={args.date}"
    else:
        input_path = args.path

    spark = get_spark("silver-gx-validation", enable_hive=False)
    try:
        df = spark.read.parquet(input_path)
        if df.rdd.isEmpty():
            raise ValueError(f"Silver partition is empty: {input_path}")

        report = assert_silver_dataframe(df)
        report["path"] = input_path
        report["validated_at_utc"] = datetime.utcnow().isoformat() + "Z"

        output = Path(args.report)
        output.parent.mkdir(parents=True, exist_ok=True)
        output.write_text(json.dumps(report, indent=2, default=str), encoding="utf-8")
        print(json.dumps(report, indent=2, default=str))
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
