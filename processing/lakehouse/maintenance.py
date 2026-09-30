"""Periodic Iceberg table maintenance outside the fraud streaming latency path."""
from __future__ import annotations

import argparse
import os
from datetime import datetime, timedelta, timezone

from processing.common.spark_session import get_spark


DEFAULT_TABLES = [
    "bronze.transactions",
    "quality.quarantine",
    "silver.transactions",
    "features.transaction_features",
    "gold.fraud_decisions",
]


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def maintain_table(
    spark,
    catalog: str,
    table: str,
    retention_hours: int,
    target_file_size_bytes: int,
) -> None:
    qualified = f"{catalog}.{table}"
    cutoff = datetime.now(timezone.utc) - timedelta(hours=retention_hours)
    cutoff_sql = cutoff.strftime("%Y-%m-%d %H:%M:%S")

    spark.sql(
        f"CALL {catalog}.system.rewrite_data_files("
        f"table => {_sql_literal(table)}, "
        f"options => map('target-file-size-bytes', '{target_file_size_bytes}'))"
    )

    spark.sql(
        f"CALL {catalog}.system.expire_snapshots("
        f"table => {_sql_literal(table)}, "
        f"older_than => TIMESTAMP '{cutoff_sql}', "
        f"retain_last => 20)"
    )

    print(
        f"[iceberg-maintenance] {qualified}: "
        f"compaction + snapshots older than {cutoff_sql} UTC"
    )


def run(
    catalog: str,
    tables: list[str],
    retention_hours: int = 168,
    target_file_size_bytes: int = 536870912,
):
    spark = get_spark("iceberg-maintenance", enable_hive=False)
    try:
        for table in tables:
            maintain_table(
                spark,
                catalog,
                table,
                retention_hours,
                target_file_size_bytes,
            )
    finally:
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--catalog",
        default=os.getenv("ICEBERG_CATALOG_NAME", "polaris"),
    )
    parser.add_argument(
        "--tables",
        nargs="+",
        default=DEFAULT_TABLES,
    )
    parser.add_argument(
        "--retention-hours",
        type=int,
        default=int(os.getenv("ICEBERG_SNAPSHOT_RETENTION_HOURS", "168")),
    )
    parser.add_argument(
        "--target-file-size-bytes",
        type=int,
        default=int(os.getenv("ICEBERG_TARGET_FILE_SIZE_BYTES", "536870912")),
    )
    args = parser.parse_args(argv)
    run(
        args.catalog,
        args.tables,
        args.retention_hours,
        args.target_file_size_bytes,
    )


if __name__ == "__main__":
    main()
