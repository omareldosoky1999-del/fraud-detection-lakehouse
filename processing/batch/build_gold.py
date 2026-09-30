"""Build daily Gold aggregates from Iceberg or legacy HDFS decisions.

Iceberg is the preferred source when ICEBERG_ENABLED=true. HDFS remains an
explicit compatibility source/sink for the migration period.
"""
from __future__ import annotations

import argparse
import os
from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, functions as F

from processing.common.spark_session import get_spark
from processing.lakehouse.iceberg_tables import TABLES, write_gold_tables


def build_customer_daily_risk(decisions_for_day: DataFrame) -> DataFrame:
    return decisions_for_day.groupBy("client_id").agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).cast("decimal(18,4)").alias("total_amount_usd"),
        F.sum(F.when(F.col("decision") == "FLAG", 1).otherwise(0)).alias(
            "flagged_count"
        ),
        F.sum(F.when(F.col("decision") == "BLOCK", 1).otherwise(0)).alias(
            "blocked_count"
        ),
        F.max("risk_score").alias("max_risk_score"),
        F.countDistinct("country_src").alias("distinct_countries"),
        F.countDistinct("device_id").alias("distinct_devices"),
    )


def build_daily_kpis(decisions_for_day: DataFrame) -> DataFrame:
    total = decisions_for_day.count()
    if total == 0:
        return decisions_for_day.sparkSession.createDataFrame(
            [(0, Decimal("0.0000"), 0.0, 0.0, 0.0)],
            "total_txns long, total_amount_usd decimal(18,4), flagged_rate double, "
            "blocked_rate double, avg_risk_score double",
        )
    return decisions_for_day.agg(
        F.count("*").alias("total_txns"),
        F.round(F.sum("amount_usd"), 2)
        .cast("decimal(18,4)")
        .alias("total_amount_usd"),
        F.round(
            F.sum(F.when(F.col("decision") == "FLAG", 1).otherwise(0)) / total,
            4,
        ).alias("flagged_rate"),
        F.round(
            F.sum(F.when(F.col("decision") == "BLOCK", 1).otherwise(0)) / total,
            4,
        ).alias("blocked_rate"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
    )


def _read_decisions(
    spark,
    *,
    iceberg_enabled: bool,
    iceberg_catalog: str,
    decisions_path: str,
) -> DataFrame:
    if iceberg_enabled:
        table_name = TABLES["decisions"]
        return spark.table(f"{iceberg_catalog}.{table_name}")
    return spark.read.parquet(decisions_path)


def run(
    decisions_path: str,
    gold_customer_path: str,
    gold_kpis_path: str,
    run_date: date,
    lookback_days: int = 2,
    *,
    iceberg_enabled: bool | None = None,
    iceberg_catalog: str | None = None,
    legacy_hdfs_output: bool | None = None,
):
    if iceberg_enabled is None:
        iceberg_enabled = (
            os.getenv("ICEBERG_ENABLED", "false").lower() == "true"
        )
    if iceberg_catalog is None:
        iceberg_catalog = os.getenv("ICEBERG_CATALOG_NAME", "polaris")
    if legacy_hdfs_output is None:
        legacy_hdfs_output = (
            os.getenv(
                "LEGACY_HDFS_GOLD_OUTPUT",
                "false" if iceberg_enabled else "true",
            ).lower()
            == "true"
        )

    spark = get_spark("fraud-gold-batch", enable_hive=False)
    all_days = _read_decisions(
        spark,
        iceberg_enabled=iceberg_enabled,
        iceberg_catalog=iceberg_catalog,
        decisions_path=decisions_path,
    )

    try:
        for offset in range(max(1, lookback_days)):
            target_date = run_date - timedelta(days=offset)
            day_df = all_days.filter(
                F.col("event_date") == F.lit(target_date.isoformat())
            )
            event_date = F.lit(target_date).cast("date")
            customer_risk = build_customer_daily_risk(day_df).withColumn(
                "event_date", event_date
            )
            kpis = build_daily_kpis(day_df).withColumn(
                "event_date", event_date
            )

            if legacy_hdfs_output:
                (
                    customer_risk.write.mode("overwrite")
                    .option("partitionOverwriteMode", "dynamic")
                    .partitionBy("event_date")
                    .parquet(gold_customer_path)
                )
                (
                    kpis.write.mode("overwrite")
                    .option("partitionOverwriteMode", "dynamic")
                    .partitionBy("event_date")
                    .parquet(gold_kpis_path)
                )

            if iceberg_enabled:
                write_gold_tables(
                    spark,
                    catalog=iceberg_catalog,
                    customer_daily_risk=customer_risk,
                    daily_kpis=kpis,
                )

            print(
                f"[build_gold] {target_date}: "
                f"{day_df.count()} decisions -> "
                f"{customer_risk.count()} clients, "
                f"iceberg={iceberg_enabled}, "
                f"legacy_hdfs_output={legacy_hdfs_output}"
            )
    finally:
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser(
        description="Build Gold aggregates from Iceberg or legacy HDFS decisions."
    )
    parser.add_argument(
        "--date",
        required=True,
        help="YYYY-MM-DD (event_date partition to aggregate).",
    )
    parser.add_argument(
        "--decisions-path",
        default=os.getenv(
            "DECISIONS_PATH",
            "hdfs://namenode:8020/warehouse/gold/fraud_decisions",
        ),
    )
    parser.add_argument(
        "--gold-customer-path",
        default=os.getenv(
            "GOLD_CUSTOMER_PATH",
            "hdfs://namenode:8020/warehouse/gold/customer_daily_risk",
        ),
    )
    parser.add_argument(
        "--gold-kpis-path",
        default=os.getenv(
            "GOLD_KPIS_PATH",
            "hdfs://namenode:8020/warehouse/gold/daily_kpis",
        ),
    )
    parser.add_argument(
        "--lookback-days",
        type=int,
        default=int(os.getenv("GOLD_LOOKBACK_DAYS", "2")),
    )
    parser.add_argument(
        "--legacy-hdfs-output",
        action="store_true",
        default=None,
        help="Force legacy HDFS Gold writes even when Iceberg is enabled.",
    )
    args = parser.parse_args(argv)

    run(
        args.decisions_path,
        args.gold_customer_path,
        args.gold_kpis_path,
        date.fromisoformat(args.date),
        args.lookback_days,
        legacy_hdfs_output=args.legacy_hdfs_output,
    )


if __name__ == "__main__":
    main()
