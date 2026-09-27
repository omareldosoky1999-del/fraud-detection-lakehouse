"""Gold batch job: aggregate one day of `fraud_decisions` into the two Gold
tables (see warehouse/hive/ddl.sql). Run once per day by
orchestration/dags/fraud_gold_dag.py, NOT continuously -- this is
deliberately a plain batch Spark job (spark-submit, runs, exits), unlike the
streaming bronze/silver job.
"""
import argparse
import os
from datetime import date, timedelta
from decimal import Decimal

from pyspark.sql import DataFrame, functions as F

from processing.common.spark_session import get_spark


def build_customer_daily_risk(decisions_for_day: DataFrame) -> DataFrame:
    return decisions_for_day.groupBy("client_id").agg(
        F.count("*").alias("txn_count"),
        F.round(F.sum("amount_usd"), 2).cast("decimal(18,4)").alias("total_amount_usd"),
        F.sum(F.when(F.col("decision") == "FLAG", 1).otherwise(0)).alias("flagged_count"),
        F.sum(F.when(F.col("decision") == "BLOCK", 1).otherwise(0)).alias("blocked_count"),
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
        F.round(F.sum("amount_usd"), 2).cast("decimal(18,4)").alias("total_amount_usd"),
        F.round(F.sum(F.when(F.col("decision") == "FLAG", 1).otherwise(0)) / total, 4).alias("flagged_rate"),
        F.round(F.sum(F.when(F.col("decision") == "BLOCK", 1).otherwise(0)) / total, 4).alias("blocked_rate"),
        F.round(F.avg("risk_score"), 2).alias("avg_risk_score"),
    )


def run(decisions_path: str, gold_customer_path: str, gold_kpis_path: str, run_date: date, lookback_days: int = 2):
    spark = get_spark("fraud-gold-batch", enable_hive=False)
    all_days = spark.read.parquet(decisions_path)
    for offset in range(max(1, lookback_days)):
        target_date = run_date - timedelta(days=offset)
        day_df = all_days.filter(F.col("event_date") == F.lit(target_date.isoformat()))
        event_date = F.lit(target_date).cast("date")
        customer_risk = build_customer_daily_risk(day_df).withColumn("event_date", event_date)
        kpis = build_daily_kpis(day_df).withColumn("event_date", event_date)
        (customer_risk.write.mode("overwrite").option("partitionOverwriteMode","dynamic").partitionBy("event_date").parquet(gold_customer_path))
        (kpis.write.mode("overwrite").option("partitionOverwriteMode","dynamic").partitionBy("event_date").parquet(gold_kpis_path))
        print(f"[build_gold] {target_date}: {day_df.count()} decisions -> {customer_risk.count()} clients, KPIs written")
    spark.stop()


def main(argv=None):
    ap = argparse.ArgumentParser(description="Build the Gold layer for one day.")
    ap.add_argument("--date", required=True, help="YYYY-MM-DD (the event_date partition to aggregate)")
    ap.add_argument("--decisions-path", default=os.getenv("DECISIONS_PATH", "hdfs://namenode:8020/warehouse/gold/fraud_decisions"))
    ap.add_argument("--gold-customer-path", default=os.getenv("GOLD_CUSTOMER_PATH", "hdfs://namenode:8020/warehouse/gold/customer_daily_risk"))
    ap.add_argument("--gold-kpis-path", default=os.getenv("GOLD_KPIS_PATH", "hdfs://namenode:8020/warehouse/gold/daily_kpis"))
    ap.add_argument("--lookback-days", type=int, default=int(os.getenv("GOLD_LOOKBACK_DAYS", "2")))
    a = ap.parse_args(argv)
    run(a.decisions_path, a.gold_customer_path, a.gold_kpis_path, date.fromisoformat(a.date), a.lookback_days)


if __name__ == "__main__":
    main()
