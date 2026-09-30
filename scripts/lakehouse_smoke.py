"""Smoke-test Spark -> Polaris REST -> MinIO -> Iceberg connectivity."""
from __future__ import annotations

from pyspark.sql import functions as F
from processing.common.spark_session import get_spark


CATALOG = "polaris"
NAMESPACE = "e2e_smoke"
TABLE = f"{CATALOG}.{NAMESPACE}.transactions"


def main() -> None:
    spark = get_spark("lakehouse-smoke", enable_hive=False)
    try:
        spark.sql(f"CREATE NAMESPACE IF NOT EXISTS {CATALOG}.{NAMESPACE}")
        frame = (
            spark.createDataFrame(
                [(1, "SMOKE", 10.50), (2, "SMOKE", 20.25)],
                ["transaction_id", "status", "amount"],
            )
            .withColumn("event_date", F.current_date())
        )

        (
            frame.writeTo(TABLE)
            .using("iceberg")
            .tableProperty("format-version", "2")
            .partitionedBy("event_date")
            .createOrReplace()
        )

        rows = spark.table(TABLE).orderBy("transaction_id").collect()
        assert [(r["transaction_id"], r["status"]) for r in rows] == [
            (1, "SMOKE"),
            (2, "SMOKE"),
        ]
        print(f"[LAKEHOUSE_SMOKE] Spark read/write OK: {TABLE}")

        spark.sql(f"DROP TABLE IF EXISTS {TABLE}")
        spark.sql(f"DROP NAMESPACE IF EXISTS {CATALOG}.{NAMESPACE}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
