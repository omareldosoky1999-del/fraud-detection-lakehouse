"""Generate a small Spark workload so OpenLineage emits dataset/job events."""
from __future__ import annotations

from pyspark.sql import functions as F

from processing.common.spark_session import get_spark

OUTPUT = "hdfs:///user/spark/openlineage-smoke-output"


def main():
    spark = get_spark("openlineage-spark-smoke", enable_hive=False)
    try:
        frame = spark.range(10).withColumn("risk_score", (F.col("id") * 10).cast("int"))
        frame.write.mode("overwrite").parquet(OUTPUT)
        check = spark.read.parquet(OUTPUT).filter("risk_score >= 50").count()
        assert check == 5
        print(f"[OPENLINEAGE_SMOKE] Spark workload completed; output={OUTPUT}; rows={check}")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
