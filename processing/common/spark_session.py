"""Single place that builds the SparkSession, so every job (streaming and
batch) uses the same Hive/warehouse configuration.

Kept deliberately tiny: cluster-specific settings (master, packages, executor
memory) belong in the spark-submit command / docker-compose, not here, so the
same code runs unchanged in `local[*]` for tests and on the real cluster.
"""
from pyspark.sql import SparkSession


def get_spark(app_name: str, *, enable_hive: bool = True, extra_conf=None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    if enable_hive:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
