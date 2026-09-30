"""Single place that builds the SparkSession, so every job (streaming and
batch) uses the same Hive/warehouse configuration.

Kept deliberately tiny: cluster-specific settings (master, packages, executor
memory) belong in the spark-submit command / docker-compose, not here, so the
same code runs unchanged in `local[*]` for tests and on the real cluster.
"""
import os

from pyspark.sql import SparkSession


def get_spark(app_name: str, *, enable_hive: bool = True, extra_conf=None) -> SparkSession:
    builder = SparkSession.builder.appName(app_name)

    if os.getenv("OPENLINEAGE_ENABLED", "false").lower() == "true":
        builder = (
            builder
            .config(
                "spark.extraListeners",
                "io.openlineage.spark.agent.OpenLineageSparkListener",
            )
            .config(
                "spark.openlineage.transport.type",
                os.getenv("OPENLINEAGE_TRANSPORT_TYPE", "http"),
            )
            .config(
                "spark.openlineage.transport.url",
                os.getenv("OPENLINEAGE_URL", "http://marquez:5000"),
            )
            .config(
                "spark.openlineage.transport.endpoint",
                os.getenv("OPENLINEAGE_ENDPOINT", "/api/v1/lineage"),
            )
            .config(
                "spark.openlineage.namespace",
                os.getenv("OPENLINEAGE_NAMESPACE", "fraud_detection"),
            )
            .config(
                "spark.openlineage.transport.timeoutInMillis",
                os.getenv("OPENLINEAGE_TIMEOUT_MS", "5000"),
            )
        )

    if os.getenv("ICEBERG_ENABLED", "false").lower() == "true":
        catalog = os.getenv("ICEBERG_CATALOG_NAME", "polaris")
        builder = (builder
                   .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")
                   .config(f"spark.sql.catalog.{catalog}", "org.apache.iceberg.spark.SparkCatalog")
                   .config(f"spark.sql.catalog.{catalog}.type", "rest")
                   .config(f"spark.sql.catalog.{catalog}.uri", os.getenv("POLARIS_URI", "http://polaris:8181/api/catalog"))
                   .config(f"spark.sql.catalog.{catalog}.warehouse", os.getenv("POLARIS_WAREHOUSE", "fraud"))
                   .config(f"spark.sql.catalog.{catalog}.rest.auth.type", "oauth2")
                   .config(f"spark.sql.catalog.{catalog}.oauth2-server-uri", os.getenv("POLARIS_TOKEN_URI", "http://polaris:8181/api/catalog/v1/oauth/tokens"))
                   .config(f"spark.sql.catalog.{catalog}.credential", os.getenv("POLARIS_CREDENTIAL", "root:s3cr3t"))
                   .config(f"spark.sql.catalog.{catalog}.scope", "PRINCIPAL_ROLE:ALL")
                   .config(f"spark.sql.catalog.{catalog}.token-refresh-enabled", "true")
                   .config("spark.redaction.regex", "(?i)credential|secret|password|token")
                   .config(
                       f"spark.sql.catalog.{catalog}.io-impl",
                       os.getenv(
                           "ICEBERG_FILEIO_IMPL",
                           "org.apache.iceberg.aws.s3.S3FileIO",
                       ),
                   ))
        access_key = os.getenv("ICEBERG_S3_ACCESS_KEY")
        secret_key = os.getenv("ICEBERG_S3_SECRET_KEY")
        if access_key and secret_key:
            builder = (builder
                       .config(f"spark.sql.catalog.{catalog}.s3.endpoint", os.getenv("ICEBERG_S3_ENDPOINT", "http://minio:9000"))
                       .config(f"spark.sql.catalog.{catalog}.s3.path-style-access", "true")
                       .config(f"spark.sql.catalog.{catalog}.s3.access-key-id", access_key)
                       .config(f"spark.sql.catalog.{catalog}.s3.secret-access-key", secret_key))
    for k, v in (extra_conf or {}).items():
        builder = builder.config(k, v)
    if enable_hive:
        builder = builder.enableHiveSupport()
    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark
