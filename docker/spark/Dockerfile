# Unified Spark runtime for both master and worker containers.
#
# Spark 3.5.9 is the project baseline for local Docker, CI and later
# Iceberg/MLflow/lineage integration. The role-specific entrypoint keeps
# cluster mechanics out of application code.

ARG BASE_IMAGE=apache/spark:3.5.9-java17-python3
FROM ${BASE_IMAGE}

ARG SPARK_ROLE=worker
ARG ICEBERG_VERSION=1.11.0
ARG OPENLINEAGE_VERSION=1.53.0

USER root
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

RUN python3 - <<PY
from urllib.request import urlretrieve
v = "${ICEBERG_VERSION}"
base = "/opt/spark/jars"
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/{v}/iceberg-spark-runtime-3.5_2.12-{v}.jar", f"{base}/iceberg-spark-runtime-3.5_2.12-{v}.jar")
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/{v}/iceberg-aws-bundle-{v}.jar", f"{base}/iceberg-aws-bundle-{v}.jar")
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-gcp-bundle/{v}/iceberg-gcp-bundle-{v}.jar", f"{base}/iceberg-gcp-bundle-{v}.jar")
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-azure-bundle/{v}/iceberg-azure-bundle-{v}.jar", f"{base}/iceberg-azure-bundle-{v}.jar")

ol = "${OPENLINEAGE_VERSION}"
urlretrieve(
    f"https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/{ol}/openlineage-spark_2.12-{ol}.jar",
    f"{base}/openlineage-spark_2.12-{ol}.jar",
)
PY

# Structured Streaming needs the Kafka source and Avro (from_avro) connectors,
# which are NOT bundled in the apache/spark base image. Versions follow Spark 3.5.x.
# hadoop-aws + SDK bundle are required for s3a:// checkpoint locations on AWS.
RUN python3 - <<'PY'
from urllib.request import urlretrieve
base = "/opt/spark/jars"
M = "https://repo1.maven.org/maven2"
for path in [
    "org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.9/spark-sql-kafka-0-10_2.12-3.5.9.jar",
    "org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.9/spark-token-provider-kafka-0-10_2.12-3.5.9.jar",
    "org/apache/spark/spark-avro_2.12/3.5.9/spark-avro_2.12-3.5.9.jar",
    "org/apache/kafka/kafka-clients/3.4.1/kafka-clients-3.4.1.jar",
    "org/apache/commons/commons-pool2/2.11.1/commons-pool2-2.11.1.jar",
    "org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar",
    "com/amazonaws/aws-java-sdk-bundle/1.12.262/aws-java-sdk-bundle-1.12.262.jar",
]:
    urlretrieve(f"{M}/{path}", f"{base}/{path.rsplit('/', 1)[1]}")
PY

COPY core-site.xml /opt/spark/conf/core-site.xml
COPY --chown=spark:spark entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

ENV SPARK_ROLE=${SPARK_ROLE}
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/app

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
