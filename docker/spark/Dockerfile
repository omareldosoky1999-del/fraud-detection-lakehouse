# Unified Spark runtime for both master and worker containers.
#
# Spark 3.5.9 is the project baseline for local Docker, CI and later
# Iceberg/MLflow/lineage integration. The role-specific entrypoint keeps
# cluster mechanics out of application code.

ARG BASE_IMAGE=apache/spark:3.5.9-java17-python3
FROM ${BASE_IMAGE}

ARG SPARK_ROLE=worker
ARG ICEBERG_VERSION=1.11.0

USER root
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

RUN python3 - <<PY
from urllib.request import urlretrieve
v = "${ICEBERG_VERSION}"
base = "/opt/spark/jars"
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/{v}/iceberg-spark-runtime-3.5_2.12-{v}.jar", f"{base}/iceberg-spark-runtime-3.5_2.12-{v}.jar")
urlretrieve(f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-aws-bundle/{v}/iceberg-aws-bundle-{v}.jar", f"{base}/iceberg-aws-bundle-{v}.jar")
PY

COPY --chown=spark:spark entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

ENV SPARK_ROLE=${SPARK_ROLE}
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/app

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
