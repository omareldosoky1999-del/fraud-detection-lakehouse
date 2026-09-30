ARG BASE_IMAGE=apache/spark:3.5.9-java17-python3
FROM ${BASE_IMAGE}

ARG ICEBERG_VERSION=1.11.0
ARG OPENLINEAGE_VERSION=1.53.0

USER root
COPY docker/spark/requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

RUN python3 - <<PY
from urllib.request import urlretrieve
iceberg = "${ICEBERG_VERSION}"
openlineage = "${OPENLINEAGE_VERSION}"
base = "/opt/spark/jars"
for artifact in [
    f"iceberg-spark-runtime-3.5_2.12-{iceberg}.jar",
    f"iceberg-aws-bundle-{iceberg}.jar",
    f"iceberg-gcp-bundle-{iceberg}.jar",
    f"iceberg-azure-bundle-{iceberg}.jar",
]:
    if artifact.startswith("iceberg-spark"):
        url = f"https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-spark-runtime-3.5_2.12/{iceberg}/{artifact}"
    else:
        name = artifact.replace(f"-{iceberg}.jar", "")
        url = f"https://repo1.maven.org/maven2/org/apache/iceberg/{name}/{iceberg}/{artifact}"
    urlretrieve(url, f"{base}/{artifact}")
ol_artifact = f"openlineage-spark_2.12-{openlineage}.jar"
urlretrieve(
    f"https://repo1.maven.org/maven2/io/openlineage/openlineage-spark_2.12/{openlineage}/{ol_artifact}",
    f"{base}/{ol_artifact}",
)
PY

COPY --chown=spark:spark common /app/common
COPY --chown=spark:spark config /app/config
COPY --chown=spark:spark ingestion /app/ingestion
COPY --chown=spark:spark processing /app/processing

ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/app

USER spark
