# Unified Spark runtime for both master and worker containers.
#
# Spark 3.5.9 is the project baseline for local Docker, CI and later
# Iceberg/MLflow/lineage integration. The role-specific entrypoint keeps
# cluster mechanics out of application code.

ARG BASE_IMAGE=apache/spark:3.5.9-java17-python3
FROM ${BASE_IMAGE}

ARG SPARK_ROLE=worker

USER root
COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt

COPY --chown=spark:spark entrypoint.sh /opt/spark/entrypoint.sh
RUN chmod +x /opt/spark/entrypoint.sh

ENV SPARK_ROLE=${SPARK_ROLE}
ENV PYSPARK_PYTHON=python3
ENV PYTHONPATH=/app

USER spark

ENTRYPOINT ["/opt/spark/entrypoint.sh"]
