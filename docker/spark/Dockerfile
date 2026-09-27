# Builds either the spark-master or spark-worker image (BASE_IMAGE build arg
# picks which) with the extra Python packages the pipeline needs that the
# base bde2020 image doesn't ship: happybase (HBase serving writes) and
# fastavro (only used by ingestion scripts if run from inside this
# container, e.g. for ad-hoc debugging).
#
# PySpark itself, Hadoop client libs, and Java are already in the base image
# -- do not reinstall them here.
ARG BASE_IMAGE=bde2020/spark-master:3.0.0-hadoop3.2
FROM ${BASE_IMAGE}

COPY requirements-spark.txt /tmp/requirements-spark.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements-spark.txt
