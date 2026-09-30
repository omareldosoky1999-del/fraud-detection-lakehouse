ARG MLFLOW_VERSION=3.16.0
FROM ghcr.io/mlflow/mlflow:v${MLFLOW_VERSION}

USER root
RUN pip install --no-cache-dir psycopg2-binary boto3

WORKDIR /mlflow
