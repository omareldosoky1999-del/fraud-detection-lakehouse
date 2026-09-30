"""Manual MLOps retraining DAG.

The DAG intentionally defaults to manual trigger. The current trainer uses the
project's labeled synthetic training source; automatic scheduled promotion is
not enabled until a governed labeled-data source is connected.
"""
from __future__ import annotations

from datetime import datetime, timedelta

import requests
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator

APP_DIR = "/app"
MLFLOW_HEALTH = "http://mlflow:5000/health"

default_args = {
    "owner": "fraud-platform",
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_ml_retraining_manual",
    description="Train and validate the Spark MLlib fraud ensemble through MLflow.",
    schedule_interval=None,
    start_date=datetime(2026, 9, 1),
    catchup=False,
    max_active_runs=1,
    tags=["fraud", "mlops", "mlflow"],
) as dag:

    def _check_mlflow():
        response = requests.get(MLFLOW_HEALTH, timeout=10)
        response.raise_for_status()
        if response.text.strip().lower() != "ok":
            raise RuntimeError(f"Unexpected MLflow health response: {response.text!r}")

    check_mlflow = PythonOperator(
        task_id="check_mlflow",
        python_callable=_check_mlflow,
    )

    train_and_promote = BashOperator(
        task_id="train_validate_register_promote",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"{APP_DIR}/processing/ml/train.py "
            "--n 20000 "
            "--tracking-uri http://mlflow:5000 "
            "--promote-alias production"
        ),
    )

    check_mlflow >> train_and_promote
