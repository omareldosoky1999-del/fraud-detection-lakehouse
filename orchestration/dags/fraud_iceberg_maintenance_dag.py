"""Weekly Iceberg maintenance DAG.
Runs only the lakehouse maintenance procedure; it never blocks the real-time fraud stream.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


with DAG(
    dag_id="fraud_iceberg_maintenance_weekly",
    description="Compact Iceberg data files and expire old snapshots.",
    schedule_interval="0 3 * * 0",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    default_args={
        "owner": "fraud-platform",
        "retries": 2,
        "retry_delay": timedelta(minutes=10),
    },
    tags=["fraud", "iceberg", "maintenance"],
) as dag:
    maintain = BashOperator(
        task_id="maintain_iceberg_tables",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            "/app/processing/lakehouse/maintenance.py "
            "--catalog \"$ICEBERG_CATALOG_NAME\" "
            "--retention-hours \"${ICEBERG_SNAPSHOT_RETENTION_HOURS:-168}\" "
            "--target-file-size-bytes \"${ICEBERG_TARGET_FILE_SIZE_BYTES:-536870912}\""
        ),
        env={
            "ICEBERG_CATALOG_NAME": "polaris",
            "ICEBERG_SNAPSHOT_RETENTION_HOURS": "168",
            "ICEBERG_TARGET_FILE_SIZE_BYTES": "536870912",
        },
    )
