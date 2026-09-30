"""Daily Iceberg-first Gold build.

The streaming application continuously writes Silver, features and fraud
decisions to Iceberg. This DAG validates the previous completed Silver
partition with Great Expectations, then builds Gold from the Iceberg decision
table.

HDFS/Hive tasks remain optional migration compatibility and are disabled unless
LEGACY_HDFS_ORCHESTRATION=true.
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

APP_DIR = "/app"

default_args = {
    "owner": "fraud-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_gold_daily",
    description="Validate Iceberg Silver and build previous-day Iceberg Gold aggregates.",
    default_args=default_args,
    schedule_interval="0 2 * * *",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["fraud", "gold", "batch", "iceberg", "great-expectations"],
) as dag:

    validate_silver_quality = BashOperator(
        task_id="validate_silver_quality",
        bash_command=(
            "docker exec spark-master python3 "
            f"{APP_DIR}/scripts/validate_silver_quality.py "
            "--date {{{{ ds }}}} "
            "--iceberg-catalog polaris "
            "--iceberg-table silver.transactions "
            f"--report /tmp/silver_validation_{{{{ ds }}}}.json"
        ),
    )

    build_gold = BashOperator(
        task_id="spark_submit_build_gold",
        bash_command=(
            "docker exec spark-master /opt/spark/bin/spark-submit "
            "--master spark://spark-master:7077 "
            f"{APP_DIR}/processing/batch/build_gold.py "
            "--date {{{{ ds }}}} "
            "--lookback-days 2"
        ),
    )

    repair_legacy_hive = BashOperator(
        task_id="legacy_hive_msck_repair",
        bash_command=(
            'if [ "$LEGACY_HDFS_ORCHESTRATION" != "true" ]; then '
            "echo 'Skipping legacy Hive repair: Iceberg is the system of record.'; "
            "exit 0; "
            "fi; "
            'docker exec hive beeline -u jdbc:hive2://localhost:10000 -e '
            '"MSCK REPAIR TABLE fraud.bronze_transactions; '
            'MSCK REPAIR TABLE fraud.quarantine_transactions; '
            'MSCK REPAIR TABLE fraud.gold_customer_daily_risk; '
            'MSCK REPAIR TABLE fraud.gold_daily_kpis; '
            'MSCK REPAIR TABLE fraud.fraud_decisions; '
            'MSCK REPAIR TABLE fraud.silver_transactions;"'
        ),
        env={
            "LEGACY_HDFS_ORCHESTRATION": "{{ var.value.get('LEGACY_HDFS_ORCHESTRATION', 'false') }}"
        },
    )

    legacy_rules_evaluation = BashOperator(
        task_id="legacy_rules_evaluation",
        bash_command=(
            'if [ "$LEGACY_HDFS_ORCHESTRATION" != "true" ]; then '
            "echo 'Skipping legacy rules evaluation: decisions are stored in Iceberg.'; "
            "exit 0; "
            "fi; "
            'docker exec spark-master bash -lc '
            '"rm -rf /tmp/decisions_local && '
            'hdfs dfs -get /warehouse/gold/fraud_decisions /tmp/decisions_local && '
            f"python3 {APP_DIR}/scripts/evaluate_rules.py "
            "--decisions /tmp/decisions_local "
            f"--labels {APP_DIR}/labels/ground_truth.csv "
            f'--out {APP_DIR}/reports/rules_report_{{{{ ds }}}}.txt"'
        ),
        env={
            "LEGACY_HDFS_ORCHESTRATION": "{{ var.value.get('LEGACY_HDFS_ORCHESTRATION', 'false') }}"
        },
    )

    validate_silver_quality >> build_gold >> repair_legacy_hive >> legacy_rules_evaluation
