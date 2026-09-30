"""Daily Gold-layer build.

Deliberately a plain BATCH DAG, scheduled once a day -- it does NOT try to
run the streaming bronze/silver job (that runs continuously as its own
long-lived Spark application, started separately; see
processing/streaming/bronze_silver_job.py and the spark-submit command in
the README). The old orchestration/dags/fraud_pipeline_dag.py tried to run a
`.awaitTermination()` streaming job as a daily Airflow task, so the task
would just hang forever -- that mistake is not repeated here.

How spark-submit is invoked from Airflow
-----------------------------------------
The Airflow container has no Spark installed. Rather than build a second,
separately-versioned PySpark install into the Airflow image (a real source
of "works in one container, not the other" bugs), this DAG shells out to
`docker exec spark-master spark-submit ...`: the docker socket is mounted
into the Airflow container (see docker-compose.yml, the airflow service) so
it can drive the SAME spark-master container everything else uses. This is a
pragmatic choice for a single-host academic deployment; a multi-host
production setup would use the Livy REST API or the SparkSubmitOperator
against a real cluster manager instead.
"""
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

APP_DIR = "/app"  # matches the repo bind-mount into spark-master (see docker-compose.yml: "..:/app")

default_args = {
    "owner": "fraud-platform",
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="fraud_gold_daily",
    description="Aggregate the previous day's fraud_decisions into the Gold layer.",
    default_args=default_args,
    schedule_interval="0 2 * * *",  # 02:00 daily, well after the day's stream has settled
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["fraud", "gold", "batch"],
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

