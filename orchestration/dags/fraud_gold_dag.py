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
from airflow.operators.python import PythonOperator

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

    def _check_silver_partition_exists(**context):
        """Fail fast with a clear message instead of silently building an
        empty Gold partition if the streaming job produced nothing for the
        day (e.g. it was down). Uses the HDFS WebHDFS HTTP API so no extra
        Python HDFS client dependency is needed in the Airflow image.
        """
        import os

        import requests

        # IMPORTANT: do NOT subtract another day here. For a daily
        # schedule_interval with catchup=False, Airflow's `ds`/`logical_date`
        # for the DagRun that actually EXECUTES at 02:00 on day N is already
        # day N-1 (the run fires at the END of the day-N-1 -> day-N interval).
        # I.e. `ds` already means "yesterday" relative to the real-world run
        # day -- exactly the completed day whose Silver partition we want.
        # Subtracting again here would target day N-2 and permanently lag the
        # Gold layer by one extra day. See:
        # https://airflow.apache.org/docs/apache-airflow/stable/templates-ref.html
        run_date = context.get("ds")
        if not run_date:
            # Airflow exposes macros through the template context in normal
            # execution; keep this callable usable in a direct unit test.
            execution_date = context.get("logical_date")
            run_date = execution_date.date().isoformat()
        namenode = os.getenv("HDFS_WEBHDFS_URL", "http://namenode:9870")
        path = f"/warehouse/silver/transactions/event_date={run_date}"
        url = f"{namenode}/webhdfs/v1{path}?op=GETFILESTATUS"
        resp = requests.get(url, timeout=10)
        if resp.status_code != 200:
            raise RuntimeError(
                f"No Silver partition found for {run_date} at {path} "
                f"(webhdfs status {resp.status_code}) -- is the streaming job running?")

    check_silver = PythonOperator(
        task_id="check_silver_partition_exists",
        python_callable=_check_silver_partition_exists,
    )

    build_gold = BashOperator(
        task_id="spark_submit_build_gold",
        bash_command=(
            "docker exec spark-master /spark/bin/spark-submit "
            f"--master spark://spark-master:7077 "
            f"{APP_DIR}/processing/batch/build_gold.py --date {{{{ ds }}}} --lookback-days 2"  # ds already = yesterday, see note above
        ),
    )

    repair_gold_tables = BashOperator(
        task_id="hive_msck_repair_gold_tables",
        bash_command=(
            'docker exec hive beeline -u jdbc:hive2://localhost:10000 -e '
            '"MSCK REPAIR TABLE fraud.bronze_transactions; '
            'MSCK REPAIR TABLE fraud.quarantine_transactions; '
            'MSCK REPAIR TABLE fraud.gold_customer_daily_risk; '
            'MSCK REPAIR TABLE fraud.gold_daily_kpis; '
            'MSCK REPAIR TABLE fraud.fraud_decisions; '
            'MSCK REPAIR TABLE fraud.silver_transactions;"'
        ),
    )

    evaluate = BashOperator(
        task_id="evaluate_rules_report",
        bash_command=(
            "docker exec spark-master bash -c \""
            "rm -rf /tmp/decisions_local && "
            "hdfs dfs -get /warehouse/gold/fraud_decisions /tmp/decisions_local && "
            f"python3 {APP_DIR}/scripts/evaluate_rules.py "
            "--decisions /tmp/decisions_local "
            f"--labels {APP_DIR}/labels/ground_truth.csv "
            f"--out {APP_DIR}/reports/rules_report_{{{{ ds }}}}.txt\""
        ),
    )

    check_silver >> build_gold >> repair_gold_tables >> evaluate
