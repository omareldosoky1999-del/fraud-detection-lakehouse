"""Docker-free end-to-end check of the fraud decision path.

train (MLflow, SQLite backend) -> promote to production -> load the registered
ensemble -> run the real ``process_batch`` over generated transactions ->
assert invariants. Needs only the Python dev requirements and Java.

    python scripts/local_e2e.py [--n 5000] [--events 1500]

It deliberately does NOT cover Kafka, Schema Registry, Iceberg/Polaris, HBase or
Airflow; those are covered by the Docker full-stack workflow.
"""
from __future__ import annotations

import argparse
import os
import re
import shutil
import subprocess
import sys
import tempfile
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _run(cmd: list[str], env: dict[str, str], cwd: Path) -> str:
    proc = subprocess.run(cmd, cwd=cwd, env=env, text=True, capture_output=True)
    if proc.returncode != 0:
        sys.stderr.write(proc.stdout[-3000:] + proc.stderr[-3000:])
        raise SystemExit(f"command failed ({proc.returncode}): {' '.join(cmd)}")
    return proc.stdout + proc.stderr


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=5000, help="training events")
    ap.add_argument("--events", type=int, default=1500, help="events to score")
    ap.add_argument("--min-recall", type=float, default=0.5,
                    help="minimum share of injected fraud ending as FLAG/BLOCK")
    args = ap.parse_args(argv)

    work = Path(tempfile.mkdtemp(prefix="fraud-local-e2e-"))
    # MLflow's SQLite backend stores artifacts under ./mlruns; remove what we create.
    mlruns = ROOT / "mlruns"
    mlruns_existed = mlruns.exists()
    tracking = f"sqlite:///{work / 'mlflow.db'}"
    env = {**os.environ, "PYTHONPATH": str(ROOT), "MLFLOW_TRACKING_URI": tracking}
    py = sys.executable

    print(f"[1/4] training ({args.n} events) -> {tracking}")
    # Run from a scratch directory (not the repo root) to prove nothing depends on the cwd.
    out = _run([py, str(ROOT / "processing/ml/train.py"), "--n", str(args.n), "--tracking-uri", tracking,
                "--promote-alias", "candidate", "--ml-threshold", "0.70",
                "--model-dir", str(work / "models")], env, work)
    match = re.findall(r"\[MLFLOW\] run_id=([0-9a-f]+)", out)
    if not match:
        raise SystemExit("training finished but printed no MLflow run_id")
    run_id = match[-1]

    print(f"[2/4] promoting run {run_id} to production")
    _run([py, str(ROOT / "scripts/promote_ensemble.py"), "--tracking-uri", tracking,
          "--expected-run-id", run_id], env, work)

    os.environ["MLFLOW_TRACKING_URI"] = tracking
    from pyspark.sql import functions as F

    from ingestion.generator.transaction_generator import generate
    from processing.common.spark_session import get_spark
    from processing.ml.train import DATA_DIR
    from processing.streaming.bronze_silver_job import build_batch_processor

    print("[3/4] scoring generated transactions with the registered ensemble")
    spark = get_spark("local-e2e", enable_hive=False)
    try:
        events = generate(args.events, seed=11, start=datetime(2026, 9, 1), data_dir=DATA_DIR)
        rows = []
        for i, e in enumerate(events):
            r = dict(e.rec)
            r.update(kafka_partition=0, kafka_offset=i, kafka_timestamp=datetime(2026, 9, 1, 10, 0, 0))
            rows.append(r)
        paths = {k: str(work / k) for k in ["bronze", "silver", "quarantine", "decisions", "commits"]}
        process_batch = build_batch_processor(
            spark, bronze_path=paths["bronze"], silver_path=paths["silver"],
            quarantine_path=paths["quarantine"], decisions_path=paths["decisions"],
            commit_root=paths["commits"], hbase_host=None, hbase_port=None,
            kafka_bootstrap=None, alerts_topic=None,
            ml_required=True,  # fail loudly if the registered models cannot be loaded
        )
        size = 500
        for start in range(0, len(rows), size):
            process_batch(spark.createDataFrame(rows[start:start + size]), start // size)

        print("[4/4] checking invariants")
        decisions = spark.read.parquet(paths["decisions"])
        labels = spark.createDataFrame(
            [(int(e.rec["Trans_id"]), int(e.is_fraud)) for e in events],
            "transaction_id long, is_fraud int")
        joined = decisions.join(labels, "transaction_id")

        total, distinct = decisions.count(), decisions.select("transaction_id").distinct().count()
        quarantined = (spark.read.parquet(paths["quarantine"]).count()
                       if Path(paths["quarantine"]).exists() else 0)
        fraud = joined.filter("is_fraud = 1")
        caught = fraud.filter(F.col("decision").isin("FLAG", "BLOCK")).count()
        fraud_total = fraud.count()
        recall = caught / fraud_total if fraud_total else 0.0
        fp = joined.filter("is_fraud = 0 AND decision != 'PASS'").count()
        legit = joined.filter("is_fraud = 0").count()

        print(f"  decisions={total}/{len(events)} distinct={distinct} quarantined={quarantined}")
        print(f"  fraud caught={caught}/{fraud_total} (recall {recall:.2%}); "
              f"legit flagged={fp}/{legit} ({fp / max(legit, 1):.2%})")
        print("  NOTE: synthetic data from the same generator the models trained on; "
              "this validates plumbing, not real-world accuracy.")

        problems = []
        if total != len(events):
            problems.append(f"expected {len(events)} decisions, got {total}")
        if distinct != total:
            problems.append("duplicate transaction_id rows in decisions")
        if quarantined:
            problems.append(f"{quarantined} generated rows were quarantined")
        if "ml_probability" not in decisions.columns:
            problems.append("ml_probability column missing (ML scoring did not run)")
        if recall < args.min_recall:
            problems.append(f"fraud recall {recall:.2%} < {args.min_recall:.0%}")
        if problems:
            print("FAILED:\n  - " + "\n  - ".join(problems))
            return 1
        print("OK")
        return 0
    finally:
        spark.stop()
        shutil.rmtree(work, ignore_errors=True)
        if not mlruns_existed:
            shutil.rmtree(mlruns, ignore_errors=True)


if __name__ == "__main__":
    raise SystemExit(main())
