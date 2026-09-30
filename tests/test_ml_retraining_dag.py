from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_ml_retraining_dag_is_manual_and_mlflow_gated():
    text = (ROOT / "orchestration" / "dags" / "fraud_ml_retraining_dag.py").read_text(
        encoding="utf-8"
    )
    assert 'schedule_interval=None' in text
    assert 'max_active_runs=1' in text
    assert 'http://mlflow:5000/health' in text
    assert '--promote-alias production' in text
    assert 'docker exec spark-master' in text
