from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_recovery_checker_exists_and_uses_spark_reads():
    path = ROOT / "scripts" / "recovery_check.py"
    text = path.read_text(encoding="utf-8")
    assert "spark.read.parquet" in text
    assert "distinct()" in text
    assert "minimum_first" in text
    assert "minimum_second" in text


def test_recovery_workflow_does_not_getmerge_parquet():
    path = ROOT / ".github" / "workflows" / "recovery-e2e.yml"
    text = path.read_text(encoding="utf-8")
    assert "getmerge" not in text
    assert "recovery_check.py" in text
