"""The streaming job must not start an embedded Hive metastore in Iceberg mode."""
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_stream_job_disables_hive_when_iceberg_enabled():
    source = (ROOT / "processing" / "streaming" / "bronze_silver_job.py").read_text(encoding="utf-8")
    assert 'get_spark("fraud-bronze-silver", enable_hive=args.no_iceberg)' in source


def test_make_does_not_run_spark_from_the_repo_mount():
    makefile = (ROOT / "Makefile").read_text(encoding="utf-8")
    assert "EXEC      := docker exec -w /tmp" in makefile
    assert "-w /app" not in makefile
