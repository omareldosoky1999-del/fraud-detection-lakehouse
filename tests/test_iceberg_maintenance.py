from pathlib import Path
import importlib.util

ROOT = Path(__file__).parents[1]


def test_iceberg_maintenance_script_exists_and_is_importable():
    path = ROOT / "processing" / "lakehouse" / "maintenance.py"
    spec = importlib.util.spec_from_file_location("iceberg_maintenance", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    assert "bronze.transactions" in module.DEFAULT_TABLES
    assert "silver.transactions" in module.DEFAULT_TABLES
    assert module._sql_literal("a'b") == "'a''b'"


def test_weekly_iceberg_maintenance_dag_exists():
    path = ROOT / "orchestration" / "dags" / "fraud_iceberg_maintenance_dag.py"
    text = path.read_text(encoding="utf-8")
    assert "fraud_iceberg_maintenance_weekly" in text
    assert "processing/lakehouse/maintenance.py" in text
    assert "--retention-hours" in text
    assert "--target-file-size-bytes" in text
