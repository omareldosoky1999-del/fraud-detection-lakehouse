from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_rollback_script_checks_validated_run():
    path = ROOT / "scripts" / "rollback_ensemble.py"
    text = path.read_text(encoding="utf-8")
    assert "validation_status" in text
    assert "ensemble_run_id" in text
    assert "set_registered_model_alias" in text


def test_rollback_workflow_uses_production_environment():
    path = ROOT / ".github" / "workflows" / "model-rollback.yml"
    text = path.read_text(encoding="utf-8")
    assert "workflow_dispatch" in text
    assert "environment: production" in text
    assert "--target-run-id" in text
