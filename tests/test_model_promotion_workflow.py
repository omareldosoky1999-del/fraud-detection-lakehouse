from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_model_promotion_workflow_is_approval_gated():
    text = (ROOT / '.github' / 'workflows' / 'model-promotion.yml').read_text(encoding='utf-8')
    assert 'environment: production' in text
    assert 'expected_run_id:' in text
    assert 'secrets.MLFLOW_TRACKING_URI' in text
    assert 'scripts/promote_ensemble.py' in text
