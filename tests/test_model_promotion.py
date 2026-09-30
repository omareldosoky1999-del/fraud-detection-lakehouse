from pathlib import Path
import importlib.util

ROOT = Path(__file__).parents[1]


def test_promotion_script_validates_candidate_release():
    path = ROOT / 'scripts' / 'promote_ensemble.py'
    spec = importlib.util.spec_from_file_location('promote_ensemble', path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    assert hasattr(module, 'promote')
    assert '--expected-run-id' in path.read_text(encoding='utf-8')


def test_registry_contract_separates_candidate_and_production():
    import yaml
    config = yaml.safe_load(
        (ROOT / 'config' / 'ml_models.yml').read_text(encoding='utf-8')
    )['registry']
    assert config['candidate_alias'] != config['production_alias']
    assert config['candidate_alias'] == 'candidate'
    assert config['production_alias'] == 'production'


def test_retraining_dag_promotes_candidate_only():
    dag = (ROOT / "orchestration" / "dags" / "fraud_ml_retraining_dag.py").read_text(
        encoding="utf-8"
    )
    assert "--promote-alias candidate" in dag
    assert "--promote-alias production" not in dag


def test_training_records_reproducibility_metadata():
    train = (ROOT / "processing" / "ml" / "train.py").read_text(encoding="utf-8")
    assert "_dataset_signature" in train
    assert "dataset_signature" in train
    assert "feature_contract" in train
    assert "GITHUB_SHA" in train
