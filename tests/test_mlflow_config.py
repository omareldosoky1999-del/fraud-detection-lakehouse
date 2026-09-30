from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]


def test_mlflow_overlay_is_pinned_and_wired():
    path = ROOT / "docker" / "docker-compose.mlflow.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = data["services"]

    assert services["rustfs"]["image"] == "rustfs/rustfs:1.0.0-rc.6"
    assert services["rustfs-init"]["profiles"] == ["mlops"]
    assert services["mlflow"]["build"]["args"]["MLFLOW_VERSION"] == "3.16.0"
    assert "mlflow-postgres" in services["mlflow"]["depends_on"]
    assert services["spark-master"]["environment"]["MLFLOW_TRACKING_URI"] == "http://mlflow:5000"
    assert services["spark-worker"]["environment"]["MLFLOW_MODEL_ALIAS"] == "production"
    mlflow_env = services["mlflow"]["environment"]
    assert services["mlflow"]["depends_on"]["rustfs-init"]["condition"] == "service_completed_successfully"
    assert "mlflow:5000" in mlflow_env["MLFLOW_SERVER_ALLOWED_HOSTS"]
    assert mlflow_env["MLFLOW_SERVER_CORS_ALLOWED_ORIGINS"] == "http://localhost:*"

    dockerfile = ROOT / "docker" / "mlflow" / "Dockerfile"
    text = dockerfile.read_text(encoding="utf-8")
    assert "ghcr.io/mlflow/mlflow:" in text
    assert "psycopg2-binary" in text
    assert "boto3" in text

    config = yaml.safe_load(
        (ROOT / "config" / "ml_models.yml").read_text(encoding="utf-8")
    )
    assert config["registry"]["candidate_alias"] == "candidate"
    assert config["registry"]["production_alias"] == "production"
    assert config["registry"]["decision_threshold"] == 0.70
    assert config["registry"]["min_validation_auc"] >= 0.5
    assert len(config["registry"]["members"]) == 3


def test_streaming_threshold_uses_registry_contract():
    source = (
        ROOT / "processing" / "streaming" / "bronze_silver_job.py"
    ).read_text(encoding="utf-8")
    assert "from processing.ml.registry import decision_threshold" in source
    assert 'default=decision_threshold()' in source

def test_ml_features_do_not_use_raw_identifier_magnitudes():
    source = (ROOT / "processing" / "ml" / "train.py").read_text(encoding="utf-8")
    start = source.index("NUMERIC = [")
    block = source[start:source.index("]", start) + 1]
    assert '"client_id"' not in block
    assert '"card_id"' not in block
    assert '"device_id"' not in block


def test_training_contract_rejects_direct_production_promotion():
    text = (ROOT / "processing" / "ml" / "train.py").read_text(encoding="utf-8")
    assert "Training cannot promote directly to the production alias" in text
