from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]


def test_mlflow_overlay_is_pinned_and_wired():
    path = ROOT / "docker" / "docker-compose.mlflow.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = data["services"]

    assert services["mlflow"]["build"]["args"]["MLFLOW_VERSION"] == "3.16.0"
    assert "mlflow-postgres" in services["mlflow"]["depends_on"]
    assert services["spark-master"]["environment"]["MLFLOW_TRACKING_URI"] == "http://mlflow:5000"
    assert services["spark-worker"]["environment"]["MLFLOW_MODEL_ALIAS"] == "production"

    dockerfile = ROOT / "docker" / "mlflow" / "Dockerfile"
    text = dockerfile.read_text(encoding="utf-8")
    assert "ghcr.io/mlflow/mlflow:" in text
    assert "psycopg2-binary" in text
    assert "boto3" in text

    config = yaml.safe_load(
        (ROOT / "config" / "ml_models.yml").read_text(encoding="utf-8")
    )
    assert config["registry"]["alias"] == "production"
    assert len(config["registry"]["members"]) == 3
