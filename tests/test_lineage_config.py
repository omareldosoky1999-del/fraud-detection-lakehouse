from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]


def test_lineage_overlay_is_pinned_and_enabled():
    path = ROOT / "docker" / "docker-compose.lineage.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = data["services"]

    assert services["marquez"]["image"] == "marquezproject/marquez:0.51.1"
    assert services["marquez-web"]["image"] == "marquezproject/marquez-web:0.51.1"
    assert services["marquez-db"]["image"] == "postgres:14"

    spark_env = services["spark-master"]["environment"]
    assert spark_env["OPENLINEAGE_ENABLED"] == "true"
    assert spark_env["OPENLINEAGE_URL"] == "http://marquez:5000"
    assert spark_env["OPENLINEAGE_ENDPOINT"] == "/api/v1/lineage"

    airflow_env = services["airflow"]["environment"]
    assert airflow_env["AIRFLOW__OPENLINEAGE__NAMESPACE"] == "fraud_detection"
    assert "http://marquez:5000" in airflow_env["AIRFLOW__OPENLINEAGE__TRANSPORT"]

    dockerfile = ROOT / "docker" / "spark" / "Dockerfile"
    docker_text = dockerfile.read_text(encoding="utf-8")
    assert "openlineage-spark_2.12" in docker_text
    assert "OPENLINEAGE_VERSION=1.53.0" in docker_text

    airflow_dockerfile = ROOT / "docker" / "airflow" / "Dockerfile"
    airflow_text = airflow_dockerfile.read_text(encoding="utf-8")
    assert "apache-airflow-providers-openlineage==1.14.0" in airflow_text
