from pathlib import Path
import yaml


def test_compose_has_no_latest_and_persistent_state():
    path = Path(__file__).parents[1] / "docker" / "docker-compose.yml"
    text = path.read_text(encoding="utf-8")
    assert ":latest" not in text
    assert "apache/spark:3.5.9-java17-python3" in text
    assert "bde2020/spark" not in text
    data = yaml.safe_load(text)
    assert "kafka_data:/var/lib/kafka/data" in text
    assert "hbase_data:/hbase-data" in text
    assert "zookeeper_data:/data" in text
    assert "kafka" not in data["services"]["kafka"].get("profiles", [])

    assert "alert-consumer" in data["services"]
    assert "alerts-data:/data" in text
    assert "fraud.alerts" in data["services"]["alert-consumer"]["command"]

def test_schema_registry_enforces_backward_compatibility():
    data = yaml.safe_load(
        (Path(__file__).parents[1] / "docker" / "docker-compose.yml").read_text(
            encoding="utf-8"
        )
    )
    env = data["services"]["schema-registry"]["environment"]
    assert env["SCHEMA_REGISTRY_AVRO_COMPATIBILITY_LEVEL"] == "BACKWARD"

def test_hbase_healthcheck_is_http_only():
    text = (Path(__file__).parents[1] / "docker" / "docker-compose.yml").read_text(
        encoding="utf-8"
    )
    assert 'curl -fsS http://localhost:16010/master-status' in text
    assert "hbase shell" not in text.split("hbase:")[1].split("hive:")[0]


def test_lakehouse_and_mlops_use_isolated_object_stores():
    import importlib.util

    module_path = Path(__file__).parents[1] / "scripts" / "validate_stack.py"
    spec = importlib.util.spec_from_file_location("validate_stack", module_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    services = module._merge_services(
        "docker/docker-compose.lakehouse.yml",
        "docker/docker-compose.mlflow.yml",
    )

    assert set(services["rustfs"]["profiles"]) == {"lakehouse"}
    assert set(services["mlflow-rustfs"]["profiles"]) == {"mlops"}
    assert set(services["rustfs-init"]["profiles"]) == {"lakehouse"}
    assert set(services["mlflow-rustfs-init"]["profiles"]) == {"mlops"}

    lineage = module._merge_services(
        "docker/docker-compose.yml",
        "docker/docker-compose.lineage.yml",
    )
    assert "marquez" in lineage
    assert "airflow" in lineage
