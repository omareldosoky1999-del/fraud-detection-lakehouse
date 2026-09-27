from pathlib import Path
import yaml


def test_compose_has_no_latest_and_persistent_state():
    path = Path(__file__).parents[1] / "docker" / "docker-compose.yml"
    text = path.read_text(encoding="utf-8")
    assert ":latest" not in text
    data = yaml.safe_load(text)
    assert "kafka_data:/var/lib/kafka/data" in text
    assert "hbase_data:/hbase-data" in text
    assert "zookeeper_data:/data" in text
    assert "kafka" not in data["services"]["kafka"].get("profiles", [])
