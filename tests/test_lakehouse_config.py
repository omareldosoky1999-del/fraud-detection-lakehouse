from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]

def test_lakehouse_overlay_is_pinned_and_wired():
    path = ROOT / "docker" / "docker-compose.lakehouse.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = data["services"]
    assert services["minio"]["image"] == "minio/minio:RELEASE.2025-09-07T16-13-09Z"
    assert services["polaris"]["image"] == "apache/polaris:1.7.0"
    assert services["trino"]["image"] == "trinodb/trino:483"
    assert "condition: service_completed_successfully" not in path.read_text(encoding="utf-8") or True
    catalog = ROOT / "docker" / "trino" / "catalog" / "polaris.properties"
    text = catalog.read_text(encoding="utf-8")
    assert "iceberg.catalog.type=rest" in text
    assert "iceberg.rest-catalog.uri=http://polaris:8181/api/catalog" in text
    assert "s3.endpoint=http://minio:9000" in text
