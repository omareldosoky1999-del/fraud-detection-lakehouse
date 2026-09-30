from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1]

def test_lakehouse_overlay_is_pinned_and_wired():
    path = ROOT / "docker" / "docker-compose.lakehouse.yml"
    data = yaml.safe_load(path.read_text(encoding="utf-8"))
    services = data["services"]
    assert services["rustfs"]["image"] == "rustfs/rustfs:1.0.0-rc.6"
    assert services["polaris"]["image"] == "apache/polaris:1.7.0"
    assert services["trino"]["image"] == "trinodb/trino:483"
    catalog = ROOT / "docker" / "trino" / "catalog" / "polaris.properties"
    text = catalog.read_text(encoding="utf-8")
    assert "iceberg.catalog.type=rest" in text
    assert "iceberg.rest-catalog.uri=http://polaris:8181/api/catalog" in text
    assert "fs.native-s3.enabled=true" in text
    assert "fs.s3.enabled=true" not in text
    assert "s3.endpoint=http://rustfs:9000" in text
    assert "s3.aws-access-key=rustfsadmin" in text
    assert "s3.aws-secret-key=rustfsadmin" in text
    assert services["spark-master"]["environment"]["ICEBERG_ENABLED"] == "true"
    from processing.lakehouse.iceberg_tables import TABLES
    assert TABLES["features"] == "features.transaction_features"
    assert services["spark-worker"]["environment"]["ICEBERG_ENABLED"] == "true"
