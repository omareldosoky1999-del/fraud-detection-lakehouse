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


def test_iceberg_partition_contract_avoids_micro_batch_partition_explosion():
    from processing.lakehouse.iceberg_tables import PARTITIONS

    assert PARTITIONS["bronze"] == ["ingest_date"]
    assert PARTITIONS["quarantine"] == ["quarantine_date"]
    assert PARTITIONS["silver"] == ["event_date"]
    assert PARTITIONS["features"] == ["event_date"]
    assert PARTITIONS["decisions"] == ["event_date"]

    for columns in PARTITIONS.values():
        assert "batch_token" not in columns


def test_spark_session_does_not_ship_polaris_secret_defaults():
    source = (
        ROOT / "processing" / "common" / "spark_session.py"
    ).read_text(encoding="utf-8")
    assert 'os.getenv("POLARIS_CREDENTIAL", "root:s3cr3t")' not in source
    assert 'os.environ["POLARIS_CREDENTIAL"]' in source


def test_trino_catalog_is_environment_driven():
    path = ROOT / "docker" / "trino" / "catalog" / "polaris.properties"
    text = path.read_text(encoding="utf-8")
    assert "${ENV:POLARIS_URI}" in text
    assert "${ENV:POLARIS_CREDENTIAL}" in text
    assert "${ENV:S3_ENDPOINT}" in text
    assert "${ENV:AWS_ACCESS_KEY_ID}" in text
    assert "rustfsadmin" not in text
    assert "root:s3cr3t" not in text


def test_polaris_setup_has_admin_oauth_credentials():
    data = yaml.safe_load(
        (Path(__file__).parents[1] / "docker" / "docker-compose.lakehouse.yml").read_text(encoding="utf-8")
    )
    env = data["services"]["polaris-setup"]["environment"]
    assert env["CLIENT_ID"] == "root"
    assert env["CLIENT_SECRET"] == "s3cr3t"
