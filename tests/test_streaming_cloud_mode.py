from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_streaming_supports_iceberg_only_mode():
    source = (
        ROOT
        / "processing"
        / "streaming"
        / "bronze_silver_job.py"
    ).read_text(encoding="utf-8")

    assert "LEGACY_HDFS_ENABLED" in source
    assert "is_committed_iceberg" in source
    assert "mark_committed_iceberg" in source
    assert 'spark.table(' in source
    assert "TABLES['silver']" in source
    assert "legacy_hdfs_enabled" in source



def test_transaction_dedup_is_parameterized_for_iceberg_mode():
    text = (
        ROOT
        / "processing"
        / "streaming"
        / "bronze_silver_job.py"
    ).read_text(encoding="utf-8")
    assert "iceberg_enabled=iceberg_enabled" in text
    assert 'spark.table(f"{iceberg_catalog}.{TABLES[\'silver\']}")' in text
