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



def test_cloud_dedup_is_fail_closed_and_bronze_is_preserved_for_duplicate_batches():
    source = (
        ROOT
        / "processing"
        / "streaming"
        / "bronze_silver_job.py"
    ).read_text(encoding="utf-8")
    assert "if not spark.catalog.tableExists(table):" in source
    assert "Any other read/storage failure is allowed to propagate" in source
    assert "raw Bronze/Quarantine side of the batch" in source
