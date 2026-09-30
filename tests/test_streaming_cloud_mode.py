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
