from datetime import datetime
import hashlib

import pytest

pyspark = pytest.importorskip('pyspark', reason='pyspark not installed; see requirements-dev.txt')

from processing.common.spark_session import get_spark
from processing.streaming.batch_commit import build_batch_token


@pytest.fixture(scope='module')
def spark():
    s = get_spark('pytest-batch-commit', enable_hive=False)
    yield s
    s.stop()


def test_batch_token_is_deterministic_for_same_kafka_ranges(spark):
    rows = [
        (0, 100, datetime(2026, 9, 1, 10, 0, 0)),
        (0, 101, datetime(2026, 9, 1, 10, 0, 1)),
        (1, 200, datetime(2026, 9, 1, 10, 0, 2)),
    ]
    df1 = spark.createDataFrame(rows, ['kafka_partition', 'kafka_offset', 'kafka_timestamp'])
    df2 = spark.createDataFrame(list(reversed(rows)), ['kafka_partition', 'kafka_offset', 'kafka_timestamp'])
    assert build_batch_token(df1, 7) == build_batch_token(df2, 99)


def test_batch_token_changes_when_offset_range_changes(spark):
    a = spark.createDataFrame([(0, 100), (0, 101)], ['kafka_partition', 'kafka_offset'])
    b = spark.createDataFrame([(0, 100), (0, 102)], ['kafka_partition', 'kafka_offset'])
    assert build_batch_token(a, 1) != build_batch_token(b, 1)


def test_batch_token_fallback_is_stable_without_kafka_metadata(spark):
    df = spark.createDataFrame([(1,), (2,)], ['value'])
    assert build_batch_token(df, 11) == 'spark-11'



def test_iceberg_commit_contract_is_defined():
    source = (
        __import__("pathlib").Path(__file__).parents[1]
        / "processing"
        / "streaming"
        / "batch_commit.py"
    ).read_text(encoding="utf-8")

    assert "DEFAULT_ICEBERG_COMMIT_TABLE" in source
    assert "is_committed_iceberg" in source
    assert "mark_committed_iceberg" in source
    assert "control.streaming_batch_commits" in source
