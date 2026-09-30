from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from processing.common.spark_session import get_spark
from processing.streaming.decode import extract_confluent_metadata


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-decode", enable_hive=False)
    yield s
    s.stop()


def test_extract_confluent_schema_id_and_magic_byte(spark):
    # Confluent header: 0x00 magic + big-endian schema id 42 + opaque Avro payload.
    payload = bytes([0x00, 0x00, 0x00, 0x00, 0x2A, 0xAA, 0xBB])
    df = spark.createDataFrame(
        [(payload, 3, 91, datetime(2026, 9, 1, 10, 0, 0))],
        ["value", "partition", "offset", "timestamp"],
    )

    row = extract_confluent_metadata(df).collect()[0]
    assert row.kafka_confluent_magic == "00"
    assert row.kafka_schema_id == 42
    assert row.partition == 3
    assert row.offset == 91
