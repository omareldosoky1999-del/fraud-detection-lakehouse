"""foreachBatch hands each micro-batch a cloned SparkSession.

A temp view registered on the batch DataFrame's session is invisible to the outer
session, which broke `CREATE TABLE ... AS SELECT * FROM <temp view>` in the stream
(TABLE_OR_VIEW_NOT_FOUND __iceberg_bronze_schema). Unit tests missed it because
they call the batch function with a single session.
"""
import pytest

pyspark = pytest.importorskip("pyspark")

from processing.common.spark_session import get_spark
from processing.lakehouse.iceberg_tables import _ensure_table


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-iceberg-session", enable_hive=False)
    yield s
    s.stop()


def test_temp_view_is_not_visible_across_sessions(spark):
    other = spark.newSession()
    other.range(1).createOrReplaceTempView("cross_session_view")
    with pytest.raises(Exception):
        spark.sql("SELECT * FROM cross_session_view").collect()
    assert other.sql("SELECT * FROM cross_session_view").count() == 1


class _Recorder:
    def __init__(self):
        self.sql_calls = []

    def sql(self, statement):
        self.sql_calls.append(statement)


class _FailingTable:
    def table(self, _name):
        raise RuntimeError("table does not exist")

    def sql(self, statement):  # the outer session must NOT be used for the CTAS
        raise AssertionError(f"outer session used: {statement}")


class _Frame:
    def __init__(self):
        self.sparkSession = _Recorder()
        self.views = []

    def createOrReplaceTempView(self, name):
        self.views.append(name)


def test_ensure_table_runs_ctas_on_the_frames_session():
    frame = _Frame()
    _ensure_table(_FailingTable(), "polaris.bronze.transactions", frame, ["event_date"], "__v")
    assert frame.views == ["__v"]
    assert len(frame.sparkSession.sql_calls) == 1
    assert "FROM __v WHERE 1=0" in frame.sparkSession.sql_calls[0]
