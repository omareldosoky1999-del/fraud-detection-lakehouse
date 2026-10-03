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


def test_first_batch_has_empty_history_when_silver_table_is_missing(spark):
    """First batch on an empty Iceberg lakehouse: Silver is created by this very batch,
    so reading history must not fail with TABLE_OR_VIEW_NOT_FOUND."""
    from datetime import datetime

    from processing.streaming.bronze_silver_job import read_silver_history

    class _Catalog:
        def tableExists(self, _name):
            return False

    class _Spark:
        catalog = _Catalog()

        def table(self, name):  # must not be called
            raise AssertionError(f"table() called for {name}")

    silver = spark.createDataFrame(
        [(1, 10, datetime(2026, 9, 1, 10, 0, 0), "tok")],
        "transaction_id long, client_id long, event_time timestamp, batch_token string",
    )
    touched = silver.select("client_id").distinct()
    history = read_silver_history(
        _Spark(), silver, touched, datetime(2026, 8, 1),
        iceberg_catalog="polaris", silver_path="unused", legacy_hdfs_enabled=False,
    )
    assert history.count() == 0
    assert set(history.columns) == set(silver.columns)


class _SqlRecorder:
    def __init__(self, fail_first_with=None):
        self.statements = []
        self._fail = fail_first_with

    def sql(self, statement):
        self.statements.append(statement)
        if self._fail is not None and len(self.statements) == 1:
            raise self._fail


def test_delete_batch_rows_plain_delete_when_it_works():
    from processing.lakehouse.iceberg_tables import _delete_batch_rows

    spark = _SqlRecorder()
    _delete_batch_rows(spark, "polaris.bronze.transactions", "tok", "(ingest_date = DATE '2026-10-03')")
    assert len(spark.statements) == 1
    assert spark.statements[0].startswith("DELETE FROM polaris.bronze.transactions WHERE batch_token = 'tok'")


def test_delete_batch_rows_falls_back_to_merge_on_partial_file_error():
    from processing.lakehouse.iceberg_tables import _delete_batch_rows

    err = Exception("ValidationException: Cannot delete file where some, but not all, rows match filter x")
    spark = _SqlRecorder(fail_first_with=err)
    _delete_batch_rows(spark, "polaris.bronze.transactions", "tok", "(ingest_date = DATE '2026-10-03')")
    assert len(spark.statements) == 2
    assert spark.statements[1].startswith("MERGE INTO polaris.bronze.transactions AS t USING (SELECT 1 AS _k) AS s ON batch_token = 'tok'")
    assert spark.statements[1].endswith("WHEN MATCHED THEN DELETE")


def test_delete_batch_rows_reraises_unrelated_errors():
    from processing.lakehouse.iceberg_tables import _delete_batch_rows

    spark = _SqlRecorder(fail_first_with=RuntimeError("connection refused"))
    with pytest.raises(RuntimeError):
        _delete_batch_rows(spark, "t", "tok", "(ingest_date = DATE '2026-10-03')")
    assert len(spark.statements) == 1


def test_generated_sql_parses_with_the_spark_parser(spark):
    """Parse-only check (no Iceberg needed): catches syntax mistakes in generated SQL."""
    from processing.lakehouse.iceberg_tables import _delete_batch_rows

    spark_rec = _SqlRecorder(fail_first_with=Exception("some, but not all, rows match filter"))
    escaped = "tok'x".replace("'", "''")  # _replace_batch escapes before calling the helper
    _delete_batch_rows(spark_rec, "polaris.bronze.transactions", escaped, "(ingest_date = DATE '2026-10-03')")
    parser = spark._jsparkSession.sessionState().sqlParser()
    for statement in spark_rec.statements:
        parser.parsePlan(statement)  # raises ParseException on bad syntax
