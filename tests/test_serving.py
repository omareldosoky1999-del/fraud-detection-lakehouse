from pathlib import Path
import json
from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from pyspark.sql.types import (  # noqa: E402
    ArrayType, IntegerType, StringType, StructField, StructType, TimestampType)

from processing.common.spark_session import get_spark  # noqa: E402
from processing.serving.alerts_sink import build_alerts  # noqa: E402
from processing.serving.hbase_sink import ensure_table, to_hbase_row, write_partition  # noqa: E402

DECIDED_SCHEMA = StructType([
    StructField("transaction_id", IntegerType()),
    StructField("client_id", IntegerType()),
    StructField("amount_usd", pyspark.sql.types.DoubleType()),
    StructField("currency", StringType()),
    StructField("decision", StringType()),
    StructField("risk_score", IntegerType()),
    StructField("matched_rules", ArrayType(StringType())),
    StructField("event_time", TimestampType()),
])


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-serving", enable_hive=False)
    yield s
    s.stop()


def decided_row(**over):
    base = dict(transaction_id=1, client_id=10, amount_usd=100.0, currency="USD",
               decision="PASS", risk_score=0, matched_rules=[], event_time=datetime(2026, 9, 1))
    base.update(over)
    return base


def decided_df(spark, rows):
    return spark.createDataFrame(rows, schema=DECIDED_SCHEMA)


def test_build_alerts_excludes_pass(spark):
    df = decided_df(spark, [decided_row(transaction_id=1, decision="PASS"),
                            decided_row(transaction_id=2, client_id=11, decision="FLAG")])
    out = build_alerts(df, batch_token="b1").collect()
    assert len(out) == 1
    assert out[0].key == "b1:2"


def test_build_alerts_payload_shape(spark):
    df = decided_df(spark, [decided_row(
        transaction_id=2, client_id=11, decision="BLOCK", risk_score=45,
        matched_rules=["HIGH_AMOUNT"])])
    val = json.loads(build_alerts(df, batch_token="b1").collect()[0].value)
    assert val == {
        "alert_id": "b1:2", "batch_token": "b1",
        "transaction_id": 2, "client_id": 11, "amount_usd": 100.0, "currency": "USD",
        "decision": "BLOCK", "risk_score": 45, "matched_rules": ["HIGH_AMOUNT"],
        "event_time": "2026-09-01T00:00:00.000Z",
    }


def test_build_alerts_empty_when_all_pass(spark):
    df = decided_df(spark, [decided_row(decision="PASS")])
    assert build_alerts(df, batch_token="b1").count() == 0


# ---- HBase sink (fake happybase-style connection) --------------------------

class _FakeBatch:
    def __init__(self, store):
        self.store = store

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def put(self, key, data):
        self.store[key] = data


class _FakeTable:
    def __init__(self, store):
        self.store = store

    def batch(self, batch_size=500):
        return _FakeBatch(self.store)


class _FakeConnection:
    def __init__(self, fail=False):
        self.store, self.closed, self.fail = {}, False, fail

    def table(self, name):
        if self.fail:
            raise ConnectionError("hbase down")
        return _FakeTable(self.store)

    def close(self):
        self.closed = True


def test_to_hbase_row_shape():
    row = to_hbase_row(decided_row(transaction_id=5, decision="FLAG", risk_score=20,
                                   matched_rules=["VELOCITY"]))
    assert row["cf:last_transaction_id"] == "5"
    assert row["cf:last_decision"] == "FLAG"
    assert row["cf:last_matched_rules"] == "VELOCITY"


def test_write_partition_puts_latest_state_per_client():
    conn = _FakeConnection()
    write_partition([decided_row(transaction_id=1, client_id=10, decision="PASS"),
                     decided_row(transaction_id=2, client_id=10, decision="BLOCK")], lambda: conn)
    # same client twice -> last put wins (that's the intended "current state" semantic)
    assert conn.store[b"10"]["cf:last_transaction_id"] == "2"
    assert conn.closed


def test_write_partition_empty_is_a_noop():
    conn = _FakeConnection()
    write_partition([], lambda: conn)
    assert conn.store == {} and not conn.closed  # connection never opened for empty input


def test_write_partition_swallows_connection_errors():
    conn = _FakeConnection(fail=True)
    write_partition([decided_row()], lambda: conn)  # must not raise


class _AdminConnection:
    def __init__(self):
        self._tables = set()
        self.created = []

    def tables(self):
        return list(self._tables)

    def create_namespace(self, name):
        return None

    def create_table(self, name, families):
        self._tables.add(name.encode())
        self.created.append((name, families))


def test_ensure_table_creates_serving_table_once():
    # NOTE: happybase's own API takes column-family names as plain str, not
    # bytes (see https://happybase.readthedocs.io/en/latest/api.html --
    # `families = {'cf1': dict(...), ...}`), which is what ensure_table()
    # actually passes to create_table(). This duplicates
    # tests/test_hbase_sink_unit.py::test_ensure_table_is_idempotent, which
    # already covers this correctly; kept here too (same expectation) so a
    # future edit to either file can't silently drop the coverage.
    conn = _AdminConnection()
    ensure_table(conn)
    ensure_table(conn)
    assert conn.created == [("fraud:client_risk", {"cf": {}})]


def test_serving_api_does_not_leak_hbase_exception_by_default():
    source = (Path(__file__).parents[1] / "processing" / "serving" / "api.py").read_text(encoding="utf-8")
    assert 'os.getenv("API_DEBUG", "false")' in source
    assert 'response = {"error": "hbase_unavailable"}' in source
