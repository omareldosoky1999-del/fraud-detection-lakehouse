from datetime import date, datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from processing.batch.build_gold import build_customer_daily_risk, build_daily_kpis  # noqa: E402
from processing.common.spark_session import get_spark  # noqa: E402


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-gold", enable_hive=False)
    yield s
    s.stop()


def decision_row(**over):
    base = dict(transaction_id=1, client_id=10, amount_usd=100.0, decision="PASS",
               risk_score=0, country_src="Egypt", device_id=1, event_time=datetime(2026, 9, 1))
    base.update(over)
    return base


def test_customer_daily_risk_counts_and_totals(spark):
    df = spark.createDataFrame([
        decision_row(transaction_id=1, client_id=10, amount_usd=100.0, decision="PASS"),
        decision_row(transaction_id=2, client_id=10, amount_usd=15000.0, decision="BLOCK", risk_score=45),
        decision_row(transaction_id=3, client_id=10, amount_usd=50.0, decision="FLAG", risk_score=20, device_id=2),
    ])
    out = build_customer_daily_risk(df).collect()[0]
    assert out.txn_count == 3
    assert out.total_amount_usd == pytest.approx(15150.0)
    assert out.flagged_count == 1
    assert out.blocked_count == 1
    assert out.max_risk_score == 45
    assert out.distinct_devices == 2


def test_daily_kpis_rates(spark):
    df = spark.createDataFrame([
        decision_row(transaction_id=1, decision="PASS"),
        decision_row(transaction_id=2, decision="PASS"),
        decision_row(transaction_id=3, decision="BLOCK", risk_score=45),
        decision_row(transaction_id=4, decision="FLAG", risk_score=20),
    ])
    out = build_daily_kpis(df).collect()[0]
    assert out.total_txns == 4
    assert out.blocked_rate == pytest.approx(0.25)
    assert out.flagged_rate == pytest.approx(0.25)


def test_daily_kpis_empty_day_does_not_crash(spark):
    df = spark.createDataFrame([decision_row()]).filter("transaction_id < 0")
    out = build_daily_kpis(df).collect()[0]
    assert out.total_txns == 0


def test_build_gold_contains_iceberg_dual_write():
    source = (
        ( __import__("pathlib").Path(__file__).parents[1] / "processing" / "batch" / "build_gold.py")
        .read_text(encoding="utf-8")
    )
    assert "ICEBERG_ENABLED" in source
    assert "write_gold_tables" in source
    assert 'customer_daily_risk=customer_risk' in source
    assert 'daily_kpis=kpis' in source


def test_build_gold_is_iceberg_first_and_hdfs_is_compatibility_path():
    source = (
        __import__("pathlib").Path(__file__).parents[1]
        / "processing"
        / "batch"
        / "build_gold.py"
    ).read_text(encoding="utf-8")
    assert 'spark.table(' in source
    assert 'TABLES["decisions"]' in source
    assert 'LEGACY_HDFS_GOLD_OUTPUT' in source
    assert 'write_gold_tables' in source
