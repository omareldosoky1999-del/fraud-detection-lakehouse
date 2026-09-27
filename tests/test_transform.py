from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from processing.common.spark_session import get_spark  # noqa: E402
from processing.streaming.transform import (  # noqa: E402
    add_dq_errors, dedup_batch, split_valid_invalid, to_silver)


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-transform", enable_hive=False)
    yield s
    s.stop()


def raw_row(**over):
    base = dict(Trans_id=1, Clt_id=10, Card_id=100, Dev_id=1000, Trans_amount=500,
               Trans_date="2026-09-01 01:00:00 PM", Trans_type="Withdrawal", Trans_status="Successful",
               Trans_destination="CIB", Dev_Ip_Location="Cairo", Trans_Ref_No="R1", Currency="EGP",
               Trans_Reason="Shopping", Dest_account_No=123, Country_Dest="Egypt", Country_Src="Egypt")
    base.update(over)
    return base


def test_valid_row_has_no_dq_errors(spark):
    df = spark.createDataFrame([raw_row()])
    out = add_dq_errors(df).collect()[0]
    assert out.dq_errors == []


@pytest.mark.parametrize("bad_field,bad_value,expected_error", [
    ("Trans_amount", -5, "invalid:Trans_amount<=0"),
    ("Trans_amount", 0, "invalid:Trans_amount<=0"),
    ("Trans_status", "PENDING", "invalid:Trans_status"),
    ("Trans_type", "POS", "invalid:Trans_type"),
    ("Currency", "GBP", "invalid:Currency"),
    ("Trans_date", "not-a-date", "invalid:Trans_date_unparseable"),
    ("Clt_id", None, "null:Clt_id"),
])
def test_each_dq_rule_catches_its_violation(spark, bad_field, bad_value, expected_error):
    # a companion valid row is included so Spark can infer the column type
    # even when the row under test has a None value.
    df = spark.createDataFrame([raw_row(Trans_id=99), raw_row(Trans_id=1, **{bad_field: bad_value})])
    out = add_dq_errors(df).filter("Trans_id = 1").collect()[0]
    assert expected_error in out.dq_errors


def test_split_valid_invalid(spark):
    df = spark.createDataFrame([raw_row(Trans_id=1), raw_row(Trans_id=2, Trans_amount=-1)])
    valid, invalid = split_valid_invalid(df)
    assert [r.Trans_id for r in valid.collect()] == [1]
    assert "dq_errors" not in valid.columns
    invalid_row = invalid.collect()[0]
    assert invalid_row.Trans_id == 2
    assert invalid_row.quarantined_at is not None


def test_dedup_batch_keeps_one_row_per_key(spark):
    df = spark.createDataFrame([raw_row(Trans_id=1), raw_row(Trans_id=1), raw_row(Trans_id=2)])
    out = dedup_batch(df)
    assert out.count() == 2
    assert sorted(r.Trans_id for r in out.collect()) == [1, 2]


def test_to_silver_converts_currency_and_parses_time(spark):
    df = spark.createDataFrame([raw_row(Trans_amount=1000, Currency="EUR",
                                        Trans_date="2026-09-01 11:30:00 PM")])
    out = to_silver(df).collect()[0]
    assert out.amount_usd == pytest.approx(1000 * 1.08)
    assert out.event_time == datetime(2026, 9, 1, 23, 30, 0)
    assert str(out.event_date) == "2026-09-01"


def test_to_silver_unknown_currency_falls_back_to_1to1(spark):
    df = spark.createDataFrame([raw_row(Trans_amount=100, Currency="XYZ")])
    out = to_silver(df).collect()[0]
    assert out.amount_usd == pytest.approx(100.0)
