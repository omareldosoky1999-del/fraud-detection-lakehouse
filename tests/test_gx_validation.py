from datetime import datetime

import pytest

pyspark = pytest.importorskip("pyspark")

from processing.common.spark_session import get_spark
from processing.quality.gx_validation import assert_silver_dataframe


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-gx-quality", enable_hive=False)
    yield s
    s.stop()


def row(txn_id=1, amount=100.0, currency="USD", country="Egypt"):
    return {
        "transaction_id": txn_id,
        "client_id": 10,
        "card_id": 1,
        "device_id": 1,
        "fx_rate": 1.0,
        "amount_usd": amount,
        "currency": currency,
        "txn_type": "Withdrawal",
        "status": "Successful",
        "destination_bank": "CIB",
        "device_location": "Cairo",
        "ref_no": f"R{txn_id}",
        "reason": "Shopping",
        "dest_account_no": 1,
        "country_src": country,
        "country_dest": "Egypt",
        "event_time": datetime(2026, 9, 1, 10, 0, 0),
        "event_date": datetime(2026, 9, 1).date(),
        "ingest_time": datetime(2026, 9, 1, 10, 0, 0),
    }


def test_valid_silver_dataframe_passes_gx(spark):
    df = spark.createDataFrame([row()])
    report = assert_silver_dataframe(df)
    assert report["success"] is True
    assert report["row_count"] == 1


def test_invalid_negative_amount_fails_gx(spark):
    df = spark.createDataFrame([row(amount=-1.0)])
    with pytest.raises(ValueError, match="Great Expectations Silver contract failed"):
        assert_silver_dataframe(df)

def test_silver_contract_is_declarative():
    from processing.quality.gx_validation import build_expectations, load_silver_contract

    contract = load_silver_contract()
    expectations = build_expectations(contract)

    assert "transaction_id" in contract["not_null_columns"]
    assert "amount_usd" == contract["non_negative"][0]["column"]
    assert len(expectations) == 6
