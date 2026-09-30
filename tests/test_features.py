from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark")

from processing.common.spark_session import get_spark
from processing.features.fraud_features import add_fraud_features


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-fraud-features", enable_hive=False)
    yield s
    s.stop()


def row(txn_id, client_id, when, amount, device, country="Egypt"):
    return {
        "transaction_id": txn_id,
        "client_id": client_id,
        "card_id": 1,
        "device_id": device,
        "fx_rate": 1.0,
        "amount_usd": float(amount),
        "currency": "USD",
        "txn_type": "Withdrawal",
        "status": "Successful",
        "destination_bank": "CIB",
        "device_location": "Cairo",
        "ref_no": f"R{txn_id}",
        "reason": "Shopping",
        "dest_account_no": 1,
        "country_src": country,
        "country_dest": "Egypt",
        "event_time": when,
        "event_date": when.date(),
        "ingest_time": when,
    }


def test_features_are_causal_and_point_in_time(spark):
    t0 = datetime(2026, 9, 1, 10, 0, 0)
    data = [
        row(1, 10, t0, 100, 1),
        row(2, 10, t0 + timedelta(seconds=30), 200, 1),
        row(3, 10, t0 + timedelta(seconds=120), 300, 2, country="France"),
        row(4, 10, t0 + timedelta(hours=2), 400, 2, country="France"),
    ]

    out = add_fraud_features(spark.createDataFrame(data)).orderBy("transaction_id")
    rows = out.select(
        "transaction_id",
        "txn_count_5m",
        "amount_sum_1h",
        "avg_amount_prior_30",
        "seconds_since_prev_txn",
        "device_new_30d",
        "country_changed",
        "amount_to_prior_avg",
    ).collect()

    assert rows[0].txn_count_5m == 1.0
    assert rows[0].avg_amount_prior_30 == 0.0
    assert rows[1].txn_count_5m == 2.0
    assert rows[1].amount_sum_1h == 300.0
    assert rows[1].avg_amount_prior_30 == 100.0
    assert rows[2].device_new_30d == 1.0
    assert rows[2].country_changed == 1.0
    assert rows[2].amount_to_prior_avg == pytest.approx(2.0)
    assert rows[3].txn_count_5m == 1.0
    assert rows[3].amount_sum_1h == 400.0
    assert rows[3].seconds_since_prev_txn == pytest.approx(120 * 60 - 120, abs=1e-6) is not None
