from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from processing.common.spark_session import get_spark  # noqa: E402
from processing.rules import rules_engine as re_mod  # noqa: E402
from processing.rules.rules_engine import apply_rules  # noqa: E402

T0 = datetime(2026, 9, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-rules", enable_hive=False)
    yield s
    s.stop()


def row(id, client, t, usd, ttype="Withdrawal", country="Egypt", device=1):
    return dict(transaction_id=id, client_id=client, card_id=1, device_id=device,
               amount=usd, fx_rate=1.0, amount_usd=float(usd), currency="USD",
               txn_type=ttype, status="Successful", destination_bank="X",
               device_location="X", ref_no=f"R{id}", reason="X", dest_account_no=1,
               country_src=country, country_dest="Egypt", event_time=t, event_date=t.date())


def decide(spark, rows):
    df = spark.createDataFrame(rows)
    out = apply_rules(df).select("transaction_id", "matched_rules", "decision").collect()
    return {r.transaction_id: (r.matched_rules, r.decision) for r in out}


def test_high_amount_blocks(spark):
    res = decide(spark, [row(1, 1, T0, 15_000)])
    assert res[1] == (["HIGH_AMOUNT"], "BLOCK")


def test_below_threshold_passes(spark):
    res = decide(spark, [row(1, 1, T0, 3_000)])
    assert res[1] == ([], "PASS")


def test_velocity_flags_after_sixth_txn_in_5_minutes(spark):
    rows = [row(100 + i, 2, T0 + timedelta(seconds=i * 30), 20) for i in range(7)]
    res = decide(spark, rows)
    assert res[105][1] == "FLAG" and "VELOCITY" in res[105][0]  # 6th txn trips it
    assert res[100] == ([], "PASS")  # 1st txn alone must not


def test_structuring_blocks_on_third_under_threshold_txn(spark):
    rows = [row(200 + i, 3, T0 + timedelta(minutes=i * 10), 8_500) for i in range(3)]
    res = decide(spark, rows)
    assert res[202][1] == "BLOCK" and "STRUCTURING" in res[202][0]
    assert res[200] == ([], "PASS")


def test_impossible_travel_blocks_far_country_fast_followup(spark):
    rows = [row(300, 4, T0, 100, country="Egypt"),
           row(301, 4, T0 + timedelta(minutes=20), 150, country="USA")]
    res = decide(spark, rows)
    assert res[300] == ([], "PASS")  # the home anchor must not be flagged
    assert res[301][1] == "BLOCK" and "IMPOSSIBLE_TRAVEL" in res[301][0]


def test_impossible_travel_ignores_nearby_countries(spark):
    rows = [row(300, 4, T0, 100, country="Egypt"),
           row(302, 4, T0 + timedelta(minutes=20), 80, country="Jordan")]
    res = decide(spark, rows)
    assert res[302] == ([], "PASS")


def test_impossible_travel_ignores_slow_followup(spark):
    rows = [row(300, 4, T0, 100, country="Egypt"),
           row(303, 4, T0 + timedelta(hours=5), 150, country="USA")]
    res = decide(spark, rows)
    assert res[303] == ([], "PASS")


def test_mule_flags_both_legs(spark):
    rows = [row(400, 5, T0, 10_000, ttype="Deposit"),
           row(401, 5, T0 + timedelta(minutes=10), 9_200, ttype="Withdrawal")]
    res = decide(spark, rows)
    assert "MULE" in res[400][0]
    assert "MULE" in res[401][0]


def test_mule_ignores_small_coincidental_amounts(spark):
    rows = [row(800, 9, T0, 500, ttype="Deposit"),
           row(801, 9, T0 + timedelta(minutes=10), 480, ttype="Withdrawal")]
    res = decide(spark, rows)
    assert res[800] == ([], "PASS")
    assert res[801] == ([], "PASS")


def test_mule_ignores_small_withdrawal_ratio(spark):
    rows = [row(800, 9, T0, 6_000, ttype="Deposit"),
           row(801, 9, T0 + timedelta(minutes=10), 1_800, ttype="Withdrawal")]
    res = decide(spark, rows)
    assert res[800] == ([], "PASS")


def test_new_device_ignored_on_clients_first_ever_transaction(spark):
    res = decide(spark, [row(500, 6, T0, 6_000, device=999)])
    assert res[500] == ([], "PASS")


def test_new_device_flags_once_client_has_history(spark):
    rows = [row(500, 6, T0, 50, device=1),
           row(501, 6, T0 + timedelta(days=1), 6_000, device=42)]
    res = decide(spark, rows)
    assert res[501][1] == "FLAG" and "NEW_DEVICE_HIGH_VALUE" in res[501][0]


def test_known_device_not_flagged_even_if_high_value(spark):
    rows = [row(500, 6, T0, 50, device=42),
           row(501, 6, T0 + timedelta(days=1), 6_000, device=42)]
    res = decide(spark, rows)
    assert "NEW_DEVICE_HIGH_VALUE" not in res[501][0]


def test_multiple_high_rules_still_block_and_cap_risk_at_100(spark):
    res = decide(spark, [row(1, 1, T0, 60_000, device=999)])
    matched, decision = res[1]
    assert decision == "BLOCK"
    assert "HIGH_AMOUNT" in matched


def test_rules_metadata_matches_engine_columns():
    assert set(re_mod.RULES) == {"HIGH_AMOUNT", "STRUCTURING", "IMPOSSIBLE_TRAVEL",
                                 "MULE", "VELOCITY", "NEW_DEVICE_HIGH_VALUE"}
    assert all(sev in ("HIGH", "MEDIUM") for sev, _ in re_mod.RULES.values())


def test_mule_uses_time_window_not_row_count(spark):
    rows = [row(900, 12, T0, 6000, ttype="Deposit")]
    rows += [row(901 + i, 12, T0 + timedelta(seconds=20 + i), 20) for i in range(25)]
    rows.append(row(950, 12, T0 + timedelta(minutes=10), 5800, ttype="Withdrawal"))
    res = decide(spark, rows)
    assert "MULE" in res[900][0]
    assert "MULE" in res[950][0]
