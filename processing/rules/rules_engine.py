"""Configurable fraud rules engine.

Business thresholds are loaded from config/fraud_rules.yml (override with the
FRAUD_RULES_CONFIG environment variable) instead of being hard-coded. The
engine still operates on a plain Spark DataFrame containing enough history for
window calculations, which keeps it reusable in foreachBatch and in tests.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml
from pyspark.sql import DataFrame, Window, functions as F

_CONFIG_PATH = Path(os.getenv("FRAUD_RULES_CONFIG", Path(__file__).resolve().parents[2] / "config" / "fraud_rules.yml"))
with _CONFIG_PATH.open("r", encoding="utf-8") as _fh:
    _CFG = yaml.safe_load(_fh)

_RULE_CFG = _CFG["rules"]
_POLICY = _CFG["policy"]

RULES = {
    name: (cfg["severity"], cfg["description"])
    for name, cfg in _RULE_CFG.items()
}

HIGH_AMOUNT_USD = _RULE_CFG["HIGH_AMOUNT"]["threshold_usd"]
STRUCTURING_MIN_USD = _RULE_CFG["STRUCTURING"]["min_usd"]
STRUCTURING_MAX_USD = _RULE_CFG["STRUCTURING"]["max_usd"]
STRUCTURING_COUNT = _RULE_CFG["STRUCTURING"]["count"]
STRUCTURING_WINDOW_S = _RULE_CFG["STRUCTURING"]["window_seconds"]
VELOCITY_COUNT = _RULE_CFG["VELOCITY"]["count"]
VELOCITY_WINDOW_S = _RULE_CFG["VELOCITY"]["window_seconds"]
TRAVEL_WINDOW_S = _RULE_CFG["IMPOSSIBLE_TRAVEL"]["window_seconds"]
MULE_WINDOW_S = _RULE_CFG["MULE"]["window_seconds"]
MULE_RATIO = _RULE_CFG["MULE"]["ratio"]
MULE_MIN_DEPOSIT_USD = _RULE_CFG["MULE"]["min_deposit_usd"]
NEW_DEVICE_MIN_USD = _RULE_CFG["NEW_DEVICE_HIGH_VALUE"]["threshold_usd"]
NEW_DEVICE_LOOKBACK_S = _RULE_CFG["NEW_DEVICE_HIGH_VALUE"]["lookback_seconds"]

_RISK_WEIGHT = {k: int(v) for k, v in _POLICY["risk_weights"].items()}
HOME_COUNTRY = _POLICY["home_country"]
FAR_COUNTRIES = set(_POLICY["far_countries"])


def _client_time_window(rows_before=None, seconds_before=0, seconds_after=0):
    w = Window.partitionBy("client_id").orderBy(F.col("_t"))
    if rows_before is not None:
        return w.rowsBetween(-rows_before, 0)
    return w.rangeBetween(-seconds_before, seconds_after)


def apply_rules(history_df: DataFrame) -> DataFrame:
    """`history_df` must be Silver-shaped (see transform.to_silver) and should
    contain, for every client present in the current micro-batch, that
    client's transactions from the lookback window PLUS the new batch rows
    (duplicates on transaction_id are fine; window functions don't care and
    the caller already deduplicates before this point).

    Returns `history_df` with extra columns: `matched_rules` (array<string>),
    `risk_score` (int, 0-100) and `decision` (PASS/FLAG/BLOCK). Callers
    typically filter this down to just the current batch's transaction_ids
    afterwards -- rows brought in only to provide history are still present.
    """
    df = history_df.withColumn("_t", F.col("event_time").cast("long"))

    # ---- HIGH_AMOUNT: single-row check, no window needed ------------------
    df = df.withColumn("r_high_amount", F.col("amount_usd") >= HIGH_AMOUNT_USD)

    # ---- VELOCITY: count of this client's txns in the trailing 5 minutes --
    vel_w = _client_time_window(seconds_before=VELOCITY_WINDOW_S)
    df = df.withColumn("_velocity_count", F.count("*").over(vel_w))
    df = df.withColumn("r_velocity", F.col("_velocity_count") >= VELOCITY_COUNT)

    # ---- STRUCTURING: count of "just under the radar" txns in 1h ----------
    is_structuring_amount = F.col("amount_usd").between(STRUCTURING_MIN_USD, STRUCTURING_MAX_USD)
    struct_w = _client_time_window(seconds_before=STRUCTURING_WINDOW_S)
    df = df.withColumn(
        "_structuring_count",
        F.sum(F.when(is_structuring_amount, 1).otherwise(0)).over(struct_w),
    )
    df = df.withColumn("r_structuring", is_structuring_amount & (F.col("_structuring_count") >= STRUCTURING_COUNT))

    # ---- IMPOSSIBLE_TRAVEL: country changed from the previous txn, fast ---
    prev_w = Window.partitionBy("client_id").orderBy(F.col("_t"))
    df = (df
          .withColumn("_prev_country", F.lag("country_src").over(prev_w))
          .withColumn("_prev_t", F.lag("_t").over(prev_w)))
    df = df.withColumn(
        "r_impossible_travel",
        (F.col("_prev_country") == HOME_COUNTRY)
        & F.col("country_src").isin(*FAR_COUNTRIES)
        & ((F.col("_t") - F.col("_prev_t")) <= TRAVEL_WINDOW_S),
    )

    # ---- MULE: a large deposit largely withdrawn again soon after ---------
    # Flags BOTH legs (the deposit, by looking forward for a matching
    # withdrawal; the withdrawal, by looking backward for a matching
    # deposit), so a mule campaign's two transactions are both caught instead
    # of only the first. Gated on a minimum deposit size: small clients'
    # day-to-day deposit/withdrawal amounts land in a narrow personal band by
    # nature, which produced coincidental ratio matches with no fraud behind
    # them; real mule activity moves materially large sums.
    is_deposit = F.col("txn_type") == "Deposit"
    is_withdrawal = F.col("txn_type") == "Withdrawal"
    big_enough = F.col("amount_usd") >= MULE_MIN_DEPOSIT_USD

    # Keep the matching pair together inside the time window. Taking max(amount)
    # and max(time) as separate window aggregates can combine values from
    # different transactions and create false matches/misses. Spark SQL's
    # higher-order `exists` evaluates the pair condition against every row in
    # the bounded event-time window.
    fwd_w = Window.partitionBy("client_id").orderBy(F.col("_t")).rangeBetween(0, MULE_WINDOW_S)
    df = df.withColumn(
        "_fwd_txns",
        F.collect_list(F.struct(F.col("txn_type"), F.col("amount_usd"))).over(fwd_w),
    )
    r_mule_deposit_leg = (
        is_deposit & big_enough
        & F.expr(f"exists(_fwd_txns, x -> x.txn_type = 'Withdrawal' AND x.amount_usd >= amount_usd * {MULE_RATIO})")
    )

    back_w = Window.partitionBy("client_id").orderBy(F.col("_t")).rangeBetween(-MULE_WINDOW_S, 0)
    df = df.withColumn(
        "_back_txns",
        F.collect_list(F.struct(F.col("txn_type"), F.col("amount_usd"))).over(back_w),
    )
    r_mule_withdrawal_leg = (
        is_withdrawal
        & F.expr(f"exists(_back_txns, x -> x.txn_type = 'Deposit' AND x.amount_usd >= {MULE_MIN_DEPOSIT_USD} AND amount_usd >= x.amount_usd * {MULE_RATIO})")
    )
    df = df.withColumn("r_mule", r_mule_deposit_leg | r_mule_withdrawal_leg)

    # ---- NEW_DEVICE_HIGH_VALUE: device unseen by this client in 30d -------
    # Requires the client to already HAVE history (rowsBetween excludes the
    # current row): a brand-new customer's very first transaction is not
    # "a new device", it is just onboarding, and must not be flagged.
    hist_w = Window.partitionBy("client_id").orderBy(F.col("_t")).rowsBetween(Window.unboundedPreceding, -1)
    seen_w = Window.partitionBy("client_id").orderBy(F.col("_t")).rangeBetween(-NEW_DEVICE_LOOKBACK_S, -1)
    df = df.withColumn("_prior_txn_count", F.coalesce(F.count("*").over(hist_w), F.lit(0)))
    df = df.withColumn("_device_seen_before", F.array_contains(
        F.collect_set("device_id").over(seen_w), F.col("device_id")))
    df = df.withColumn(
        "r_new_device_high_value",
        (F.col("_prior_txn_count") > 0)
        & (~F.coalesce(F.col("_device_seen_before"), F.lit(False)))
        & (F.col("amount_usd") >= NEW_DEVICE_MIN_USD),
    )

    rule_cols = {
        "HIGH_AMOUNT": "r_high_amount",
        "STRUCTURING": "r_structuring",
        "IMPOSSIBLE_TRAVEL": "r_impossible_travel",
        "MULE": "r_mule",
        "VELOCITY": "r_velocity",
        "NEW_DEVICE_HIGH_VALUE": "r_new_device_high_value",
    }
    matched = F.array([F.when(F.col(c), F.lit(name)) for name, c in rule_cols.items()])
    df = df.withColumn("matched_rules", F.array_except(matched, F.array(F.lit(None).cast("string"))))

    risk_expr = F.lit(0)
    for name, c in rule_cols.items():
        risk_expr = risk_expr + F.when(F.col(c), F.lit(_RISK_WEIGHT[RULES[name][0]])).otherwise(0)
    df = df.withColumn("risk_score", F.least(risk_expr, F.lit(100)))

    has_high = F.array_max(F.array([
        F.when(F.col(c) & F.lit(RULES[name][0] == "HIGH"), F.lit(1)).otherwise(0)
        for name, c in rule_cols.items()
    ])) == 1
    df = df.withColumn(
        "decision",
        F.when(has_high, "BLOCK")
        .when(F.size("matched_rules") > 0, "FLAG")
        .otherwise("PASS"),
    )

    drop_cols = ["_t", "_prev_country", "_prev_t",
                "_fwd_txns", "_back_txns",
                "_velocity_count", "_structuring_count", "_prior_txn_count",
                "_device_seen_before"] + list(rule_cols.values())
    return df.drop(*drop_cols)
