"""Causal fraud features shared by offline training and streaming inference."""
from __future__ import annotations

from pyspark.sql import DataFrame, Window, functions as F


VELOCITY_WINDOW_SECONDS = 5 * 60
AMOUNT_SUM_WINDOW_SECONDS = 60 * 60
DEVICE_LOOKBACK_SECONDS = 30 * 24 * 60 * 60
PRIOR_TRANSACTION_LOOKBACK = 30


def add_fraud_features(df: DataFrame) -> DataFrame:
    """Add point-in-time features without using future transactions.

    Input must be the canonical Silver transaction shape. All temporal
    features are ordered by event time and therefore use only the current row
    or rows strictly before it.
    """
    work = df.withColumn("_feature_ts", F.col("event_time").cast("long"))

    trailing_5m = (
        Window.partitionBy("client_id")
        .orderBy(F.col("_feature_ts"))
        .rangeBetween(-VELOCITY_WINDOW_SECONDS, 0)
    )
    trailing_1h = (
        Window.partitionBy("client_id")
        .orderBy(F.col("_feature_ts"))
        .rangeBetween(-AMOUNT_SUM_WINDOW_SECONDS, 0)
    )
    prior_30_rows = (
        Window.partitionBy("client_id")
        .orderBy(F.col("event_time"), F.col("transaction_id"))
        .rowsBetween(-PRIOR_TRANSACTION_LOOKBACK, -1)
    )
    prior_all = (
        Window.partitionBy("client_id")
        .orderBy(F.col("event_time"), F.col("transaction_id"))
        .rowsBetween(Window.unboundedPreceding, -1)
    )
    prior_30d = (
        Window.partitionBy("client_id")
        .orderBy(F.col("_feature_ts"))
        .rangeBetween(-DEVICE_LOOKBACK_SECONDS, -1)
    )
    sequence_w = (
        Window.partitionBy("client_id")
        .orderBy(F.col("event_time"), F.col("transaction_id"))
    )

    amount = F.col("amount_usd").cast("double")

    work = (
        work
        .withColumn("txn_count_5m", F.count("*").over(trailing_5m).cast("double"))
        .withColumn("amount_sum_1h", F.sum(amount).over(trailing_1h))
        .withColumn("avg_amount_prior_30", F.avg(amount).over(prior_30_rows))
        .withColumn(
            "stddev_amount_prior_30",
            F.stddev_pop(amount).over(prior_30_rows),
        )
        .withColumn("_prior_txn_count", F.count("*").over(prior_all))
        .withColumn("_device_seen_30d", F.array_contains(
            F.collect_set("device_id").over(prior_30d),
            F.col("device_id"),
        ))
        .withColumn("_prev_event_ts", F.lag("_feature_ts").over(sequence_w))
        .withColumn("_prev_country", F.lag("country_src").over(sequence_w))
    )

    return (
        work
        .withColumn(
            "seconds_since_prev_txn",
            F.when(
                F.col("_prev_event_ts").isNull(),
                F.lit(0.0),
            ).otherwise(
                F.greatest(
                    F.col("_feature_ts") - F.col("_prev_event_ts"),
                    F.lit(0),
                ).cast("double")
            ),
        )
        .withColumn(
            "device_new_30d",
            F.when(
                (F.col("_prior_txn_count") > 0)
                & (~F.coalesce(F.col("_device_seen_30d"), F.lit(False))),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "country_changed",
            F.when(
                F.col("_prev_country").isNotNull()
                & (F.col("country_src") != F.col("_prev_country")),
                F.lit(1.0),
            ).otherwise(F.lit(0.0)),
        )
        .withColumn(
            "amount_to_prior_avg",
            F.when(
                F.col("avg_amount_prior_30") > 0,
                F.col("amount_usd").cast("double")
                / F.col("avg_amount_prior_30"),
            ).otherwise(F.lit(1.0)),
        )
        .withColumn("amount_sum_1h", F.coalesce(F.col("amount_sum_1h"), F.lit(0.0)))
        .withColumn("avg_amount_prior_30", F.coalesce(F.col("avg_amount_prior_30"), F.lit(0.0)))
        .withColumn(
            "stddev_amount_prior_30",
            F.coalesce(F.col("stddev_amount_prior_30"), F.lit(0.0)),
        )
        .drop(
            "_feature_ts",
            "_prior_txn_count",
            "_device_seen_30d",
            "_prev_event_ts",
            "_prev_country",
        )
    )
