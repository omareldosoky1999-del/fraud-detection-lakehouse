"""Kafka alert sink with deterministic IDs for downstream de-duplication."""
from pyspark.sql import DataFrame, functions as F


def build_alerts(decided_df: DataFrame, batch_token: str = "unknown") -> DataFrame:
    alerts = decided_df.filter(F.col("decision") != "PASS")
    alert_id = F.concat_ws(":", F.lit(batch_token), F.col("transaction_id").cast("string"))
    payload = F.to_json(F.struct(
        alert_id.alias("alert_id"),
        F.lit(batch_token).alias("batch_token"),
        "transaction_id", "client_id", "amount_usd", "currency", "decision",
        "risk_score", "matched_rules", "event_time",
    ))
    return alerts.select(
        alert_id.alias("key"),
        payload.alias("value"),
    )


def write_alerts(decided_df: DataFrame, *, bootstrap_servers: str, topic: str, batch_token: str) -> int:
    to_send = build_alerts(decided_df, batch_token=batch_token)
    count = to_send.count()
    if count:
        (to_send.write.format("kafka")
         .option("kafka.bootstrap.servers", bootstrap_servers)
         .option("topic", topic)
         .save())
    return count
