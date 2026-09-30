"""Real-time fraud pipeline: Bronze -> Silver -> Decisions/Gold inputs.

Kafka -> Confluent Avro decode -> Bronze (raw decoded events) -> DQ split ->
Silver (validated Decimal money) -> rules -> fraud decisions -> HBase serving
cache + Kafka alerts.

Restart semantics:
* Spark checkpoint tracks Kafka progress.
* Each micro-batch gets a deterministic token from Kafka partition/offset
  ranges. Silver/quarantine/decisions/Bronze writes use that token as a
  partition, so retrying the same batch overwrites only that batch partition.
* A final COMMITTED marker is written after core sinks complete. This prevents
  a completed batch from being replayed after a driver restart.
* Kafka alert delivery remains at-least-once at the topic level; alert_id is
  deterministic so a downstream consumer can de-duplicate safely.
"""
from __future__ import annotations

import argparse
import os
import time
from datetime import timedelta
from pathlib import Path

from pyspark.sql import DataFrame, Window, functions as F

from processing.common.spark_session import get_spark
from processing.lakehouse.iceberg_tables import write_micro_batch
from processing.features.fraud_features import add_fraud_features
from processing.ml.inference import apply_ml_ensemble, load_models
from processing.ml.registry import decision_threshold
from processing.monitoring.metrics import (
    fraud_alerts_total, fraud_batches_total, fraud_batch_duration_seconds,
    fraud_decisions_total, fraud_quarantined_total, fraud_ml_probability,
    fraud_feature_rows_total, start_metrics_server,
)
from processing.rules.rules_engine import apply_rules
from processing.serving.alerts_sink import write_alerts
from processing.serving.hbase_sink import build_connection_factory, write_partition
from processing.streaming.batch_commit import (
    batch_path, build_batch_token, delete_path, is_committed, mark_committed,
    is_committed_iceberg, mark_committed_iceberg,
)
from processing.streaming.decode import decode_transactions
from processing.streaming.transform import dedup_batch, split_valid_invalid, to_silver

SCHEMA_PATH = Path(__file__).resolve().parents[2] / "ingestion" / "schema" / "transaction.avsc"
HISTORY_LOOKBACK_HOURS = 30 * 24
DEDUP_WATERMARK = "2 hours"
DEFAULT_BRONZE_PATH = "hdfs://namenode:8020/warehouse/bronze/transactions"
DEFAULT_BATCH_COMMITS = "hdfs://namenode:8020/checkpoints/bronze_silver_commits"


def _write_bronze(batch_df: DataFrame, *, bronze_path: str, batch_token: str) -> None:
    bronze = (batch_df
              .withColumn("ingest_date", F.to_date("kafka_timestamp"))
              .withColumn("batch_token", F.lit(batch_token)))
    (bronze.write.mode("overwrite")
     .partitionBy("batch_token", "ingest_date")
     .parquet(bronze_path))


def _filter_transactions_already_committed(
    silver_df: DataFrame,
    spark,
    silver_path: str,
    batch_token: str,
    *,
    iceberg_enabled: bool,
    iceberg_catalog: str,
) -> DataFrame:
    """Prevent duplicate transaction_ids after a Kafka checkpoint reset.

    Missing Silver storage is treated as the first-ever write. Any other
    read/storage failure is allowed to propagate so a banking workload cannot
    silently disable duplicate protection during an outage.
    """
    if silver_df.take(1) == []:
        return silver_df

    if iceberg_enabled:
        table = f"{iceberg_catalog}.{TABLES['silver']}"
        if not spark.catalog.tableExists(table):
            return silver_df
        existing = (
            spark.table(table)
            .filter(F.col("batch_token") != F.lit(batch_token))
            .select("transaction_id")
            .dropDuplicates()
        )
    else:
        if not path_exists(spark, silver_path):
            return silver_df
        existing = (
            spark.read.parquet(str(silver_path))
            .filter(F.col("batch_token") != F.lit(batch_token))
            .select("transaction_id")
            .dropDuplicates()
        )

    return silver_df.join(
        existing,
        on="transaction_id",
        how="left_anti",
    )


def build_batch_processor(spark, *, bronze_path=DEFAULT_BRONZE_PATH,
                          silver_path, quarantine_path, decisions_path,
                          commit_root=None,
                          hbase_host, hbase_port, kafka_bootstrap, alerts_topic,
                          history_hours=HISTORY_LOOKBACK_HOURS, ml_model_dir=None, ml_threshold=0.70,
                          ml_required=True, iceberg_enabled=None, iceberg_catalog=None,
                          legacy_hdfs_enabled=None):
    commit_root = commit_root or f"{decisions_path.rstrip('/')} /_batch_commits".replace(" /", "/")
    if iceberg_enabled is None:
        iceberg_enabled = os.getenv("ICEBERG_ENABLED", "false").lower() == "true"
    if iceberg_catalog is None:
        iceberg_catalog = os.getenv("ICEBERG_CATALOG_NAME", "polaris")
    if legacy_hdfs_enabled is None:
        legacy_hdfs_enabled = (
            os.getenv(
                "LEGACY_HDFS_ENABLED",
                "false" if iceberg_enabled else "true",
            ).lower()
            == "true"
        )
    hbase_factory = build_connection_factory(hbase_host, hbase_port) if hbase_host else None
    ml_models = load_models(ml_model_dir)
    if ml_required and not ml_models:
        raise RuntimeError(
            "ML is required for the fraud decision pipeline, but no production "
            "MLflow models or local model artifacts were resolved."
        )
    spark.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    start_metrics_server()

    def process_batch(batch_df: DataFrame, batch_id: int) -> None:
        if batch_df.rdd.isEmpty():
            return

        batch_started = time.monotonic()
        batch_token = build_batch_token(batch_df, batch_id)
        if iceberg_enabled and not legacy_hdfs_enabled:
            if is_committed_iceberg(spark, iceberg_catalog, batch_token):
                return
        elif is_committed(spark, commit_root, batch_token):
            return

        # Re-running the same uncommitted batch clears only the legacy HDFS
        # artifacts for that batch. The Iceberg writer performs its own
        # row-level idempotent replacement before the committed marker.
        if legacy_hdfs_enabled:
            for path in (
                batch_path(bronze_path, batch_token),
                batch_path(quarantine_path, batch_token),
                batch_path(silver_path, batch_token),
                batch_path(decisions_path, batch_token),
            ):
                delete_path(spark, path)

            _write_bronze(
                batch_df,
                bronze_path=bronze_path,
                batch_token=batch_token,
            )
        batch_df = dedup_batch(batch_df)
        valid, invalid = split_valid_invalid(batch_df)

        if not invalid.rdd.isEmpty():
            if fraud_quarantined_total is not None:
                fraud_quarantined_total.inc(invalid.count())
            if legacy_hdfs_enabled:
                (
                    invalid
                    .withColumn("quarantine_date", F.to_date("quarantined_at"))
                    .withColumn("batch_token", F.lit(batch_token))
                    .write.mode("overwrite")
                    .partitionBy("batch_token", "quarantine_date")
                    .parquet(quarantine_path)
                )

        # Persist the raw Bronze/Quarantine side of the batch before
        # duplicate filtering so a batch containing only already-seen
        # transaction IDs is still represented in the audit layer.
        if iceberg_enabled and not legacy_hdfs_enabled:
            write_micro_batch(
                spark,
                catalog=iceberg_catalog,
                batch_token=batch_token,
                bronze=batch_df,
                quarantine=invalid,
                silver=None,
                decisions=None,
            )

        if valid.rdd.isEmpty():
            if iceberg_enabled and not legacy_hdfs_enabled:
                mark_committed_iceberg(
                    spark,
                    iceberg_catalog,
                    batch_token,
                )
            else:
                mark_committed(spark, commit_root, batch_token)
            return

        silver = to_silver(valid).withColumn(
            "batch_token",
            F.lit(batch_token),
        )
        silver = _filter_transactions_already_committed(
            silver,
            spark,
            silver_path,
            batch_token,
            iceberg_enabled=iceberg_enabled,
            iceberg_catalog=iceberg_catalog,
        ).cache()
        if silver.rdd.isEmpty():
            silver.unpersist()
            mark_committed(spark, commit_root, batch_token)
            return

        if legacy_hdfs_enabled:
            (
                silver.write.mode("overwrite")
                .partitionBy("batch_token", "event_date")
                .parquet(silver_path)
            )

        batch_transactions = silver.select("transaction_id").distinct().cache()
        touched_clients = silver.select("client_id").distinct()
        batch_min_event_time = silver.agg(
            F.min("event_time").alias("min_event_time")
        ).first()["min_event_time"]
        if batch_min_event_time is None:
            silver.unpersist()
            batch_transactions.unpersist()
            mark_committed(spark, commit_root, batch_token)
            return

        history_start = batch_min_event_time - timedelta(hours=history_hours)
        if legacy_hdfs_enabled:
            history = (
                spark.read.parquet(str(silver_path))
                .filter(F.col("event_time") >= F.lit(history_start))
                .join(touched_clients, on="client_id", how="left_semi")
            )
        else:
            history = (
                spark.table(
                    f"{iceberg_catalog}.{TABLES['silver']}"
                )
                .filter(F.col("event_time") >= F.lit(history_start))
                .join(touched_clients, on="client_id", how="left_semi")
            )
        combined = history.unionByName(silver, allowMissingColumns=True).dropDuplicates(["transaction_id"])
        featured_all = add_fraud_features(combined)
        feature_batch = (
            featured_all
            .join(batch_transactions, on="transaction_id", how="inner")
            .withColumn("batch_token", F.lit(batch_token))
            .cache()
        )

        decided_all = apply_rules(featured_all)
        if ml_models:
            decided_all = apply_ml_ensemble(decided_all, ml_models, threshold=ml_threshold)
        decided_batch = (
            decided_all
            .join(batch_transactions, on="transaction_id", how="inner")
            .withColumn("batch_token", F.lit(batch_token))
            .cache()
        )

        if legacy_hdfs_enabled:
            (
                decided_batch.write.mode("overwrite")
                .partitionBy("batch_token", "event_date")
                .parquet(decisions_path)
            )

        if iceberg_enabled:
            write_micro_batch(
                spark,
                catalog=iceberg_catalog,
                batch_token=batch_token,
                bronze=batch_df,
                quarantine=invalid,
                silver=silver,
                decisions=decided_batch,
                features=feature_batch,
            )

        if fraud_feature_rows_total is not None:
            fraud_feature_rows_total.inc(feature_batch.count())
        if fraud_ml_probability is not None:
            probability_summary = (
                decided_batch
                .select(F.avg("ml_probability").alias("avg_ml_probability"))
                .first()
            )
            if probability_summary and probability_summary["avg_ml_probability"] is not None:
                fraud_ml_probability.observe(float(probability_summary["avg_ml_probability"]))

        if hbase_factory:
            latest_w = (Window.partitionBy("client_id")
                        .orderBy(F.col("event_time").desc(), F.col("transaction_id").desc()))
            latest_per_client = (decided_batch
                                 .withColumn("_rn", F.row_number().over(latest_w))
                                 .filter(F.col("_rn") == 1)
                                 .drop("_rn"))
            latest_per_client.foreachPartition(lambda part: write_partition(part, hbase_factory))

        if fraud_decisions_total is not None:
            for decision_row in decided_batch.groupBy("decision").count().collect():
                fraud_decisions_total.labels(decision_row.decision).inc(decision_row["count"])

        if kafka_bootstrap and alerts_topic:
            alert_count = write_alerts(decided_batch, bootstrap_servers=kafka_bootstrap,
                                       topic=alerts_topic, batch_token=batch_token)
            if fraud_alerts_total is not None:
                fraud_alerts_total.inc(alert_count)

        if iceberg_enabled and not legacy_hdfs_enabled:
            mark_committed_iceberg(
                spark,
                iceberg_catalog,
                batch_token,
            )
        else:
            mark_committed(spark, commit_root, batch_token)
        if fraud_batches_total is not None:
            fraud_batches_total.inc()
        if fraud_batch_duration_seconds is not None:
            fraud_batch_duration_seconds.observe(time.monotonic() - batch_started)
        silver.unpersist()
        feature_batch.unpersist()
        decided_batch.unpersist()
        batch_transactions.unpersist()

    return process_batch


def run(args):
    spark = get_spark("fraud-bronze-silver")
    schema_json = SCHEMA_PATH.read_text()

    raw = (spark.readStream
          .format("kafka")
          .option("kafka.bootstrap.servers", args.kafka_bootstrap)
          .option("subscribe", args.topic)
          .option("startingOffsets", args.starting_offsets)
          .option("maxOffsetsPerTrigger", args.max_offsets_per_trigger)
          .load())

    events = decode_transactions(raw, schema_json)
    events = events.withWatermark("kafka_timestamp", DEDUP_WATERMARK).dropDuplicates(["Trans_id"])

    process_batch = build_batch_processor(
        spark,
        bronze_path=args.bronze_path,
        silver_path=args.silver_path,
        quarantine_path=args.quarantine_path,
        decisions_path=args.decisions_path,
        commit_root=args.commit_root,
        hbase_host=args.hbase_host,
        hbase_port=args.hbase_port,
        kafka_bootstrap=args.kafka_bootstrap,
        alerts_topic=args.alerts_topic,
        ml_model_dir=args.ml_model_dir,
        ml_threshold=args.ml_threshold,
        ml_required=not args.rules_only,
        iceberg_enabled=not args.no_iceberg,
        iceberg_catalog=args.iceberg_catalog,
        legacy_hdfs_enabled=args.legacy_hdfs_enabled,
    )

    (events.writeStream
     .foreachBatch(process_batch)
     .option("checkpointLocation", args.checkpoint)
     .trigger(processingTime=args.trigger)
     .start()
     .awaitTermination())


def main(argv=None):
    ap = argparse.ArgumentParser(description="Bronze/Silver fraud-detection streaming job.")
    ap.add_argument("--kafka-bootstrap", default=os.getenv("KAFKA_BOOTSTRAP_INTERNAL", "kafka:9092"))
    ap.add_argument("--topic", default=os.getenv("TOPIC_TRANSACTIONS", "transactions"))
    ap.add_argument("--alerts-topic", default=os.getenv("TOPIC_ALERTS", "fraud.alerts"))
    ap.add_argument("--starting-offsets", default="earliest")
    ap.add_argument("--max-offsets-per-trigger", default="5000")
    ap.add_argument("--trigger", default="10 seconds")
    ap.add_argument("--bronze-path", default=os.getenv("BRONZE_PATH", DEFAULT_BRONZE_PATH))
    ap.add_argument("--silver-path", default=os.getenv("SILVER_PATH", "hdfs://namenode:8020/warehouse/silver/transactions"))
    ap.add_argument("--quarantine-path", default=os.getenv("QUARANTINE_PATH", "hdfs://namenode:8020/warehouse/quarantine/transactions"))
    ap.add_argument("--decisions-path", default=os.getenv("DECISIONS_PATH", "hdfs://namenode:8020/warehouse/gold/fraud_decisions"))
    ap.add_argument("--checkpoint", default=os.getenv("STREAMING_CHECKPOINT", "hdfs://namenode:8020/checkpoints/bronze_silver"))
    ap.add_argument("--commit-root", default=os.getenv("BATCH_COMMIT_ROOT", DEFAULT_BATCH_COMMITS))
    ap.add_argument("--ml-model-dir", default=os.getenv("ML_MODEL_DIR"))
    ap.add_argument("--ml-threshold", type=float, default=decision_threshold())
    ap.add_argument(
        "--rules-only",
        action="store_true",
        help="Explicit legacy/test mode that disables the ML-required startup gate.",
    )
    ap.add_argument("--iceberg-catalog", default=os.getenv("ICEBERG_CATALOG_NAME", "polaris"))
    ap.add_argument(
        "--legacy-hdfs-enabled",
        action=argparse.BooleanOptionalAction,
        default=None,
        help="Keep migration-era HDFS writes/history/commit markers enabled.",
    )
    ap.add_argument("--no-iceberg", action="store_true")
    ap.add_argument("--hbase-host", default=os.getenv("HBASE_HOST", "hbase"))
    ap.add_argument("--hbase-port", type=int, default=int(os.getenv("HBASE_THRIFT_PORT", "9090")))
    ap.add_argument("--no-hbase", action="store_true")
    a = ap.parse_args(argv)
    if a.no_hbase:
        a.hbase_host = None
    run(a)


if __name__ == "__main__":
    main()
