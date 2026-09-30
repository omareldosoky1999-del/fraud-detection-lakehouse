"""Seed production-shaped Iceberg tables for the Trino semantic-layer E2E."""
from __future__ import annotations

from datetime import datetime

from pyspark.sql import Row
from processing.common.spark_session import get_spark


def main():
    spark = get_spark("trino-semantic-smoke", enable_hive=False)
    try:
        spark.sql('CREATE NAMESPACE IF NOT EXISTS polaris.gold')
        spark.sql('CREATE NAMESPACE IF NOT EXISTS polaris.features')

        decisions = spark.createDataFrame([
            Row(
                transaction_id=1, client_id=10, event_date=datetime(2026, 9, 1).date(),
                event_time=datetime(2026, 9, 1, 10, 0, 0), amount_usd=100.0, currency='USD',
                txn_type='Withdrawal', status='Successful', country_src='Egypt',
                country_dest='Egypt', decision='PASS', risk_score=10, ml_probability=0.05,
                matched_rules=[], batch_token='e2e-1'
            ),
            Row(
                transaction_id=2, client_id=10, event_date=datetime(2026, 9, 1).date(),
                event_time=datetime(2026, 9, 1, 10, 5, 0), amount_usd=1000.0, currency='USD',
                txn_type='Withdrawal', status='Successful', country_src='Egypt',
                country_dest='France', decision='FLAG', risk_score=85, ml_probability=0.91,
                matched_rules=['COUNTRY_CHANGE'], batch_token='e2e-1'
            ),
        ])
        decisions.writeTo("polaris.gold.fraud_decisions").using("iceberg").createOrReplace()

        features = spark.createDataFrame([
            Row(event_date=datetime(2026, 9, 1).date(), txn_count_5m=1.0, amount_sum_1h=100.0,
                amount_to_prior_avg=1.0, device_new_30d=0.0, country_changed=0.0,
                batch_token='e2e-1', transaction_id=1),
            Row(event_date=datetime(2026, 9, 1).date(), txn_count_5m=2.0, amount_sum_1h=1100.0,
                amount_to_prior_avg=10.0, device_new_30d=1.0, country_changed=1.0,
                batch_token='e2e-1', transaction_id=2),
        ])
        features.writeTo("polaris.features.transaction_features").using("iceberg").createOrReplace()
        print("[TRINO_SEMANTIC_SMOKE] production-shaped Iceberg tables ready")
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
