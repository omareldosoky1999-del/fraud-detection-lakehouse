"""Validate Kafka/Spark recovery semantics for the rules-only E2E workload."""
from __future__ import annotations

import argparse

from pyspark.sql import functions as F

from processing.common.spark_session import get_spark


def validate(
    path: str,
    first_start: int,
    first_end: int,
    second_start: int,
    second_end: int,
    minimum_first: int,
    minimum_second: int,
):
    spark = get_spark("recovery-check", enable_hive=False)
    try:
        df = spark.read.parquet(path).select("transaction_id", "batch_token", "event_date")
        first = df.filter(F.col('transaction_id').between(first_start, first_end))
        second = df.filter(F.col('transaction_id').between(second_start, second_end))
        first_count = first.count()
        second_count = second.count()
        total = df.count()
        distinct_ids = df.select("transaction_id").distinct().count()

        if first_count < minimum_first:
            raise AssertionError(f"first recovered batch incomplete: {first_count} < {minimum_first}")
        if second_count < minimum_second:
            raise AssertionError(f"second recovered batch incomplete: {second_count} < {minimum_second}")
        if total != distinct_ids:
            raise AssertionError(f"duplicate transaction IDs detected: total={total}, distinct={distinct_ids}")

        print("[RECOVERY_CHECK] PASS "
              f"first={first_count} second={second_count} total={total} distinct={distinct_ids}")
    finally:
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--path', default='hdfs://namenode:8020/warehouse/gold/fraud_decisions')
    parser.add_argument('--first-start', type=int, default=300001)
    parser.add_argument('--first-end', type=int, default=300800)
    parser.add_argument('--second-start', type=int, default=400001)
    parser.add_argument('--second-end', type=int, default=400600)
    parser.add_argument('--minimum-first', type=int, default=400)
    parser.add_argument('--minimum-second', type=int, default=300)
    args = parser.parse_args(argv)
    validate(args.path, args.first_start, args.first_end, args.second_start, args.second_end, args.minimum_first, args.minimum_second)


if __name__ == '__main__':
    main()
