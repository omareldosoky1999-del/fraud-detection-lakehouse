from pathlib import Path
import shutil
from datetime import datetime, timedelta

import pytest

pyspark = pytest.importorskip("pyspark", reason="pyspark not installed; see requirements-dev.txt")

from processing.common.spark_session import get_spark  # noqa: E402
from processing.streaming.bronze_silver_job import build_batch_processor  # noqa: E402

T0 = datetime(2026, 9, 1, 12, 0, 0)


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-bronze-silver", enable_hive=False)
    yield s
    s.stop()


@pytest.fixture
def warehouse(tmp_path):
    paths = dict(bronze=tmp_path / "bronze", silver=tmp_path / "silver",
                quarantine=tmp_path / "quarantine", decisions=tmp_path / "decisions",
                commits=tmp_path / "commits")
    yield paths
    shutil.rmtree(tmp_path, ignore_errors=True)


def raw_row(id, client, t, usd, ttype="Withdrawal", status="Successful", country="Egypt", **kw):
    # Mirrors what decode_transactions() actually hands to process_batch in
    # production: the Avro fields PLUS the three Kafka metadata columns
    # (kafka_partition/kafka_offset feed build_batch_token; kafka_timestamp
    # feeds _write_bronze's ingest_date). Without these, build_batch_token()
    # silently falls back to "spark-{batch_id}" and _write_bronze() throws
    # an AnalysisException on a column that plain unit-built DataFrames
    # never had -- exactly the gap that let this bug ship untested.
    base = dict(Trans_id=id, Clt_id=client, Card_id=1, Dev_id=1, Trans_amount=usd,
               Trans_date=t.strftime("%Y-%m-%d %I:%M:%S %p"), Trans_type=ttype, Trans_status=status,
               Trans_destination="CIB", Dev_Ip_Location="Cairo", Trans_Ref_No=f"R{id}", Currency="USD",
               Trans_Reason="Shopping", Dest_account_No=1, Country_Dest="Egypt", Country_Src=country,
               kafka_partition=0, kafka_offset=id, kafka_timestamp=t)
    base.update(kw)
    return base


def make_processor(spark, warehouse):
    return build_batch_processor(
        spark, bronze_path=str(warehouse["bronze"]), silver_path=str(warehouse["silver"]),
        quarantine_path=str(warehouse["quarantine"]), decisions_path=str(warehouse["decisions"]),
        commit_root=str(warehouse["commits"]), hbase_host=None, hbase_port=None,
        kafka_bootstrap=None, alerts_topic=None, ml_required=False,
    )


def test_valid_rows_land_in_silver_invalid_in_quarantine(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    batch = spark.createDataFrame([
        raw_row(1, 10, T0, 50),
        raw_row(2, 11, T0, 100, Trans_status="PENDING"),  # DQ violation
    ])
    process_batch(batch, 0)

    silver_ids = {r.transaction_id for r in spark.read.parquet(str(warehouse["silver"])).collect()}
    quarantine_ids = {r.Trans_id for r in spark.read.parquet(str(warehouse["quarantine"])).collect()}
    assert silver_ids == {1}
    assert quarantine_ids == {2}


def test_high_amount_decision_is_block(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    process_batch(spark.createDataFrame([raw_row(1, 10, T0, 15_000)]), 0)
    decisions = spark.read.parquet(str(warehouse["decisions"])).collect()
    assert decisions[0].decision == "BLOCK"
    assert "HIGH_AMOUNT" in decisions[0].matched_rules


def test_history_read_lets_a_later_batch_complete_a_velocity_burst(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    process_batch(spark.createDataFrame([raw_row(1, 10, T0, 20)]), 0)
    later = [raw_row(2 + i, 10, T0 + timedelta(seconds=(i + 1) * 20), 20) for i in range(6)]
    process_batch(spark.createDataFrame(later), 1)

    decisions = spark.read.parquet(str(warehouse["decisions"]))
    flagged = decisions.filter("decision != 'PASS'").collect()
    assert any("VELOCITY" in r.matched_rules for r in flagged)


def test_within_batch_duplicate_transaction_id_is_deduplicated(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    batch = spark.createDataFrame([raw_row(1, 10, T0, 50), raw_row(1, 10, T0, 50)])
    process_batch(batch, 0)
    silver = spark.read.parquet(str(warehouse["silver"]))
    assert silver.count() == 1


def test_empty_batch_is_a_noop(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    empty = spark.createDataFrame([raw_row(1, 10, T0, 50)]).filter("Trans_id < 0")
    process_batch(empty, 0)  # must not raise, and must not create the warehouse dirs
    assert not warehouse["silver"].exists()


def test_new_device_history_is_available_across_30_days(spark, warehouse):
    process_batch = make_processor(spark, warehouse)
    process_batch(spark.createDataFrame([raw_row(1, 10, T0, 50, Dev_id=1)]), 0)
    later = T0 + timedelta(days=29, hours=1)
    process_batch(spark.createDataFrame([raw_row(2, 10, later, 6000, Dev_id=2)]), 1)
    decisions = spark.read.parquet(str(warehouse["decisions"]))
    row = decisions.filter("transaction_id = 2").collect()[0]
    assert "NEW_DEVICE_HIGH_VALUE" in row.matched_rules


def test_cloud_streaming_code_uses_iceberg_commit_marker_on_empty_filtered_batch():
    source = (Path(__file__).parents[1] / "processing" / "streaming" / "bronze_silver_job.py").read_text(encoding="utf-8")
    assert "from processing.lakehouse.iceberg_tables import TABLES, write_micro_batch" in source
    assert "if iceberg_enabled and not legacy_hdfs_enabled:" in source
    assert "mark_committed_iceberg(spark, iceberg_catalog, batch_token)" in source
