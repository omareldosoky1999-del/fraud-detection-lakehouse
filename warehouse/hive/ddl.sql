-- Hive external tables over the Parquet data written by Spark.
CREATE DATABASE IF NOT EXISTS fraud;
USE fraud;

CREATE EXTERNAL TABLE IF NOT EXISTS bronze_transactions (
    Trans_id BIGINT, Clt_id BIGINT, Card_id BIGINT, Dev_id BIGINT,
    Trans_amount BIGINT, Trans_date STRING, Trans_type STRING, Trans_status STRING,
    Trans_destination STRING, Dev_Ip_Location STRING, Trans_Ref_No STRING,
    Currency STRING, Trans_Reason STRING, Dest_account_No BIGINT,
    Country_Dest STRING, Country_Src STRING,
    kafka_partition INT, kafka_offset BIGINT, kafka_timestamp TIMESTAMP
)
PARTITIONED BY (batch_token STRING, ingest_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/bronze/transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS silver_transactions (
    transaction_id BIGINT, client_id BIGINT, card_id BIGINT, device_id BIGINT,
    amount DECIMAL(18,4), fx_rate DECIMAL(18,8), amount_usd DECIMAL(18,4),
    currency STRING, txn_type STRING, status STRING, destination_bank STRING,
    device_location STRING, ref_no STRING, reason STRING, dest_account_no BIGINT,
    country_src STRING, country_dest STRING, event_time TIMESTAMP, ingest_time TIMESTAMP
)
PARTITIONED BY (batch_token STRING, event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/silver/transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS quarantine_transactions (
    Trans_id BIGINT, Clt_id BIGINT, Card_id BIGINT, Dev_id BIGINT, Trans_amount BIGINT,
    Trans_date STRING, Trans_type STRING, Trans_status STRING, Trans_destination STRING,
    Dev_Ip_Location STRING, Trans_Ref_No STRING, Currency STRING, Trans_Reason STRING,
    Dest_account_No BIGINT, Country_Dest STRING, Country_Src STRING,
    dq_errors ARRAY<STRING>, quarantined_at TIMESTAMP
)
PARTITIONED BY (batch_token STRING, quarantine_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/quarantine/transactions';

CREATE EXTERNAL TABLE IF NOT EXISTS fraud_decisions (
    transaction_id BIGINT, client_id BIGINT, card_id BIGINT, device_id BIGINT,
    amount DECIMAL(18,4), fx_rate DECIMAL(18,8), amount_usd DECIMAL(18,4),
    currency STRING, txn_type STRING, status STRING, destination_bank STRING,
    device_location STRING, ref_no STRING, reason STRING, dest_account_no BIGINT,
    country_src STRING, country_dest STRING, event_time TIMESTAMP, ingest_time TIMESTAMP,
    matched_rules ARRAY<STRING>, risk_score INT, decision STRING
)
PARTITIONED BY (batch_token STRING, event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/gold/fraud_decisions';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_customer_daily_risk (
    client_id BIGINT, txn_count BIGINT, total_amount_usd DECIMAL(18,4),
    flagged_count BIGINT, blocked_count BIGINT, max_risk_score INT,
    distinct_countries BIGINT, distinct_devices BIGINT
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/gold/customer_daily_risk';

CREATE EXTERNAL TABLE IF NOT EXISTS gold_daily_kpis (
    total_txns BIGINT, total_amount_usd DECIMAL(18,4), flagged_rate DOUBLE,
    blocked_rate DOUBLE, avg_risk_score DOUBLE
)
PARTITIONED BY (event_date DATE)
STORED AS PARQUET
LOCATION 'hdfs://namenode:8020/warehouse/gold/daily_kpis';
