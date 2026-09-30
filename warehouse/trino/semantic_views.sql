-- Trino semantic layer for BI and analytical consumers.
-- Run after the corresponding Iceberg tables exist in Polaris.

CREATE SCHEMA IF NOT EXISTS polaris.analytics;

CREATE OR REPLACE VIEW polaris.analytics.fraud_transaction_risk AS
SELECT
    transaction_id,
    client_id,
    event_date,
    event_time,
    amount_usd,
    currency,
    txn_type,
    status,
    country_src,
    country_dest,
    decision,
    risk_score,
    ml_probability,
    matched_rules,
    batch_token
FROM polaris.gold.fraud_decisions;

CREATE OR REPLACE VIEW polaris.analytics.daily_fraud_kpis AS
SELECT
    event_date,
    count(*) AS total_transactions,
    sum(amount_usd) AS total_amount_usd,
    count_if(decision = 'FLAG') AS flagged_transactions,
    count_if(decision = 'BLOCK') AS blocked_transactions,
    round(count_if(decision = 'FLAG') * 1.0 / nullif(count(*), 0), 4) AS flagged_rate,
    round(count_if(decision = 'BLOCK') * 1.0 / nullif(count(*), 0), 4) AS blocked_rate,
    round(avg(risk_score), 2) AS avg_risk_score,
    round(avg(ml_probability), 4) AS avg_ml_probability
FROM polaris.gold.fraud_decisions
GROUP BY event_date;

CREATE OR REPLACE VIEW polaris.analytics.client_risk_summary AS
SELECT
    client_id,
    max(event_time) AS last_event_time,
    count(*) AS transaction_count,
    sum(amount_usd) AS total_amount_usd,
    max(risk_score) AS max_risk_score,
    count_if(decision = 'FLAG') AS flagged_count,
    count_if(decision = 'BLOCK') AS blocked_count,
    max_by(decision, event_time) AS last_decision,
    max_by(ml_probability, event_time) AS last_ml_probability
FROM polaris.gold.fraud_decisions
GROUP BY client_id;

CREATE OR REPLACE VIEW polaris.analytics.fraud_feature_monitoring AS
SELECT
    event_date,
    count(*) AS feature_rows,
    avg(txn_count_5m) AS avg_txn_count_5m,
    avg(amount_sum_1h) AS avg_amount_sum_1h,
    avg(amount_to_prior_avg) AS avg_amount_to_prior_avg,
    avg(device_new_30d) AS new_device_rate,
    avg(country_changed) AS country_change_rate
FROM polaris.features.transaction_features
GROUP BY event_date;

CREATE OR REPLACE VIEW polaris.analytics.model_decision_monitoring AS
SELECT
    event_date,
    count(*) AS scored_rows,
    avg(ml_probability) AS avg_ml_probability,
    approx_percentile(ml_probability, 0.50) AS p50_ml_probability,
    approx_percentile(ml_probability, 0.95) AS p95_ml_probability,
    count_if(decision IN ('FLAG', 'BLOCK')) AS alerting_decisions
FROM polaris.gold.fraud_decisions
GROUP BY event_date;
