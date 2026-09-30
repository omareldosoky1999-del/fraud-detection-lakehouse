from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_trino_semantic_layer_contains_core_views():
    sql = (ROOT / 'warehouse' / 'trino' / 'semantic_views.sql').read_text(encoding='utf-8')
    assert 'CREATE SCHEMA IF NOT EXISTS polaris.analytics' in sql
    assert 'polaris.analytics.fraud_transaction_risk' in sql
    assert 'polaris.analytics.daily_fraud_kpis' in sql
    assert 'polaris.analytics.client_risk_summary' in sql
    assert 'polaris.analytics.fraud_feature_monitoring' in sql
    assert 'polaris.analytics.model_decision_monitoring' in sql
    assert 'polaris.gold.fraud_decisions' in sql
    assert 'polaris.features.transaction_features' in sql


def test_trino_semantic_views_are_not_in_streaming_code():
    stream = (ROOT / 'processing' / 'streaming' / 'bronze_silver_job.py').read_text(encoding='utf-8')
    assert 'CREATE OR REPLACE VIEW' not in stream
