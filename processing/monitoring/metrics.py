"""Lightweight Prometheus metrics emitted by the Spark driver."""
from __future__ import annotations
import os
_started=False
try:
    from prometheus_client import Counter, Histogram, start_http_server
    fraud_decisions_total=Counter("fraud_decisions_total","Fraud decisions emitted",["decision"])
    fraud_alerts_total=Counter("fraud_alerts_total","Fraud alerts published")
    fraud_quarantined_total=Counter("fraud_quarantined_total","Rows sent to quarantine")
    fraud_batches_total=Counter("fraud_batches_total","Micro-batches processed")
    fraud_batch_duration_seconds=Histogram("fraud_batch_duration_seconds","Micro-batch duration")
    fraud_ml_probability=Histogram("fraud_ml_probability","ML ensemble fraud probability",buckets=[0.01,0.05,0.1,0.25,0.5,0.7,0.9,0.95,0.99,1.0])
    fraud_feature_rows_total=Counter("fraud_feature_rows_total","Rows materialized into the shared fraud feature layer")
except Exception:
    start_http_server=None
    fraud_decisions_total=fraud_alerts_total=fraud_quarantined_total=fraud_batches_total=fraud_batch_duration_seconds=fraud_ml_probability=fraud_feature_rows_total=None

def start_metrics_server():
    global _started
    if _started or start_http_server is None:
        return
    start_http_server(int(os.getenv("METRICS_PORT","8000")))
    _started=True
