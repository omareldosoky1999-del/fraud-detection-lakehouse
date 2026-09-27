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
except Exception:
    start_http_server=None
    fraud_decisions_total=fraud_alerts_total=fraud_quarantined_total=fraud_batches_total=fraud_batch_duration_seconds=None

def start_metrics_server():
    global _started
    if _started or start_http_server is None:
        return
    start_http_server(int(os.getenv("METRICS_PORT","8000")))
    _started=True
