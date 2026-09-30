# Trino semantic layer

The project exposes a stable `polaris.analytics` namespace for BI and analytical consumers.

Views:

- `fraud_transaction_risk`: transaction-level fraud decision and ML risk fields.
- `daily_fraud_kpis`: daily counts, monetary volume, rates and average risk.
- `client_risk_summary`: customer-level aggregation for current risk analytics.
- `fraud_feature_monitoring`: daily statistics over the shared feature layer.
- `model_decision_monitoring`: daily ML probability distribution and alerting decision counts.

The semantic views depend on the Iceberg `gold.fraud_decisions` and `features.transaction_features` tables. They intentionally sit outside the streaming path and can be recreated after a new environment is provisioned.

SQL source: `warehouse/trino/semantic_views.sql`.