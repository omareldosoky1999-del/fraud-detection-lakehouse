# End-to-End Testing

The repository uses layered GitHub Actions E2E workflows rather than one always-on heavyweight integration job.

## Fast push-triggered gates

- **Lakehouse E2E**: boots the local Iceberg/Polaris/RustFS stack and verifies Spark writes, Gold construction, and Trino reads.
- **Serving E2E**: verifies deterministic alert de-duplication in the SQLite-backed consumer.
- **Quality E2E**: validates a persisted Silver dataset through the Great Expectations gate.
- **MLOps E2E**: verifies Spark MLlib -> MLflow tracking/registry integration.
- **Schema Registry E2E**: validates the Avro/schema-registry contract.
- **Lineage E2E**: validates OpenLineage/Marquez integration.
- **Trino Semantic E2E**: validates the analytical semantic layer.
- **Monitoring Validation**: validates Prometheus/Alertmanager configuration and monitoring tests.
- **Recovery E2E**: manually validates Spark restart/checkpoint recovery and duplicate protection.

## Heavy full-stack gate

`.github/workflows/full-stack-e2e.yml` is intentionally manual or weekly. It exercises the complete banking path:

```text
Kafka + Schema Registry
        -> Spark Structured Streaming
        -> Bronze / Silver / Features
        -> Rules + MLlib ensemble
        -> Iceberg / Polaris / RustFS
        -> Trino + HBase + Kafka alerts
        -> Great Expectations + Gold
```

The full-stack workflow also trains a candidate ensemble, performs controlled production promotion, evaluates decisions against generated labels, checks the semantic SQL layer, verifies alert persistence/deduplication, and checks the HBase-backed serving API.

## Local execution

From the repository root:

```bash
python -m pytest tests -q
```

For runtime E2E, use the corresponding GitHub Actions workflow or run the documented Docker stack locally. The layer-specific workflows are the normal regression gates; the full-stack workflow is the heavier release-level integration check.

The E2E suite validates the local academic/development deployment boundary. Passing it does not by itself mean cloud infrastructure has been deployed or production credentials have been exercised.