# Docker E2E Integration Test

A separate GitHub Actions workflow (`.github/workflows/e2e.yml`) boots the Docker stack and validates the real Kafka -> Schema Registry -> Spark 3.5.9 -> HDFS -> HBase/Hive path.

It also validates fraud alert delivery, serving API health, monitoring endpoints, Spark restart/replay behavior, and optional Airflow Gold execution.

Run it from GitHub Actions with `include_airflow=false` first, then `true`.

The workflow uploads an E2E JSON report and Docker logs for every run. The test is an integration/recovery test for the single-host academic deployment; it is not a production-readiness claim.
