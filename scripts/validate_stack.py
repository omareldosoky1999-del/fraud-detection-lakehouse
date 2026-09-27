"""Static validation for the local fraud platform stack.

This deliberately does not require Docker or Spark. It catches configuration
regressions before a developer spends time building images.
"""
from pathlib import Path
import re
import sys
import yaml

ROOT = Path(__file__).resolve().parents[1]

def fail(msg):
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)

def main():
    compose_path = ROOT / "docker" / "docker-compose.yml"
    compose_text = compose_path.read_text(encoding="utf-8")
    compose = yaml.safe_load(compose_text)
    services = compose["services"]

    if ":latest" in compose_text:
        fail("unpinned :latest image tag remains")

    required = {"zookeeper", "kafka", "schema-registry", "namenode", "datanode", "spark-master", "spark-worker", "hbase", "hive", "airflow", "kafka-exporter", "prometheus", "grafana", "serving-api"}
    missing = required - services.keys()
    if missing:
        fail(f"missing services: {sorted(missing)}")

    for name, spec in services.items():
        profiles = set(spec.get("profiles", []))
        deps = spec.get("depends_on", {})
        if isinstance(deps, list):
            deps = {d: {} for d in deps}
        for dep in deps:
            if dep not in services:
                fail(f"{name} depends on unknown service {dep}")
            dep_profiles = set(services[dep].get("profiles", []))
            if dep_profiles and not (profiles & dep_profiles):
                # A default-active dependency has no profile and is always valid.
                fail(f"profile mismatch: {name} {profiles} -> {dep} {dep_profiles}")

    required_mounts = [
        "kafka_data:/var/lib/kafka/data",
        "zookeeper_data:/data",
        "hbase_data:/hbase-data",
    ]
    for item in required_mounts:
        if item not in compose_text:
            fail(f"missing persistent mount {item}")

    for rel in [
        "config/fraud_rules.yml",
        "monitoring/prometheus.yml",
        "docker/grafana/provisioning/datasources/prometheus.yml",
        "docker/grafana/provisioning/dashboards/dashboards.yml",
        "docker/grafana/dashboards/fraud-platform.json",
        "docker/serving/Dockerfile",
        "docker/serving/requirements-serving.txt",
    ]:
        if not (ROOT / rel).exists():
            fail(f"required file missing: {rel}")

    config = yaml.safe_load((ROOT / "config/fraud_rules.yml").read_text(encoding="utf-8"))
    expected_rules = {"HIGH_AMOUNT", "STRUCTURING", "IMPOSSIBLE_TRAVEL", "MULE", "VELOCITY", "NEW_DEVICE_HIGH_VALUE"}
    if set(config["rules"]) != expected_rules:
        fail("fraud rule catalogue mismatch")

    ddl = (ROOT / "warehouse/hive/ddl.sql").read_text(encoding="utf-8")
    for table in ["bronze_transactions", "silver_transactions", "quarantine_transactions", "fraud_decisions"]:
        if f"CREATE EXTERNAL TABLE IF NOT EXISTS {table}" not in ddl:
            fail(f"Hive DDL missing {table}")
    if "PARTITIONED BY (batch_token STRING, event_date DATE)" not in ddl:
        fail("Silver/decision batch-token partitions are not declared in Hive DDL")

    stream = (ROOT / "processing/streaming/bronze_silver_job.py").read_text(encoding="utf-8")
    for needle in ["build_batch_token", "mark_committed", "partitionBy(\"batch_token\", \"event_date\")", "DEFAULT_BRONZE_PATH"]:
        if needle not in stream:
            fail(f"streaming job missing {needle}")

    print("PASS: fraud stack static validation")
    print(f"services={len(services)} rules={len(config['rules'])}")

if __name__ == "__main__":
    main()
