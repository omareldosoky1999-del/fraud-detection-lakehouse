"""Static validation for the local and profile-based fraud platform stack.

This intentionally does not require Docker or Spark. It catches configuration
regressions before an integration run spends time pulling images.
"""
from __future__ import annotations

from pathlib import Path
import sys

import yaml

ROOT = Path(__file__).resolve().parents[1]


def fail(msg: str) -> None:
    print(f"FAIL: {msg}", file=sys.stderr)
    raise SystemExit(1)


def _load(path: str) -> dict:
    file_path = ROOT / path
    if not file_path.exists():
        fail(f"required compose/config file missing: {path}")
    return yaml.safe_load(file_path.read_text(encoding="utf-8")) or {}


def _merge_services(*paths: str) -> dict:
    services = {}
    for path in paths:
        overlay_services = _load(path).get("services", {})
        for name, spec in overlay_services.items():
            if name not in services:
                services[name] = dict(spec)
                continue

            merged = {**services[name], **spec}

            # Compose overlays can intentionally attach different profiles to
            # the same shared service (e.g. RustFS serves both Lakehouse and
            # MLOps). Preserve the union for static dependency validation.
            previous_profiles = services[name].get("profiles", [])
            overlay_profiles = spec.get("profiles", [])
            profiles = list(dict.fromkeys(previous_profiles + overlay_profiles))
            if profiles:
                merged["profiles"] = profiles

            services[name] = merged
    return services


def _validate_dependencies(services: dict) -> None:
    for name, spec in services.items():
        profiles = set(spec.get("profiles", []))
        deps = spec.get("depends_on", {})
        if isinstance(deps, list):
            deps = {d: {} for d in deps}

        for dep in deps:
            if dep not in services:
                fail(f"{name} depends on unknown service {dep}")

            dep_profiles = set(services[dep].get("profiles", []))
            if dep_profiles and profiles and not (profiles & dep_profiles):
                fail(
                    f"profile mismatch: {name} {profiles} -> "
                    f"{dep} {dep_profiles}"
                )


def main() -> None:
    base_path = "docker/docker-compose.yml"
    overlay_paths = [
        "docker/docker-compose.lakehouse.yml",
        "docker/docker-compose.mlflow.yml",
        "docker/docker-compose.lineage.yml",
    ]

    base_text = (ROOT / base_path).read_text(encoding="utf-8")
    if ":latest" in base_text:
        fail("unpinned :latest image tag remains in base Compose")

    services = _merge_services(base_path, *overlay_paths)

    required_core = {
        "zookeeper",
        "kafka",
        "schema-registry",
        "namenode",
        "datanode",
        "spark-master",
        "spark-worker",
    }
    required_profiles = {
        "serving": {"hbase", "serving-api", "alert-consumer"},
        "warehouse": {"hive"},
        "orchestration": {"airflow"},
        "monitoring": {
            "kafka-exporter",
            "prometheus",
            "grafana",
            "alertmanager",
        },
        "lakehouse": {
            "postgres",
            "rustfs",
            "rustfs-init",
            "polaris-bootstrap",
            "polaris",
            "polaris-setup",
            "trino",
        },
        "mlops": {
            "mlflow-postgres",
            "mlflow",
            "rustfs",
            "rustfs-init",
        },
        "lineage": {
            "marquez-db",
            "marquez",
            "marquez-web",
        },
    }

    missing_core = required_core - services.keys()
    if missing_core:
        fail(f"missing core services: {sorted(missing_core)}")

    for profile, required in required_profiles.items():
        missing = required - services.keys()
        if missing:
            fail(
                f"missing {profile} services: {sorted(missing)}"
            )

    _validate_dependencies(services)

    required_mounts = [
        "kafka_data:/var/lib/kafka/data",
        "zookeeper_data:/data",
        "hadoop_namenode:/hadoop/dfs/name",
        "hadoop_datanode:/hadoop/dfs/data",
    ]
    for item in required_mounts:
        if item not in base_text:
            fail(f"missing persistent mount {item}")

    required_files = [
        "config/fraud_rules.yml",
        "config/ml_models.yml",
        "config/quality/silver.yml",
        "config/storage_profiles.yml",
        "monitoring/prometheus.yml",
        "monitoring/alerts.yml",
        "monitoring/alertmanager.yml",
        "docker/grafana/provisioning/datasources/prometheus.yml",
        "docker/grafana/provisioning/dashboards/dashboards.yml",
        "docker/grafana/dashboards/fraud-platform.json",
        "docker/serving/Dockerfile",
        "docker/serving/requirements-serving.txt",
        "processing/features/fraud_features.py",
        "processing/quality/gx_validation.py",
        "processing/lakehouse/iceberg_tables.py",
        "processing/lakehouse/maintenance.py",
        "scripts/promote_ensemble.py",
        "scripts/rollback_ensemble.py",
    ]
    for rel in required_files:
        if not (ROOT / rel).exists():
            fail(f"required file missing: {rel}")

    rules = yaml.safe_load(
        (ROOT / "config/fraud_rules.yml").read_text(encoding="utf-8")
    )
    expected_rules = {
        "HIGH_AMOUNT",
        "STRUCTURING",
        "IMPOSSIBLE_TRAVEL",
        "MULE",
        "VELOCITY",
        "NEW_DEVICE_HIGH_VALUE",
    }
    if set(rules["rules"]) != expected_rules:
        fail("fraud rule catalogue mismatch")

    registry = yaml.safe_load(
        (ROOT / "config/ml_models.yml").read_text(encoding="utf-8")
    )["registry"]
    if registry["candidate_alias"] == registry["production_alias"]:
        fail("ML candidate and production aliases must be distinct")
    logical_names = {
        member["logical_name"] for member in registry["members"]
    }
    if logical_names != {
        "logistic_regression",
        "random_forest",
        "gbt",
    }:
        fail("ML registry must contain exactly three ensemble members")

    storage_profiles = yaml.safe_load(
        (ROOT / "config/storage_profiles.yml").read_text(encoding="utf-8")
    )["storage_profiles"]
    if set(storage_profiles) != {"local", "aws", "azure", "gcp"}:
        fail("storage profiles must cover local/aws/azure/gcp")

    for env in ("aws", "azure", "gcp"):
        values = yaml.safe_load(
            (
                ROOT
                / "deploy"
                / "helm"
                / "fraud-platform"
                / f"values-{env}.yaml"
            ).read_text(encoding="utf-8")
        )
        env_vars = values["env"]
        if env_vars["LEGACY_HDFS_ENABLED"] != "false":
            fail(f"{env} Helm profile must disable legacy HDFS")
        if env_vars["HBASE_HOST"] != "":
            fail(f"{env} Helm profile must disable local HBase")

    stream = (
        ROOT / "processing" / "streaming" / "bronze_silver_job.py"
    ).read_text(encoding="utf-8")
    for needle in [
        "build_batch_token",
        "is_committed_iceberg",
        "mark_committed_iceberg",
        "LEGACY_HDFS_ENABLED",
        "spark.catalog.tableExists(table)",
    ]:
        if needle not in stream:
            fail(f"streaming job missing cloud-safety guard: {needle}")

    commit_source = (
        ROOT / "processing" / "streaming" / "batch_commit.py"
    ).read_text(encoding="utf-8")
    if "MERGE INTO" not in commit_source:
        fail("Iceberg commit marker must be idempotent via MERGE")

    helm = (
        ROOT
        / "deploy"
        / "helm"
        / "fraud-platform"
        / "templates"
        / "sparkapplication.yaml"
    ).read_text(encoding="utf-8")
    for needle in [
        "kind: SparkApplication",
        "sparkoperator.k8s.io/v1beta2",
        "runtimeSecret.existingSecret",
        "image.immutableTag",
    ]:
        if needle not in helm:
            fail(f"Helm SparkApplication missing {needle}")

    print("PASS: fraud platform static validation")
    print(f"merged_services={len(services)} rules={len(rules['rules'])}")
    print("profiles=serving,warehouse,orchestration,monitoring,lakehouse,mlops,lineage")
    print("cloud_mode=iceberg-only+hdfs-free+identity-ready")


if __name__ == "__main__":
    main()
