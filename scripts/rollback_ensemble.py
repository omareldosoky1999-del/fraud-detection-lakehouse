"""Rollback the production fraud ensemble to a validated MLflow run."""
from __future__ import annotations

import argparse
import os

from mlflow import MlflowClient

from processing.ml.registry import production_alias, registry_members


def rollback(
    tracking_uri: str,
    target_run_id: str,
    registry_config: str = "config/ml_models.yml",
    production: str | None = None,
):
    os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    client = MlflowClient(tracking_uri=tracking_uri)
    production_name = production or production_alias(registry_config)

    members = registry_members(registry_config)
    # registry_members normally provides logical_name. Keep rollback
    # backward-compatible with test/legacy registry entries that only expose
    # registered_name (fraud_<logical_name>).
    expected_members = {
        member.get("logical_name")
        or member["registered_name"].removeprefix("fraud_")
        for member in members
    }
    if expected_members != {
        "logistic_regression",
        "random_forest",
        "gbt",
    }:
        raise RuntimeError(
            "Fraud ensemble registry contract must contain exactly "
            "logistic_regression, random_forest and gbt."
        )

    target_versions = []
    for member in members:
        name = member["registered_name"]
        versions = client.search_model_versions(f"name='{name}'")
        matches = [
            v
            for v in versions
            if v.run_id == target_run_id
            and v.tags.get("ensemble_run_id") == target_run_id
        ]
        if not matches:
            raise RuntimeError(
                f"No registered {name} version found for target run {target_run_id}"
            )

        version = max(matches, key=lambda v: int(v.version))
        if version.tags.get("validation_status") != "PASSED":
            raise RuntimeError(
                f"{name}@{version.version} is not validation-approved"
            )
        target_versions.append((name, version.version))

    for name, version in target_versions:
        client.set_registered_model_alias(name, production_name, version)

    formatted = ", ".join(f"{name}@{version}" for name, version in target_versions)
    print(f"Rolled back production ensemble to run {target_run_id}: {formatted}")
    return target_run_id, target_versions


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI"))
    parser.add_argument("--target-run-id", required=True)
    parser.add_argument(
        "--registry-config",
        default=os.getenv("MLFLOW_MODEL_CONFIG", "config/ml_models.yml"),
    )
    parser.add_argument("--production")
    args = parser.parse_args(argv)

    if not args.tracking_uri:
        raise SystemExit("--tracking-uri or MLFLOW_TRACKING_URI is required")

    rollback(
        args.tracking_uri,
        args.target_run_id,
        args.registry_config,
        args.production,
    )


if __name__ == "__main__":
    main()
