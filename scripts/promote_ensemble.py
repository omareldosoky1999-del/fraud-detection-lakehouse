"""Promote one validated fraud ensemble release from candidate to production."""
from __future__ import annotations

import argparse
import os

from mlflow import MlflowClient

from processing.ml.registry import candidate_alias, load_registry_config, production_alias, registry_members


def promote(
    tracking_uri: str,
    registry_config: str = "config/ml_models.yml",
    candidate: str | None = None,
    production: str | None = None,
    expected_run_id: str | None = None,
):
    os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    client = MlflowClient(tracking_uri=tracking_uri)

    candidate_name = candidate or candidate_alias(registry_config)
    production_name = production or production_alias(registry_config)
    versions = []
    release_ids = set()

    for member in registry_members(registry_config):
        name = member["registered_name"]
        version = client.get_model_version_by_alias(name, candidate_name)
        if version.tags.get('validation_status') != 'PASSED':
            raise RuntimeError(f'{name}@{version.version} is not validation-approved')
        release_id = version.tags.get('ensemble_run_id')
        if not release_id:
            raise RuntimeError(f'{name}@{version.version} has no ensemble_run_id')
        release_ids.add(release_id)
        versions.append((name, version.version, release_id))

    if len(release_ids) != 1:
        raise RuntimeError(
            f"Candidate ensemble is mixed across training releases: {sorted(release_ids)}"
        )

    release_id = next(iter(release_ids))
    if expected_run_id and release_id != expected_run_id:
        raise RuntimeError(
            f"Candidate release {release_id} does not match expected run {expected_run_id}"
        )

    for name, version, _ in versions:
        client.set_registered_model_alias(name, production_name, version)

    formatted = ", ".join(f"{name}@{version}" for name, version, _ in versions)
    print(f"Promoted ensemble release {release_id} from {candidate_name} to {production_name}: {formatted}")
    return release_id, versions


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument('--tracking-uri', default=os.getenv('MLFLOW_TRACKING_URI'))
    parser.add_argument('--registry-config', default=os.getenv('MLFLOW_MODEL_CONFIG', 'config/ml_models.yml'))
    parser.add_argument('--candidate')
    parser.add_argument('--production')
    parser.add_argument('--expected-run-id')
    args = parser.parse_args(argv)
    if not args.tracking_uri:
        raise SystemExit('--tracking-uri or MLFLOW_TRACKING_URI is required')
    promote(
        args.tracking_uri,
        args.registry_config,
        args.candidate,
        args.production,
        args.expected_run_id,
    )


if __name__ == "__main__":
    main()
