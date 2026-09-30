"""MLflow Model Registry contract and helpers for the fraud ensemble."""
from __future__ import annotations

import os
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path("config/ml_models.yml")


def load_registry_config(path: str | Path | None = None) -> dict:
    config_path = Path(path or os.getenv("MLFLOW_MODEL_CONFIG", DEFAULT_CONFIG))
    data = yaml.safe_load(config_path.read_text(encoding="utf-8")) or {}
    registry = data.get("registry", {})
    if not registry.get("members"):
        raise ValueError(f"No model registry members configured in {config_path}")
    return registry


def registry_members(path: str | Path | None = None) -> list[dict]:
    return load_registry_config(path)["members"]


def production_alias(path: str | Path | None = None) -> str:
    configured = os.getenv("MLFLOW_MODEL_ALIAS")
    return configured or load_registry_config(path).get("production_alias", "production")


def candidate_alias(path: str | Path | None = None) -> str:
    configured = os.getenv("MLFLOW_CANDIDATE_ALIAS")
    return configured or load_registry_config(path).get("candidate_alias", "candidate")


def decision_threshold(path: str | Path | None = None) -> float:
    configured = os.getenv("ML_THRESHOLD")
    if configured is not None:
        return float(configured)
    return float(load_registry_config(path).get("decision_threshold", 0.70))
