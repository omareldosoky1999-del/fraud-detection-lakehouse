"""Spark MLlib ensemble inference from MLflow Registry with local fallback."""
from __future__ import annotations

import os
from pathlib import Path

from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, functions as F

from processing.ml.registry import production_alias, registry_members

NAMES = ["logistic_regression", "random_forest", "gbt"]


def _load_local_models(model_dir: str | None) -> list[PipelineModel]:
    if not model_dir:
        return []
    root = Path(model_dir)
    if not root.exists():
        return []
    return [
        PipelineModel.load(str(root / name))
        for name in NAMES
        if (root / name).exists()
    ]


def _load_registered_models(
    config_path: str | None = None,
    alias: str | None = None,
) -> list[PipelineModel]:
    tracking_uri = os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        return []

    import mlflow
    import mlflow.spark

    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(os.getenv("MLFLOW_REGISTRY_URI", tracking_uri))

    selected_alias = alias or production_alias(config_path)
    models: list[PipelineModel] = []

    for member in registry_members(config_path):
        name = member["registered_name"]
        uri = f"models:/{name}@{selected_alias}"
        models.append(mlflow.spark.load_model(uri))

    if not models:
        raise RuntimeError(
            f"MLflow registry returned no models for alias '{selected_alias}'"
        )
    return models


def load_models(
    model_dir: str | None = None,
    *,
    registry_config: str | None = None,
    alias: str | None = None,
) -> list[PipelineModel]:
    """Load the production ensemble from MLflow when configured.

    Local artifacts remain an explicit fallback for offline development.
    """
    if os.getenv("MLFLOW_TRACKING_URI"):
        return _load_registered_models(registry_config, alias)
    return _load_local_models(model_dir)


def apply_ml_ensemble(
    df: DataFrame,
    models: list[PipelineModel],
    threshold: float = 0.70,
) -> DataFrame:
    if not models:
        raise RuntimeError(
            "ML is a core fraud decision component, but no trained models were supplied."
        )

    out = df
    cols = []

    for i, model in enumerate(models):
        col = f"_ml_prob_{i}"
        prediction = (
            model.transform(out)
            .select("transaction_id", F.col("probability")[1].alias(col))
        )
        out = out.join(prediction, "transaction_id", "left")
        cols.append(col)

    probability = F.array_max(
        F.array(*[F.coalesce(F.col(col), F.lit(0.0)) for col in cols])
    )

    out = (
        out.withColumn("ml_probability", probability)
        .withColumn(
            "risk_score",
            F.greatest(
                F.col("risk_score"),
                F.round(probability * 100).cast("int"),
            ),
        )
    )

    return (
        out.withColumn(
            "decision",
            F.when(F.col("decision") == "BLOCK", "BLOCK")
            .when(probability >= F.lit(threshold), "FLAG")
            .otherwise(F.col("decision")),
        )
        .drop(*cols)
    )
