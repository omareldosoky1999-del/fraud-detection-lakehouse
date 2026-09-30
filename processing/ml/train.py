"""Train Spark MLlib fraud models and manage them through MLflow."""
from __future__ import annotations

import argparse
import json
import os
from datetime import datetime
from pathlib import Path

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from ingestion.generator.transaction_generator import generate
from processing.common.spark_session import get_spark
from processing.features.fraud_features import add_fraud_features
from processing.ml.registry import candidate_alias, load_registry_config, registry_members
from processing.streaming.transform import to_silver

CATEGORICAL = ["txn_type", "status", "currency", "country_src", "country_dest"]
NUMERIC = [
    "amount_usd",
    "txn_count_5m",
    "amount_sum_1h",
    "avg_amount_prior_30",
    "stddev_amount_prior_30",
    "seconds_since_prev_txn",
    "device_new_30d",
    "country_changed",
    "amount_to_prior_avg",
]


def _pipeline(estimator):
    idx = [
        StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep")
        for c in CATEGORICAL
    ]
    enc = OneHotEncoder(
        inputCols=[f"{c}_idx" for c in CATEGORICAL],
        outputCols=[f"{c}_ohe" for c in CATEGORICAL],
    )
    asm = VectorAssembler(
        inputCols=NUMERIC + [f"{c}_ohe" for c in CATEGORICAL],
        outputCol="features",
        handleInvalid="keep",
    )
    return Pipeline(
        stages=idx + [enc, asm, estimator.setFeaturesCol("features").setLabelCol("label")]
    )


def _register_spark_model(
    *,
    model,
    run_id: str,
    artifact_path: str,
    registered_name: str,
    client: MlflowClient,
    metadata: dict,
):
    mlflow.spark.log_model(
        spark_model=model,
        artifact_path=artifact_path,
        registered_model_name=registered_name,
        dfs_tmpdir="/tmp/mlflow-spark-tmp",
    )

    versions = client.search_model_versions(f"name='{registered_name}'")
    candidates = [v for v in versions if v.run_id == run_id]
    if not candidates:
        raise RuntimeError(
            f"MLflow did not expose a registered version for {registered_name} "
            f"from run {run_id}"
        )
    version = max(candidates, key=lambda v: int(v.version))

    for key, value in metadata.items():
        client.set_model_version_tag(
            name=registered_name,
            version=version.version,
            key=key,
            value=str(value),
        )

    return version.version


def train(
    n: int,
    model_dir: str,
    seed: int = 42,
    tracking_uri: str | None = None,
    experiment: str = "fraud-detection/spark-ml-ensemble",
    registry_config: str = "config/ml_models.yml",
    promote_alias: str | None = None,
    ml_threshold: float | None = None,
):
    tracking_uri = tracking_uri or os.getenv("MLFLOW_TRACKING_URI")
    if not tracking_uri:
        raise RuntimeError(
            "MLFLOW_TRACKING_URI is required. "
            "Use an explicit local/offline mode only for isolated development."
        )

    os.environ["MLFLOW_TRACKING_URI"] = tracking_uri
    mlflow.set_tracking_uri(tracking_uri)
    mlflow.set_registry_uri(tracking_uri)
    mlflow.set_experiment(experiment)

    registry = load_registry_config(registry_config)
    alias = (
        promote_alias
        or os.getenv("MLFLOW_PROMOTE_ALIAS")
        or candidate_alias(registry_config)
    )
    min_validation_auc = float(
        os.getenv(
            "MLFLOW_MIN_VALIDATION_AUC",
            registry.get("min_validation_auc", 0.65),
        )
    )
    decision_threshold = (
        float(ml_threshold)
        if ml_threshold is not None
        else float(os.getenv("ML_THRESHOLD", registry.get("decision_threshold", 0.70)))
    )
    members = {m["logical_name"]: m["registered_name"] for m in registry_members(registry_config)}

    spark = get_spark("fraud-ml-train", enable_hive=False)
    train_df = None
    validation_df = None
    try:
        events = generate(
            n,
            seed=seed,
            start=datetime(2026, 9, 1),
            data_dir="ingestion/data",
        )
        raw = spark.createDataFrame(
            [{**e.rec, "label": float(e.is_fraud)} for e in events]
        )
        silver = add_fraud_features(
            to_silver(raw).withColumn("label", F.col("label").cast("double"))
        )

        cutoff = (
            silver.select(
                F.expr("percentile_approx(event_time, 0.8)").alias("cutoff")
            )
            .first()["cutoff"]
        )
        if cutoff is None:
            raise ValueError("Unable to derive time-based validation cutoff")

        train_df = silver.filter(F.col("event_time") < F.lit(cutoff)).cache()
        validation_df = silver.filter(F.col("event_time") >= F.lit(cutoff)).cache()
        train_count = train_df.count()
        validation_count = validation_df.count()
        if train_count == 0 or validation_count == 0:
            raise ValueError(
                f"Invalid temporal split: train={train_count}, validation={validation_count}"
            )

        out = Path(model_dir)
        out.mkdir(parents=True, exist_ok=True)

        positives = train_df.filter(F.col("label") == 1.0).count()
        negatives = train_df.filter(F.col("label") == 0.0).count()
        if positives == 0 or negatives == 0:
            raise ValueError(
                f"Training split must contain both classes: positives={positives}, "
                f"negatives={negatives}"
            )

        positive_weight = negatives / positives
        weighted_train = train_df.withColumn(
            "class_weight",
            F.when(F.col("label") == 1.0, F.lit(positive_weight))
            .otherwise(F.lit(1.0)),
        ).cache()

        evaluator_roc = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderROC",
        )
        evaluator_pr = BinaryClassificationEvaluator(
            labelCol="label",
            rawPredictionCol="rawPrediction",
            metricName="areaUnderPR",
        )
        estimators = {
            "logistic_regression": LogisticRegression(
                maxIter=50,
                regParam=0.05,
                weightCol="class_weight",
            ),
            "random_forest": RandomForestClassifier(
                numTrees=80,
                maxDepth=10,
                seed=seed,
                weightCol="class_weight",
            ),
            "gbt": GBTClassifier(
                maxIter=60,
                maxDepth=5,
                seed=seed,
                weightCol="class_weight",
            ),
        }

        metrics = {
            "spark_version": spark.version,
            "mlflow_version": mlflow.__version__,
            "cutoff": str(cutoff),
            "train_rows": train_count,
            "validation_rows": validation_count,
            "validation_strategy": "time_based_80_20",
        }
        registered_versions = {}

        client = MlflowClient()
        with mlflow.start_run(run_name=f"fraud-ensemble-{seed}") as run:
            mlflow.log_params(
                {
                    "seed": seed,
                    "events": n,
                    "train_rows": train_count,
                    "validation_rows": validation_count,
                    "validation_strategy": "time_based_80_20",
                    "spark_version": spark.version,
                    "min_validation_auc": min_validation_auc,
                    "decision_threshold": decision_threshold,
                    "positive_weight": positive_weight,
                    "train_positive_rows": positives,
                    "train_negative_rows": negatives,
                }
            )

            for logical_name, estimator in estimators.items():
                model = _pipeline(estimator).fit(weighted_train)
                predictions = model.transform(validation_df)
                auc = float(evaluator_roc.evaluate(predictions))
                auprc = float(evaluator_pr.evaluate(predictions))

                scored = predictions.withColumn(
                    "_ml_score",
                    F.col("probability")[1],
                ).withColumn(
                    "_ml_decision",
                    F.when(
                        F.col("_ml_score") >= F.lit(decision_threshold),
                        F.lit(1.0),
                    ).otherwise(F.lit(0.0)),
                )
                tp = scored.filter(
                    (F.col("label") == 1.0) & (F.col("_ml_decision") == 1.0)
                ).count()
                fp = scored.filter(
                    (F.col("label") == 0.0) & (F.col("_ml_decision") == 1.0)
                ).count()
                fn = scored.filter(
                    (F.col("label") == 1.0) & (F.col("_ml_decision") == 0.0)
                ).count()
                precision = tp / (tp + fp) if (tp + fp) else 0.0
                recall = tp / (tp + fn) if (tp + fn) else 0.0
                f1 = (
                    2 * precision * recall / (precision + recall)
                    if (precision + recall)
                    else 0.0
                )

                model.write().overwrite().save(str(out / logical_name))

                if auc < min_validation_auc:
                    raise RuntimeError(
                        f"{logical_name} validation AUC {auc:.4f} is below the "
                        f"production threshold {min_validation_auc:.4f}; ensemble "
                        "promotion is blocked."
                    )

                registered_name = members[logical_name]
                mlflow.log_metric(f"{logical_name}_validation_auc", auc)
                mlflow.log_metric(f"{logical_name}_validation_auprc", auprc)
                mlflow.log_metric(f"{logical_name}_validation_precision", precision)
                mlflow.log_metric(f"{logical_name}_validation_recall", recall)
                mlflow.log_metric(f"{logical_name}_validation_f1", f1)

                version = _register_spark_model(
                    model=model,
                    run_id=run.info.run_id,
                    artifact_path=f"model_{logical_name}",
                    registered_name=registered_name,
                    client=client,
                    metadata={
                        "validation_status": "PASSED",
                        "validation_auc": auc,
                        "validation_auprc": auprc,
                        "validation_precision": precision,
                        "validation_recall": recall,
                        "validation_f1": f1,
                        "decision_threshold": decision_threshold,
                        "ensemble_member": "true",
                        "ensemble_run_id": run.info.run_id,
                        "spark_version": spark.version,
                        "validation_strategy": "time_based_80_20",
                        "promotion_threshold": min_validation_auc,
                    },
                )
                registered_versions[logical_name] = {
                    "registered_name": registered_name,
                    "version": version,
                }

                metrics[logical_name] = {
                    "validation_auc": auc,
                    "validation_auprc": auprc,
                    "validation_precision": precision,
                    "validation_recall": recall,
                    "validation_f1": f1,
                    "decision_threshold": decision_threshold,
                    "min_validation_auc": min_validation_auc,
                    "registered_name": registered_name,
                    "registry_version": str(version),
                    "promotion_alias": alias,
                }

                print(
                    f"[MLFLOW] {logical_name}: auc={auc:.4f}, auprc={auprc:.4f}, "
                    f"precision@{decision_threshold:.2f}={precision:.4f}, "
                    f"recall@{decision_threshold:.2f}={recall:.4f}, "
                    f"registered={registered_name}@{version}; pending alias={alias}"
                )

            # Promote the complete ensemble only after every member has
            # trained, validated and registered successfully.
            for member in registered_versions.values():
                client.set_registered_model_alias(
                    member["registered_name"],
                    alias,
                    member["version"],
                )

            metrics["ensemble_release"] = {
                "run_id": run.info.run_id,
                "promotion_alias": alias,
                "production_alias": registry.get("production_alias", "production"),
                "members": registered_versions,
                "promotion_policy": (
                    "coordinated_alias_promotion_after_full_registration_and_validation_gate"
                ),
                "min_validation_auc": min_validation_auc,
            }

            manifest_path = out / "ensemble_manifest.json"
            manifest_path.write_text(
                json.dumps(metrics["ensemble_release"], indent=2),
                encoding="utf-8",
            )
            mlflow.log_artifact(str(manifest_path), artifact_path="ensemble_release")

            print(
                f"[MLFLOW] ensemble release promoted: "
                f"alias={alias}, run_id={run.info.run_id}"
            )

            metrics_path = out / "training_metrics.json"
            metrics_path.write_text(
                json.dumps(metrics, indent=2),
                encoding="utf-8",
            )
            mlflow.log_artifact(str(metrics_path), artifact_path="training_metadata")
            mlflow.set_tags(
                {
                    "pipeline": "fraud_detection",
                    "model_family": "spark_mllib_ensemble",
                    "registry_alias": alias,
                    "training_status": "completed",
                }
            )

            print(f"[MLFLOW] run_id={run.info.run_id}")

    finally:
        if train_df is not None:
            train_df.unpersist()
        if validation_df is not None:
            validation_df.unpersist()
        if "weighted_train" in locals() and weighted_train is not None:
            weighted_train.unpersist()
        spark.stop()


def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model-dir", default="models/fraud_ensemble")
    parser.add_argument("--tracking-uri", default=os.getenv("MLFLOW_TRACKING_URI"))
    parser.add_argument(
        "--experiment",
        default="fraud-detection/spark-ml-ensemble",
    )
    parser.add_argument(
        "--registry-config",
        default=os.getenv("MLFLOW_MODEL_CONFIG", "config/ml_models.yml"),
    )
    parser.add_argument(
        "--promote-alias",
        default=os.getenv("MLFLOW_PROMOTE_ALIAS"),
        help="Alias assigned after validation; defaults to the candidate alias, not production.",
    )
    parser.add_argument(
        "--ml-threshold",
        type=float,
        default=os.getenv("ML_THRESHOLD"),
        help="Probability threshold used for validation precision/recall metrics.",
    )
    args = parser.parse_args(argv)
    train(
        args.n,
        args.model_dir,
        args.seed,
        tracking_uri=args.tracking_uri,
        experiment=args.experiment,
        registry_config=args.registry_config,
        promote_alias=args.promote_alias,
        ml_threshold=args.ml_threshold,
    )


if __name__ == "__main__":
    main()
