"""Train the core Spark MLlib fraud ensemble with temporal validation."""
from __future__ import annotations

import argparse
import json
from datetime import datetime
from pathlib import Path

from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from ingestion.generator.transaction_generator import generate
from processing.common.spark_session import get_spark
from processing.streaming.transform import to_silver

CATEGORICAL = ["txn_type", "status", "currency", "country_src", "country_dest"]
NUMERIC = ["amount_usd", "card_id", "device_id", "client_id"]

def _pipeline(estimator):
    idx = [StringIndexer(inputCol=c, outputCol=f"{c}_idx", handleInvalid="keep") for c in CATEGORICAL]
    enc = OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL], outputCols=[f"{c}_ohe" for c in CATEGORICAL])
    asm = VectorAssembler(inputCols=NUMERIC + [f"{c}_ohe" for c in CATEGORICAL], outputCol="features", handleInvalid="keep")
    return Pipeline(stages=idx + [enc, asm, estimator.setFeaturesCol("features").setLabelCol("label")])

def train(n: int, model_dir: str, seed: int = 42):
    spark = get_spark("fraud-ml-train", enable_hive=False)
    try:
        events = generate(n, seed=seed, start=datetime(2026, 9, 1), data_dir="ingestion/data")
        raw = spark.createDataFrame([{**e.rec, "label": float(e.is_fraud)} for e in events])
        silver = to_silver(raw).withColumn("label", F.col("label").cast("double"))

        cutoff = silver.select(F.expr("percentile_approx(event_time, 0.8)").alias("cutoff")).first()["cutoff"]
        if cutoff is None: raise ValueError("Unable to derive time-based validation cutoff")
        train_df = silver.filter(F.col("event_time") < F.lit(cutoff)).cache()
        validation_df = silver.filter(F.col("event_time") >= F.lit(cutoff)).cache()
        train_count = train_df.count()
        validation_count = validation_df.count()
        if train_count == 0 or validation_count == 0:
            raise ValueError(f"Invalid temporal split: train={train_count}, validation={validation_count}")

        out = Path(model_dir)
        out.mkdir(parents=True, exist_ok=True)
        evaluator = BinaryClassificationEvaluator(labelCol="label", rawPredictionCol="rawPrediction", metricName="areaUnderROC")
        estimators = {
            "logistic_regression": LogisticRegression(maxIter=50, regParam=0.05),
            "random_forest": RandomForestClassifier(numTrees=80, maxDepth=10, seed=seed),
            "gbt": GBTClassifier(maxIter=60, maxDepth=5, seed=seed),
        }
        metrics = {"spark_version": spark.version, "cutoff": str(cutoff), "train_rows": train_count, "validation_rows": validation_count}

        for name, estimator in estimators.items():
            model = _pipeline(estimator).fit(train_df)
            predictions = model.transform(validation_df)
            auc = evaluator.evaluate(predictions)
            model.write().overwrite().save(str(out / name))
            metrics[name] = {"validation_auc": float(auc)}
            print(f"[ML] {name}: validation_auc={auc:.4f}")

        (out / "training_metrics.json").write_text(json.dumps(metrics, indent=2), encoding="utf-8")
    finally:
        spark.stop()

def main(argv=None):
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=20000)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument("--model-dir", default="models/fraud_ensemble")
    args = parser.parse_args(argv)
    train(args.n, args.model_dir, args.seed)

if __name__ == "__main__":
    main()
