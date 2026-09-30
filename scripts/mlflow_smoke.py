"""End-to-end smoke test for Spark MLlib -> MLflow Tracking -> Model Registry."""
from __future__ import annotations

import mlflow
import mlflow.spark
from mlflow import MlflowClient
from pyspark.ml.classification import LogisticRegression
from pyspark.sql import Row

from processing.common.spark_session import get_spark


MODEL_NAME = "fraud_smoke_model"
ALIAS = "smoke"


def main():
    spark = get_spark("mlflow-smoke", enable_hive=False)
    mlflow.set_tracking_uri("http://mlflow:5000")
    mlflow.set_registry_uri("http://mlflow:5000")
    mlflow.set_experiment("fraud-detection/mlflow-smoke")

    data = spark.createDataFrame(
        [
            Row(label=0.0, x1=0.0, x2=0.0),
            Row(label=0.0, x1=0.1, x2=0.2),
            Row(label=1.0, x1=2.0, x2=2.1),
            Row(label=1.0, x1=2.2, x2=1.8),
            Row(label=0.0, x1=0.2, x2=0.0),
            Row(label=1.0, x1=1.8, x2=2.2),
        ]
    )

    from pyspark.ml.feature import VectorAssembler

    features = VectorAssembler(inputCols=["x1", "x2"], outputCol="features").transform(data)
    model = LogisticRegression(maxIter=10, featuresCol="features", labelCol="label").fit(features)

    with mlflow.start_run(run_name="spark-mllib-registry-smoke") as run:
        mlflow.log_param("runtime", spark.version)
        mlflow.log_metric("smoke_auc", 1.0)
        mlflow.spark.log_model(
            spark_model=model,
            artifact_path="spark_model",
            registered_model_name=MODEL_NAME,
            dfs_tmpdir="/tmp/mlflow-spark-tmp",
        )

        client = MlflowClient()
        versions = client.search_model_versions(f"name='{MODEL_NAME}'")
        candidates = [v for v in versions if v.run_id == run.info.run_id]
        if not candidates:
            raise RuntimeError("Registered Spark model version not found")

        version = max(candidates, key=lambda v: int(v.version))
        client.set_registered_model_alias(MODEL_NAME, ALIAS, version.version)
        resolved = client.get_model_version_by_alias(MODEL_NAME, ALIAS)

        assert str(resolved.version) == str(version.version)
        print(
            f"[MLFLOW_SMOKE] registered {MODEL_NAME}@{version.version}; "
            f"alias={ALIAS}; run_id={run.info.run_id}"
        )

    spark.stop()


if __name__ == "__main__":
    main()
