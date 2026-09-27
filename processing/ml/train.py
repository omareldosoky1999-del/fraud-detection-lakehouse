"""Train optional Spark MLlib fraud models from generator ground truth."""
from __future__ import annotations
from datetime import datetime
from pathlib import Path
import argparse
from pyspark.ml import Pipeline
from pyspark.ml.classification import GBTClassifier, LogisticRegression, RandomForestClassifier
from pyspark.ml.feature import OneHotEncoder, StringIndexer, VectorAssembler
from pyspark.sql import functions as F
from ingestion.generator.transaction_generator import generate
from processing.common.spark_session import get_spark
from processing.streaming.transform import to_silver
CATEGORICAL=["txn_type","status","currency","country_src","country_dest"]
NUMERIC=["amount_usd","card_id","device_id","client_id"]

def _pipeline(estimator):
    idx=[StringIndexer(inputCol=c,outputCol=f"{c}_idx",handleInvalid="keep") for c in CATEGORICAL]
    enc=OneHotEncoder(inputCols=[f"{c}_idx" for c in CATEGORICAL],outputCols=[f"{c}_ohe" for c in CATEGORICAL])
    asm=VectorAssembler(inputCols=NUMERIC+[f"{c}_ohe" for c in CATEGORICAL],outputCol="features",handleInvalid="keep")
    return Pipeline(stages=idx+[enc,asm,estimator.setFeaturesCol("features").setLabelCol("label")])

def train(n,model_dir,seed=42):
    spark=get_spark("fraud-ml-train",enable_hive=False)
    try:
        events=generate(n,seed=seed,start=datetime(2026,9,1),data_dir="ingestion/data")
        raw=spark.createDataFrame([{**e.rec,"label":float(e.is_fraud)} for e in events])
        silver=to_silver(raw).withColumn("label",F.col("label").cast("double"))
        train_df,_=silver.randomSplit([0.8,0.2],seed=seed)
        out=Path(model_dir); out.mkdir(parents=True,exist_ok=True)
        models={"logistic_regression":LogisticRegression(maxIter=50,regParam=0.05),"random_forest":RandomForestClassifier(numTrees=80,maxDepth=10,seed=seed),"gbt":GBTClassifier(maxIter=60,maxDepth=5,seed=seed)}
        for name,est in models.items():
            _pipeline(est).fit(train_df).write().overwrite().save(str(out/name))
    finally:
        spark.stop()

def main(argv=None):
    p=argparse.ArgumentParser(); p.add_argument("--n",type=int,default=20000); p.add_argument("--seed",type=int,default=42); p.add_argument("--model-dir",default="models/fraud_ensemble"); a=p.parse_args(argv); train(a.n,a.model_dir,a.seed)
if __name__=="__main__": main()
