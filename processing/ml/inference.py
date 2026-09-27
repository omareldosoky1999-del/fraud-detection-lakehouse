"""Optional MLlib ensemble inference."""
from __future__ import annotations
from pathlib import Path
from pyspark.ml import PipelineModel
from pyspark.sql import DataFrame, functions as F
NAMES=["logistic_regression","random_forest","gbt"]

def load_models(model_dir):
    root=Path(model_dir); return [PipelineModel.load(str(root/n)) for n in NAMES if (root/n).exists()] if root.exists() else []

def apply_ml_ensemble(df:DataFrame,models,threshold=0.70):
    if not models: return df
    out=df; cols=[]
    for i,m in enumerate(models):
        c=f"_ml_prob_{i}"; pred=m.transform(out).select("transaction_id",F.col("probability")[1].alias(c)); out=out.join(pred,"transaction_id","left"); cols.append(c)
    p=F.array_max(F.array(*[F.coalesce(F.col(c),F.lit(0.0)) for c in cols]))
    out=out.withColumn("ml_probability",p).withColumn("risk_score",F.greatest(F.col("risk_score"),F.round(p*100).cast("int")))
    return out.withColumn("decision",F.when(F.col("decision")=="BLOCK","BLOCK").when(p>=F.lit(threshold),"FLAG").otherwise(F.col("decision"))).drop(*cols)
