"""apply_ml_ensemble: join-free scoring must match the previous join-based one."""
import pytest

pyspark = pytest.importorskip("pyspark")

from pyspark.ml import Pipeline
from pyspark.ml.classification import LogisticRegression, RandomForestClassifier
from pyspark.ml.functions import vector_to_array
from pyspark.ml.feature import StringIndexer, VectorAssembler
from pyspark.sql import functions as F

from processing.common.spark_session import get_spark
from processing.ml.inference import apply_ml_ensemble


@pytest.fixture(scope="module")
def spark():
    s = get_spark("pytest-ml-inference", enable_hive=False)
    yield s
    s.stop()


def _frame(spark):
    rows = [
        (i, float(i % 7) * 100.0, "Withdrawal" if i % 2 else "Deposit",
         "PASS", 0, float(i % 3 == 0))
        for i in range(1, 61)
    ]
    return spark.createDataFrame(
        rows, "transaction_id long, amount_usd double, txn_type string, "
              "decision string, risk_score int, label double")


def _models(df):
    def pipe(est):
        return Pipeline(stages=[
            StringIndexer(inputCol="txn_type", outputCol="t_idx", handleInvalid="keep"),
            VectorAssembler(inputCols=["amount_usd", "t_idx"], outputCol="features"),
            est.setFeaturesCol("features").setLabelCol("label"),
        ]).fit(df)
    return [pipe(LogisticRegression(maxIter=20)), pipe(RandomForestClassifier(numTrees=5, seed=1))]


def _legacy(df, models, threshold):
    """The previous implementation (transform + join per member)."""
    out, cols = df, []
    for i, m in enumerate(models):
        c = f"_ml_prob_{i}"
        pred = m.transform(out).select("transaction_id", vector_to_array(F.col("probability"))[1].alias(c))
        out = out.join(pred, "transaction_id", "left")
        cols.append(c)
    p = F.array_max(F.array(*[F.coalesce(F.col(c), F.lit(0.0)) for c in cols]))
    out = (out.withColumn("ml_probability", p)
              .withColumn("risk_score", F.greatest(F.col("risk_score"), F.round(p * 100).cast("int"))))
    return (out.withColumn("decision",
                           F.when(F.col("decision") == "BLOCK", "BLOCK")
                            .when(p >= F.lit(threshold), "FLAG")
                            .otherwise(F.col("decision")))
               .drop(*cols))


def test_matches_legacy_join_implementation(spark):
    df = _frame(spark)
    models = _models(df)
    new = apply_ml_ensemble(df, models, threshold=0.5)
    old = _legacy(df, models, 0.5)

    assert sorted(new.columns) == sorted(old.columns)
    cols = sorted(new.columns)
    a = {r["transaction_id"]: r for r in new.select(*cols).collect()}
    b = {r["transaction_id"]: r for r in old.select(*cols).collect()}
    assert a.keys() == b.keys()
    for k in a:
        assert a[k]["decision"] == b[k]["decision"]
        assert a[k]["risk_score"] == b[k]["risk_score"]
        assert a[k]["ml_probability"] == pytest.approx(b[k]["ml_probability"])


def test_no_intermediate_pipeline_columns_leak(spark):
    df = _frame(spark)
    out = apply_ml_ensemble(df, _models(df), threshold=0.5)
    assert set(out.columns) == set(df.columns) | {"ml_probability"}
    assert out.count() == df.count()


def test_probability_extraction_works_on_vector_column(spark):
    """Regression: F.col("probability")[1] raises INVALID_EXTRACT_BASE_FIELD_TYPE
    on Spark 3.5 because MLlib probabilities are Vectors, not arrays."""
    df = _frame(spark)
    scored = _models(df)[0].transform(df)
    with pytest.raises(Exception):
        scored.select(F.col("probability")[1]).collect()
    values = scored.select(vector_to_array(F.col("probability"))[1].alias("p")).collect()
    assert all(0.0 <= r["p"] <= 1.0 for r in values)
