from pathlib import Path
import yaml

ROOT = Path(__file__).parents[1] / "deploy" / "helm" / "fraud-platform"


def test_helm_chart_and_spark_application_contract():
    chart = yaml.safe_load((ROOT / "Chart.yaml").read_text(encoding="utf-8"))
    values = yaml.safe_load((ROOT / "values.yaml").read_text(encoding="utf-8"))
    app = (ROOT / "templates" / "sparkapplication.yaml").read_text(encoding="utf-8")

    assert chart["apiVersion"] == "v2"
    assert values["spark"]["version"] == "3.5.9"
    assert values["spark"]["restartPolicy"] == "Always"
    assert "sparkoperator.k8s.io/v1beta2" in app
    assert "kind: SparkApplication" in app
    assert "io.openlineage.spark.agent.OpenLineageSparkListener" in app
    assert "MLFLOW_MODEL_ALIAS" in values["env"]
