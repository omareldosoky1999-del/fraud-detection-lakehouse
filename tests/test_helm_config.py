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


def test_helm_supports_immutable_image_and_runtime_secret():
    values = yaml.safe_load((ROOT / "values.yaml").read_text(encoding="utf-8"))
    app = (ROOT / "templates" / "sparkapplication.yaml").read_text(encoding="utf-8")

    assert "immutableTag" in values["image"]
    assert "existingSecret" in values["runtimeSecret"]
    assert "$imageTag" in app
    assert "secretRef" in app



def test_cloud_values_define_workload_identity_contracts():
    azure = yaml.safe_load((ROOT / "values-azure.yaml").read_text(encoding="utf-8"))
    gcp = yaml.safe_load((ROOT / "values-gcp.yaml").read_text(encoding="utf-8"))
    aws = yaml.safe_load((ROOT / "values-aws.yaml").read_text(encoding="utf-8"))

    assert azure["serviceAccount"]["podLabels"]["azure.workload.identity/use"] == "true"
    assert azure["serviceAccount"]["annotations"] == {}
    assert gcp["serviceAccount"]["annotations"] == {}
    assert aws["serviceAccount"]["annotations"] == {}


def test_external_secret_template_and_cloud_contracts():
    template = (ROOT / "templates" / "externalsecret.yaml").read_text(encoding="utf-8")
    values = yaml.safe_load((ROOT / "values.yaml").read_text(encoding="utf-8"))

    assert "apiVersion: external-secrets.io/v1" in template
    assert "kind: ExternalSecret" in template
    assert "secretStoreRef:" in template
    assert "remoteRef:" in template
    assert values["externalSecrets"]["enabled"] is False
    assert values["externalSecrets"]["targetName"] == "fraud-platform-runtime"

    for env in ["aws", "azure", "gcp"]:
        cfg = yaml.safe_load(
            (ROOT / f"values-{env}.yaml").read_text(encoding="utf-8")
        )
        assert cfg["externalSecrets"]["enabled"] is False
        assert cfg["externalSecrets"]["secretStoreRef"]["kind"] == "ClusterSecretStore"
        assert cfg["externalSecrets"]["targetName"] == "fraud-platform-runtime"


def test_helm_does_not_duplicate_pod_labels():
    app = (ROOT / "templates" / "sparkapplication.yaml").read_text(encoding="utf-8")
    assert app.count("range $key, $value := .Values.serviceAccount.podLabels") == 2
