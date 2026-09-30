from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_cloud_deploy_workflow_requires_immutable_tag():
    text = (ROOT / ".github" / "workflows" / "deploy-helm.yml").read_text(
        encoding="utf-8"
    )
    assert "workflow_dispatch" in text
    assert "image_tag:" in text
    assert "image.immutableTag" in text
    assert "--atomic" in text
    assert 'grep -q ":latest"' in text

    # The workflow resolves the environment-specific values file dynamically.
    assert "file=deploy/helm/fraud-platform/values-${{ inputs.environment }}.yaml" in text

    for env in ["aws", "azure", "gcp"]:
        assert f"values-{env}.yaml" in text
        assert env in text.split("case")[1].split("esac")[0]


def test_cloud_deploy_requires_explicit_non_aws_identity_reference():
    text = (
        ROOT / ".github" / "workflows" / "deploy-helm.yml"
    ).read_text(encoding="utf-8")
    assert "workload_identity_ref:" in text
    assert 'test -n "$IDENTITY_REF"' in text
    assert "serviceAccount.annotations.azure" in text
    assert "serviceAccount.annotations.iam" in text



def test_cloud_values_disable_local_hdfs_and_hbase_dependencies():
    import yaml

    root = ROOT / "deploy" / "helm" / "fraud-platform"
    for env in ["aws", "azure", "gcp"]:
        data = yaml.safe_load(
            (root / f"values-{env}.yaml").read_text(encoding="utf-8")
        )
        assert data["env"]["LEGACY_HDFS_ENABLED"] == "false"
        assert data["env"]["HBASE_HOST"] == ""


def test_airflow_gold_dag_is_iceberg_first():
    text = (ROOT / "orchestration" / "dags" / "fraud_gold_dag.py").read_text(encoding="utf-8")
    assert "--iceberg-catalog polaris" in text
    assert "--iceberg-table silver.transactions" in text
    assert "processing/batch/build_gold.py" in text
    assert "LEGACY_HDFS_ORCHESTRATION" in text


def test_full_stack_e2e_uses_iceberg_for_evaluation():
    text = (ROOT / ".github" / "workflows" / "full-stack-e2e.yml").read_text(encoding="utf-8")
    assert "export_iceberg_decisions.py" in text
    assert "--table gold.fraud_decisions" in text
    assert "hdfs dfs -get /warehouse/gold/fraud_decisions" not in text
