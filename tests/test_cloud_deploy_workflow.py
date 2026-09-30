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
    assert "! grep -q ':latest'" in text
    for env in ["aws", "azure", "gcp"]:
        assert f"values-{env}.yaml" in text

def test_cloud_deploy_requires_explicit_non_aws_identity_reference():
    text = (
        ROOT / ".github" / "workflows" / "deploy-helm.yml"
    ).read_text(encoding="utf-8")
    assert "workload_identity_ref:" in text
    assert "test -n \"$IDENTITY_REF\"" in text
    assert "serviceAccount.annotations.azure" in text
    assert "serviceAccount.annotations.iam" in text
