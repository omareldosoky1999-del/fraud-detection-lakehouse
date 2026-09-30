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
