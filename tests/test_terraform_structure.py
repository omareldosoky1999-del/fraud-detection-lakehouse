from pathlib import Path

ROOT = Path(__file__).parents[1] / "infrastructure" / "terraform"


def test_terraform_environment_roots_exist():
    for env in ["local", "aws", "azure", "gcp"]:
        path = ROOT / "environments" / env
        assert (path / "main.tf").exists()
        assert (path / "variables.tf").exists() or env == "local"


def test_cloud_provider_versions_are_pinned():
    expected = {
        "aws": 'version = "6.61.0"',
        "azure": 'version = "5.7.0"',
        "gcp": 'version = "8.2.0"',
    }
    for env, marker in expected.items():
        text = (ROOT / "environments" / env / "versions.tf").read_text(encoding="utf-8")
        assert marker in text


def test_no_terraform_state_is_tracked_by_ignore_rules():
    gitignore = (ROOT.parents[1] / ".gitignore").read_text(encoding="utf-8")
    assert "*.tfstate" in gitignore
    assert ".terraform/" in gitignore


def test_cloud_network_modules_exist():
    for module in ["aws_vpc", "aws_eks", "azure_vnet", "azure_aks", "gcp_vpc", "gcp_gke"]:
        path = ROOT / "modules" / module
        assert (path / "main.tf").exists()
        assert (path / "variables.tf").exists()
        assert (path / "outputs.tf").exists()
