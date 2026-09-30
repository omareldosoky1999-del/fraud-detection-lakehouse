import yaml
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



def test_workload_identity_modules_exist():
    for module, marker in {
        "aws_workload_identity": "aws_eks_pod_identity_association",
        "azure_workload_identity": "azurerm_federated_identity_credential",
        "gcp_workload_identity": "google_service_account_iam_member",
    }.items():
        text = (ROOT / "modules" / module / "main.tf").read_text(encoding="utf-8")
        assert marker in text


def test_aws_eks_includes_pod_identity_agent():
    text = (ROOT / "modules" / "aws_eks" / "main.tf").read_text(encoding="utf-8")
    assert 'addon_name   = "eks-pod-identity-agent"' in text


def test_cloud_environments_export_workload_identity_reference():
    expected = {
        "aws": 'workload_identity_role_arn',
        "azure": 'workload_identity_client_id',
        "gcp": 'workload_identity_service_account_email',
    }
    for env, marker in expected.items():
        text = (ROOT / "environments" / env / "outputs.tf").read_text(encoding="utf-8")
        assert f'output "{marker}"' in text



def test_cloud_storage_profiles_use_workload_identity():
    profiles = yaml.safe_load(
        (ROOT.parents[1] / "config" / "storage_profiles.yml").read_text(
            encoding="utf-8"
        )
    )["storage_profiles"]

    assert profiles["aws"]["authentication"] == "eks_pod_identity"
    assert profiles["azure"]["authentication"] == "aks_workload_identity"
    assert profiles["gcp"]["authentication"] == "gke_workload_identity_federation"
