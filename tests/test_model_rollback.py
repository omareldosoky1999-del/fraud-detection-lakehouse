from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_rollback_script_checks_validated_run():
    path = ROOT / "scripts" / "rollback_ensemble.py"
    text = path.read_text(encoding="utf-8")
    assert "validation_status" in text
    assert "ensemble_run_id" in text
    assert "set_registered_model_alias" in text


def test_rollback_workflow_uses_production_environment():
    path = ROOT / ".github" / "workflows" / "model-rollback.yml"
    text = path.read_text(encoding="utf-8")
    assert "workflow_dispatch" in text
    assert "environment: production" in text
    assert "--target-run-id" in text



def test_rollback_selects_same_validated_run_for_all_members(monkeypatch):
    import importlib.util

    script_path = ROOT / "scripts" / "rollback_ensemble.py"
    spec = importlib.util.spec_from_file_location("rollback_ensemble", script_path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)

    class Version:
        def __init__(self, name, version, run_id, tags):
            self.name = name
            self.version = str(version)
            self.run_id = run_id
            self.tags = tags

    class FakeClient:
        def __init__(self, *args, **kwargs):
            self.alias_updates = []

        def search_model_versions(self, query):
            name = query.split("'")[1]
            return [
                Version(
                    name,
                    3,
                    "target-run",
                    {
                        "validation_status": "PASSED",
                        "ensemble_run_id": "target-run",
                    },
                )
            ]

        def set_registered_model_alias(self, name, alias, version):
            self.alias_updates.append((name, alias, str(version)))

    client = FakeClient()

    monkeypatch.setattr(module, "MlflowClient", lambda *a, **k: client)
    monkeypatch.setattr(
        module,
        "registry_members",
        lambda _path: [
            {"registered_name": "fraud_logistic_regression"},
            {"registered_name": "fraud_random_forest"},
            {"registered_name": "fraud_gbt"},
        ],
    )
    monkeypatch.setattr(module, "production_alias", lambda _path: "production")

    run_id, versions = module.rollback(
        "http://mlflow:5000",
        "target-run",
    )

    assert run_id == "target-run"
    assert len(versions) == 3
    assert client.alias_updates == [
        ("fraud_logistic_regression", "production", "3"),
        ("fraud_random_forest", "production", "3"),
        ("fraud_gbt", "production", "3"),
    ]
