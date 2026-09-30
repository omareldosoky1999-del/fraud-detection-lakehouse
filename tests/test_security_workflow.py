from pathlib import Path

ROOT = Path(__file__).parents[1]


def test_codeql_security_workflow_contract():
    workflow = (ROOT / ".github" / "workflows" / "security-codeql.yml").read_text(
        encoding="utf-8"
    )
    config = (ROOT / ".github" / "codeql-config.yml").read_text(encoding="utf-8")

    assert "github/codeql-action/init@v4.38.2" in workflow
    assert "github/codeql-action/analyze@v4.38.2" in workflow
    assert "python" in workflow
    assert "actions" in workflow
    assert "security-events: write" in workflow
    assert "ingestion/data" in config
