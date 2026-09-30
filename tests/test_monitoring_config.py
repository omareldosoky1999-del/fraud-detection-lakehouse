from pathlib import Path

import yaml

ROOT = Path(__file__).parents[1]


def test_prometheus_loads_rule_files():
    data = yaml.safe_load(
        (ROOT / "monitoring" / "prometheus.yml").read_text(encoding="utf-8")
    )
    assert data["rule_files"]
    assert "monitoring/alerts.yml" in data["rule_files"]
    assert "monitoring/rules/recording.yml" in data["rule_files"]


def test_alert_rules_have_explicit_severity_and_service():
    data = yaml.safe_load(
        (ROOT / "monitoring" / "alerts.yml").read_text(encoding="utf-8")
    )
    rules = data["groups"][0]["rules"]
    assert len(rules) >= 5
    for rule in rules:
        assert rule["labels"]["severity"] in {"warning", "critical"}
        assert rule["labels"]["service"]
