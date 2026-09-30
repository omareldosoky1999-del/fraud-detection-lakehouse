"""Great Expectations validation for the fraud platform Silver layer."""
from __future__ import annotations

from pathlib import Path
from typing import Any

import great_expectations as gx
import yaml
from pyspark.sql import DataFrame

PROJECT_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_CONTRACT = PROJECT_ROOT / "config" / "quality" / "silver.yml"

def _resolve_contract_path(path: str | Path | None = None) -> Path:
    """Resolve the quality contract independently of the process working directory."""
    if path is not None:
        candidate = Path(path)
        if candidate.is_absolute():
            return candidate
        return Path.cwd() / candidate
    return DEFAULT_CONTRACT


def load_silver_contract(path: str | Path | None = None) -> dict[str, Any]:
    contract_path = _resolve_contract_path(path)
    data = yaml.safe_load(contract_path.read_text(encoding="utf-8")) or {}
    silver = data.get("quality", {}).get("silver", {})
    if not silver:
        raise ValueError(f"Silver quality contract is empty: {contract_path}")
    return silver


def build_expectations(contract: dict[str, Any]) -> list[Any]:
    expectations = []

    for column in contract.get("not_null_columns", []):
        expectations.append(
            gx.expectations.ExpectColumnValuesToNotBeNull(column=column)
        )

    for spec in contract.get("non_negative", []):
        expectations.append(
            gx.expectations.ExpectColumnValuesToBeBetween(
                column=spec["column"],
                min_value=spec.get("min_value", 0),
                strict_min=spec.get("strict_min", False),
            )
        )

    if not expectations:
        raise ValueError("Silver quality contract produced no expectations")
    return expectations


def validate_silver_dataframe(
    df: DataFrame,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    """Run the Silver data contract as an independent batch gate.

    The streaming path performs lightweight Spark-native DQ. This validator
    runs after persistence and is intentionally outside the latency-sensitive
    micro-batch decision path.
    """
    contract = load_silver_contract(contract_path)
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(name="fraud_silver_spark")
    asset = data_source.add_dataframe_asset(name="silver_runtime_batch")
    batch = asset.add_batch_definition_whole_dataframe("runtime").get_batch(
        batch_parameters={"dataframe": df}
    )

    results = []
    for expectation in build_expectations(contract):
        result = batch.validate(expectation)
        results.append(
            {
                "expectation": expectation.expectation_type,
                "success": bool(result["success"]),
                "result": result["result"],
            }
        )

    success = all(item["success"] for item in results)
    return {
        "success": success,
        "expectations": results,
        "row_count": df.count(),
        "contract_path": str(contract_path or DEFAULT_CONTRACT),
    }


def assert_silver_dataframe(
    df: DataFrame,
    contract_path: str | Path | None = None,
) -> dict[str, Any]:
    report = validate_silver_dataframe(df, contract_path)
    if not report["success"]:
        failed = [
            item["expectation"]
            for item in report["expectations"]
            if not item["success"]
        ]
        raise ValueError(
            "Great Expectations Silver contract failed: " + ", ".join(failed)
        )
    return report
