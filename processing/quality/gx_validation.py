"""Great Expectations validation for fraud platform Silver batches."""
from __future__ import annotations

from typing import Any

import great_expectations as gx
from pyspark.sql import DataFrame


def validate_silver_dataframe(df: DataFrame) -> dict[str, Any]:
    """Run the Silver data contract against a Spark DataFrame.

    GX is deliberately used as a batch gate after the real-time Silver write,
    not inside every streaming micro-batch. Low-level DQ still performs the
    immediate quarantine decision; this contract provides an independent,
    declarative validation boundary before downstream Gold consumption.
    """
    context = gx.get_context(mode="ephemeral")
    data_source = context.data_sources.add_spark(name="fraud_silver_spark")
    asset = data_source.add_dataframe_asset(name="silver_runtime_batch")
    batch = asset.add_batch_definition_whole_dataframe("runtime").get_batch(
        batch_parameters={"dataframe": df}
    )

    expectations = [
        gx.expectations.ExpectColumnValuesToNotBeNull(column="transaction_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="client_id"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="event_time"),
        gx.expectations.ExpectColumnValuesToBeBetween(
            column="amount_usd",
            min_value=0,
            strict_min=False,
        ),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="currency"),
        gx.expectations.ExpectColumnValuesToNotBeNull(column="country_src"),
    ]

    results = []
    for expectation in expectations:
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
    }


def assert_silver_dataframe(df: DataFrame) -> dict[str, Any]:
    report = validate_silver_dataframe(df)
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
