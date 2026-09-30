# ADR-0005: Great Expectations as the downstream Silver quality gate

## Status
Accepted.

## Decision

Use Great Expectations for an independent, declarative validation gate between persisted Silver data and downstream Gold processing.

The real-time path keeps lightweight Spark-native DQ checks for immediate quarantine. Great Expectations validates the resulting Silver partition as a batch before Gold is built.

## Contract

The initial contract verifies:

- required transaction/client identifiers are not null
- event timestamps are not null
- amount_usd is non-negative
- currency and source-country values are present

## Why

Putting a full declarative validation framework inside every streaming micro-batch would add repeated framework startup and validation overhead to the latency-sensitive fraud decision path.

A separate Silver-to-Gold validation stage gives the platform two independent controls:

1. real-time correctness for fraud processing and quarantine
2. downstream data contract enforcement for analytics and reporting

## Runtime

Great Expectations is pinned to `1.23.2` and uses its Spark DataFrame integration directly against the existing Spark session.

## Consequences

A failed Silver quality contract blocks the daily Gold build. Validation output is written to `reports/dq/` for auditability. Future cloud deployments can keep the same quality code while changing only the underlying storage location.
