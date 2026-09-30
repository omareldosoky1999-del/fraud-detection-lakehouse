# ADR-0006: Time-based Iceberg partitioning with row-level batch idempotency

## Status
Accepted.

## Decision

Iceberg tables are partitioned by business/event time only:

- Bronze: ingest_date
- Quarantine: quarantine_date
- Silver: event_date
- Features: event_date
- Decisions: event_date

The deterministic batch_token remains a row-level idempotency key.

For each micro-batch, the Iceberg writer:

1. ensures the table exists
2. deletes rows matching the current batch_token
3. appends the complete micro-batch
4. only then allows the external committed marker to be written

## Why

Using a unique batch_token as a partition key creates one or more new partitions for every micro-batch. In a long-running streaming workload this causes partition and metadata growth that is unrelated to query dimensions.

Time-based partitions align with fraud analytics access patterns and keep table metadata bounded relative to event history.

The delete-then-append pattern preserves retry idempotency without embedding operational batch identity into the physical partition layout.

## Failure semantics

The multi-table write is not a single cross-table transaction. A process failure may leave some tables updated and others untouched. The deterministic batch token plus the absence of the external committed marker makes the batch retryable: every affected table removes the previous partial batch before appending it again.

## Consequence

Operational metadata can still be queried by batch_token, while Trino and Iceberg optimize around event dates rather than streaming job internals.
