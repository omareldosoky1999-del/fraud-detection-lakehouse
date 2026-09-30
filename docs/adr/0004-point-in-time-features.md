# ADR-0004: Shared point-in-time fraud features

## Status
Accepted.

## Decision

Feature engineering is a first-class layer between Silver and the fraud decision stage.

The same feature builder is used by:

- offline Spark MLlib training
- online Structured Streaming inference
- persisted Iceberg feature records

The feature builder is causal: a transaction never reads a later transaction to compute its features.

## Feature contract

- `txn_count_5m`
- `amount_sum_1h`
- `avg_amount_prior_30`
- `stddev_amount_prior_30`
- `seconds_since_prev_txn`
- `device_new_30d`
- `country_changed`
- `amount_to_prior_avg`

## Why

Training-serving skew is a common source of degraded model behavior. Centralizing feature definitions keeps offline and online computation under the same code contract.

Feature records are also retained in Iceberg as `features.transaction_features` so model inputs can be audited alongside the corresponding transaction and decision records.

## Consequences

The streaming job must construct a sufficient recent history window before feature calculation. The current implementation uses Spark window functions and a bounded 30-day device lookback. A future high-volume deployment can replace the window implementation with stateful streaming feature computation without changing the feature column contract.
