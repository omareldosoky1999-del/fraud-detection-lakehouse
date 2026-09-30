# ADR-0002: Make MLlib + MLflow the managed fraud decision model lifecycle

## Status
Accepted.

## Decision

Spark MLlib remains the inference engine inside Structured Streaming. MLflow becomes the lifecycle control plane for the same models:

```text
Spark MLlib training
    -> MLflow Tracking
    -> PostgreSQL metadata
    -> RustFS/S3-compatible artifacts
    -> Model Registry
    -> production alias
    -> Spark streaming inference
```

The ensemble is registered as three independently versioned members:

- `fraud_logistic_regression`
- `fraud_random_forest`
- `fraud_gbt`

The streaming application resolves the configured alias for all members at startup rather than reading an implicit model from a local filesystem.

## Why

A financial fraud platform needs model lineage, reproducible versions, auditable metrics and controlled promotion. Keeping only Spark `PipelineModel` directories makes deployment dependent on shared filesystem state and makes rollback or promotion harder.

MLflow's Tracking Server supports a database backend such as PostgreSQL and remote artifact storage, while Model Registry aliases provide a mutable deployment reference such as `models:/name@production`.

## Consequences

- Training must have an MLflow Tracking URI.
- Every successful training run creates a version for each ensemble member.
- The configured alias is assigned only after the corresponding model is logged successfully.
- Local model directories remain as a diagnostic/offline-development mirror, not the production source of truth.
- Future cloud deployments can replace RustFS with S3, ADLS Gen2 or GCS without changing the Spark model code.
- Authentication, secret management and promotion gates remain environment-specific infrastructure concerns.

## Validation

`scripts/mlflow_smoke.py` validates:

1. Spark MLlib model training.
2. MLflow Tracking Server connectivity.
3. Model artifact logging.
4. Model Registry version creation.
5. Alias assignment.
6. Alias-based model resolution.
