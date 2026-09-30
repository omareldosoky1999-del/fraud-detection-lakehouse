# ADR-0006: External Secrets Operator for cloud secret delivery

## Status
Accepted.

## Decision

The Helm chart supports an optional `ExternalSecret` resource. The chart never stores secret values or cloud credentials. It references a pre-provisioned `ClusterSecretStore` and synchronizes selected provider-side secrets into the Kubernetes Secret consumed by Spark.

Supported provider contract:

- AWS: AWS Secrets Manager
- Azure: Azure Key Vault
- GCP: Google Cloud Secret Manager

Each cloud profile defaults to a provider-specific `ClusterSecretStore` name and a common target Secret named `fraud-platform-runtime`. The actual store and its cloud identity are managed outside the application chart.

## Why

External Secrets Operator provides a Kubernetes abstraction over external secret systems and can synchronize provider-side secrets into Kubernetes Secrets. This keeps secret lifecycle and rotation outside application manifests.

Authentication is cloud-native:

- AWS can use the controller Pod Identity path.
- Azure uses Workload Identity.
- GCP uses GKE Workload Identity or federation as appropriate.

## Safety boundary

`externalSecrets.enabled` is false by default. A deployment enables it only when the corresponding External Secrets Operator CRDs and ClusterSecretStore already exist.

Secret values never appear in Git, Terraform outputs, Helm values, or the Docker image.
