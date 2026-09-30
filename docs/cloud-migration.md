# Cloud migration contract

The application is designed so the fraud-detection business pipeline stays the same while infrastructure swaps underneath it.

## Storage mapping

| Environment | Object storage | Iceberg FileIO | Authentication |
| --- | --- | --- | --- |
| Local | RustFS | S3FileIO | local development credentials |
| AWS | S3 | S3FileIO | IAM role / workload identity |
| Azure | ADLS Gen2 | ADLSFileIO | managed identity / workload identity |
| GCP | GCS | GCSFileIO | workload identity |

Apache Iceberg 1.11.0 ships dedicated AWS, GCP and Azure bundles, and the Spark runtime reads the FileIO implementation from `ICEBERG_FILEIO_IMPL`.

## Catalog

Polaris remains the Iceberg REST catalog abstraction. The catalog storage configuration is environment-specific; local Docker uses RustFS and cloud environments use the corresponding object-store integration.

## Compute

The deployment target is Kubernetes:

- AWS: EKS + Spark on Kubernetes
- Azure: AKS + Spark on Kubernetes
- GCP: GKE + Spark on Kubernetes

The Kubeflow Spark Operator exposes Spark applications as Kubernetes custom resources. Its current API is `sparkoperator.k8s.io/v1beta2`, and its 2.2.x line is based on Spark 3.5.x.

## Security boundary

No cloud secret is committed to the repository. Use workload identity, managed identity, and provider-native secret systems in cloud environments. Terraform CI performs syntax/validation only; applies require an explicitly authenticated operator or separately governed deployment pipeline.

## Kubernetes deployment contract

The Spark runtime is packaged as a dedicated application image and published to GHCR from `main`. Cloud releases should reference an immutable `sha-<commit>` image tag. The Helm chart accepts this through `image.immutableTag`.

The cloud value files reference a pre-created Kubernetes Secret named `fraud-platform-runtime`. The Secret is intentionally not rendered by this repository. At minimum it should provide the `POLARIS_CREDENTIAL` required by the Iceberg REST catalog; provider-specific cloud credentials should be supplied through workload identity or managed identity rather than static access keys whenever the cloud platform supports it. Spark Operator supports both `envFrom` and secret-backed environment variables in the `v1beta2` SparkApplication API. citeturn255934search0

A manual GitHub Actions workflow, `.github/workflows/deploy-helm.yml`, deploys the selected AWS/Azure/GCP values with Helm `--atomic` and requires an operator-provided `KUBE_CONFIG_DATA` secret. The workflow is deliberately manual so applying cloud infrastructure remains an explicit operator action.
