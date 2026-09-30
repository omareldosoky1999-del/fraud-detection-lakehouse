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
