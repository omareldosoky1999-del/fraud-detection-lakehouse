# Fraud platform Helm deployment

This chart packages the production streaming workload as a SparkApplication for Kubernetes. Install the Kubeflow Spark Operator separately, then deploy this chart with the environment-specific values file.

The chart uses Spark Operator API `sparkoperator.k8s.io/v1beta2` and a configurable Spark 3.5.9 application image. OpenLineage, MLflow Model Registry, Polaris and the selected Iceberg FileIO remain runtime configuration rather than application-code branches.

Examples:

```bash
helm upgrade --install fraud-platform ./deploy/helm/fraud-platform \
  --namespace fraud-platform --create-namespace \
  -f ./deploy/helm/fraud-platform/values-aws.yaml
```

The AWS/Azure/GCP value files contain intentional placeholder service endpoints. Replace them with the private endpoints produced by the corresponding cloud deployment. Secrets are injected by the cluster identity/secret mechanism and are not stored in this chart.

For operator installation, use the official Spark Operator Helm repository. The operator supports automatic application restart and Prometheus application metrics.
