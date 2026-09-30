terraform {
  required_version = ">= 1.16.0, < 2.0.0"
}

locals {
  environment = "local"
  object_store = {
    type             = "s3-compatible"
    endpoint         = "http://localhost:9000"
    iceberg_bucket   = "iceberg"
    mlflow_artifacts = "mlflow-artifacts"
  }
}

output "environment" {
  value = local.environment
}

output "object_store" {
  value = local.object_store
}
