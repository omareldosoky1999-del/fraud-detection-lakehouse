module "object_storage" {
  source = "../../modules/aws_object_storage"

  bucket_name = var.bucket_name

  tags = {
    Project     = var.project_name
    Environment = var.environment
    ManagedBy   = "terraform"
    Workload    = "fraud-detection-lakehouse"
  }
}
