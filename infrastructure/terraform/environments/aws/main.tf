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

module "eks" {
  count = var.enable_eks ? 1 : 0

  source = "../../modules/aws_eks"

  cluster_name         = var.eks_cluster_name
  kubernetes_version   = var.eks_kubernetes_version
  subnet_ids           = var.eks_subnet_ids
  node_instance_types  = var.eks_node_instance_types
  node_min             = var.eks_node_min
  node_desired         = var.eks_node_desired
  node_max             = var.eks_node_max
}
