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

module "network" {
  count = var.create_network ? 1 : 0

  source               = "../../modules/aws_vpc"
  name                 = var.vpc_name
  cidr_block           = var.vpc_cidr
  availability_zones   = var.availability_zones
  public_subnet_cidrs  = var.public_subnet_cidrs
  private_subnet_cidrs = var.private_subnet_cidrs
  cluster_name         = var.eks_cluster_name
  single_nat_gateway   = var.single_nat_gateway
}

locals {
  eks_subnet_ids = var.create_network ? module.network[0].private_subnet_ids : var.eks_subnet_ids
}

module "eks" {
  count = var.enable_eks ? 1 : 0

  source = "../../modules/aws_eks"

  cluster_name        = var.eks_cluster_name
  kubernetes_version  = var.eks_kubernetes_version
  subnet_ids          = local.eks_subnet_ids
  node_instance_types = var.eks_node_instance_types
  node_min            = var.eks_node_min
  node_desired        = var.eks_node_desired
  node_max            = var.eks_node_max
}
module "workload_identity" {
  count = var.enable_eks ? 1 : 0

  source = "../../modules/aws_workload_identity"

  name            = "${var.eks_cluster_name}-fraud-spark"
  cluster_name    = var.eks_cluster_name
  namespace       = var.workload_namespace
  service_account = var.workload_service_account
  bucket_arn      = module.object_storage.bucket_arn

  depends_on = [module.eks]
}
