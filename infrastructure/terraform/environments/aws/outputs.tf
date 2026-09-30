output "lakehouse_bucket" {
  value = module.object_storage.bucket_name
}

output "lakehouse_bucket_arn" {
  value = module.object_storage.bucket_arn
}

output "vpc_id" {
  value = var.create_network ? module.network[0].vpc_id : null
}

output "private_subnet_ids" {
  value = var.create_network ? module.network[0].private_subnet_ids : []
}

output "eks_cluster_name" {
  value = var.enable_eks ? module.eks[0].cluster_name : null
}

output "eks_oidc_issuer" {
  value = var.enable_eks ? module.eks[0].oidc_issuer : null
}

output "workload_identity_role_arn" {
  value = var.enable_eks ? module.workload_identity[0].role_arn : null
}
