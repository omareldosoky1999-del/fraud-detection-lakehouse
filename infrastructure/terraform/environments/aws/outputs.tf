output "lakehouse_bucket" {
  value = module.object_storage.bucket_name
}

output "lakehouse_bucket_arn" {
  value = module.object_storage.bucket_arn
}

output "eks_cluster_name" {
  value = var.enable_eks ? module.eks[0].cluster_name : null
}

output "eks_oidc_issuer" {
  value = var.enable_eks ? module.eks[0].oidc_issuer : null
}
