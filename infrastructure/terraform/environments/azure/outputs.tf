output "storage_account_name" {
  value = module.object_storage.storage_account_name
}

output "container_name" {
  value = module.object_storage.container_name
}

output "aks_cluster_name" {
  value = var.enable_aks ? module.aks[0].cluster_name : null
}

output "aks_oidc_issuer_url" {
  value = var.enable_aks ? module.aks[0].oidc_issuer_url : null
}
