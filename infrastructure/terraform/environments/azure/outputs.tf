output "storage_account_name" {
  value = module.object_storage.storage_account_name
}

output "container_name" {
  value = module.object_storage.container_name
}

output "vnet_id" {
  value = var.create_network ? module.network[0].vnet_id : null
}

output "aks_subnet_id" {
  value = var.create_network ? module.network[0].aks_subnet_id : var.aks_subnet_id
}

output "aks_cluster_name" {
  value = var.enable_aks ? module.aks[0].cluster_name : null
}

output "aks_oidc_issuer_url" {
  value = var.enable_aks ? module.aks[0].oidc_issuer_url : null
}
