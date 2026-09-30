module "object_storage" {
  source = "../../modules/azure_object_storage"

  resource_group_name  = var.resource_group_name
  location             = var.location
  storage_account_name = var.storage_account_name
  container_name       = var.container_name
}

module "network" {
  count = var.create_network ? 1 : 0

  source = "../../modules/azure_vnet"

  resource_group_name       = var.resource_group_name
  location                  = var.location
  name                      = var.vnet_name
  address_space             = var.vnet_address_space
  aks_subnet_address_prefix = var.aks_subnet_address_prefix
}

locals {
  aks_subnet_id = var.create_network ? module.network[0].aks_subnet_id : var.aks_subnet_id
}

module "aks" {
  count = var.enable_aks ? 1 : 0

  source = "../../modules/azure_aks"

  name                = var.aks_name
  resource_group_name = var.resource_group_name
  location            = var.location
  subnet_id           = local.aks_subnet_id
  kubernetes_version  = var.aks_kubernetes_version
  node_count          = var.aks_node_count
  vm_size             = var.aks_vm_size
}

module "workload_identity" {
  count = var.enable_aks ? 1 : 0

  source = "../../modules/azure_workload_identity"

  name                = "${var.aks_name}-fraud-spark"
  resource_group_name = var.resource_group_name
  location            = var.location
  oidc_issuer_url     = module.aks[0].oidc_issuer_url
  namespace           = var.workload_namespace
  service_account     = var.workload_service_account
  storage_scope       = module.object_storage.storage_account_id

  depends_on = [module.aks]
}
