module "object_storage" {
  source = "../../modules/azure_object_storage"

  resource_group_name   = var.resource_group_name
  location              = var.location
  storage_account_name  = var.storage_account_name
  container_name        = var.container_name
}
