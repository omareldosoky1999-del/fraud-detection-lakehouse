output "storage_account_name" {
  value = azurerm_storage_account.this.name
}

output "storage_account_id" {
  value = azurerm_storage_account.this.id
}

output "container_name" {
  value = azurerm_storage_container.this.name
}

output "container_id" {
  value = azurerm_storage_container.this.id
}
