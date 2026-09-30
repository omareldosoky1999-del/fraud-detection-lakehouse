resource "azurerm_user_assigned_identity" "this" {
  name                = var.name
  resource_group_name = var.resource_group_name
  location            = var.location
}

resource "azurerm_federated_identity_credential" "this" {
  name                      = "${var.name}-federation"
  audience                  = ["api://AzureADTokenExchange"]
  issuer                    = var.oidc_issuer_url
  user_assigned_identity_id = azurerm_user_assigned_identity.this.id
  subject                   = "system:serviceaccount:${var.namespace}:${var.service_account}"
}

resource "azurerm_role_assignment" "storage" {
  scope                = var.storage_scope
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_user_assigned_identity.this.principal_id
}
