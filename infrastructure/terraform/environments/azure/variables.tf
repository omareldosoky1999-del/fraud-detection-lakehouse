variable "resource_group_name" {
  type    = string
  default = "rg-fraud-detection-dev"
}

variable "location" {
  type    = string
  default = "West Europe"
}

variable "storage_account_name" {
  type        = string
  description = "Globally unique storage account name."
}

variable "container_name" {
  type    = string
  default = "iceberg"
}
