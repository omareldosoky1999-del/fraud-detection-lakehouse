variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "storage_account_name" {
  type        = string
  description = "Globally unique Azure Storage account name."
}

variable "container_name" {
  type    = string
  default = "iceberg"
}
