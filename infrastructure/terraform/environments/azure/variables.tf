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

variable "create_network" {
  type    = bool
  default = false
}

variable "vnet_name" {
  type    = string
  default = "fraud-detection-vnet"
}

variable "vnet_address_space" {
  type    = list(string)
  default = ["10.30.0.0/16"]
}

variable "aks_subnet_address_prefix" {
  type    = string
  default = "10.30.1.0/24"
}

variable "enable_aks" {
  type    = bool
  default = false
}

variable "aks_name" {
  type    = string
  default = "fraud-detection-aks"
}

variable "aks_kubernetes_version" {
  type    = string
  default = "1.33"
}

variable "aks_subnet_id" {
  type    = string
  default = ""
}

variable "aks_node_count" {
  type    = number
  default = 2
}

variable "aks_vm_size" {
  type    = string
  default = "Standard_D4s_v5"
}

variable "workload_namespace" {
  type    = string
  default = "fraud-platform"
}

variable "workload_service_account" {
  type    = string
  default = "fraud-spark"
}
