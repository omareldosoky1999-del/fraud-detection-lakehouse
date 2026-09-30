variable "name" { type = string }
variable "resource_group_name" { type = string }
variable "location" { type = string }
variable "subnet_id" { type = string, description = "Existing subnet ID for the AKS agent pool." }
variable "kubernetes_version" { type = string, default = "1.33" }
variable "node_count" { type = number, default = 2 }
variable "vm_size" { type = string, default = "Standard_D4s_v5" }
