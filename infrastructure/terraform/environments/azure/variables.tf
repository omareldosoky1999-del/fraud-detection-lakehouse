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
