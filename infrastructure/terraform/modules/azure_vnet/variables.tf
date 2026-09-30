variable "resource_group_name" {
  type = string
}

variable "location" {
  type = string
}

variable "name" {
  type = string
}

variable "address_space" {
  type    = list(string)
  default = ["10.30.0.0/16"]
}

variable "aks_subnet_address_prefix" {
  type    = string
  default = "10.30.1.0/24"
}
