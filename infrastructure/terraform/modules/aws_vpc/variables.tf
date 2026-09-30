variable "name" {
  type = string
}

variable "cidr_block" {
  type    = string
  default = "10.20.0.0/16"
}

variable "availability_zones" {
  type        = list(string)
  description = "Two or more availability zones."
}

variable "public_subnet_cidrs" {
  type = list(string)
}

variable "private_subnet_cidrs" {
  type = list(string)
}

variable "cluster_name" {
  type    = string
  default = ""
}

variable "single_nat_gateway" {
  type    = bool
  default = true
}
