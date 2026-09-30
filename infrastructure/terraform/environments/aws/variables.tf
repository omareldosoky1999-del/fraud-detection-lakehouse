variable "region" {
  type    = string
  default = "eu-central-1"
}

variable "project_name" {
  type    = string
  default = "fraud-detection"
}

variable "environment" {
  type    = string
  default = "dev"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique bucket for the fraud lakehouse."
}

variable "create_network" {
  type    = bool
  default = false
}

variable "vpc_name" {
  type    = string
  default = "fraud-detection-vpc"
}

variable "vpc_cidr" {
  type    = string
  default = "10.20.0.0/16"
}

variable "availability_zones" {
  type    = list(string)
  default = ["eu-central-1a", "eu-central-1b"]
}

variable "public_subnet_cidrs" {
  type    = list(string)
  default = ["10.20.1.0/24", "10.20.2.0/24"]
}

variable "private_subnet_cidrs" {
  type    = list(string)
  default = ["10.20.11.0/24", "10.20.12.0/24"]
}

variable "single_nat_gateway" {
  type    = bool
  default = true
}

variable "enable_eks" {
  type    = bool
  default = false
}

variable "eks_cluster_name" {
  type    = string
  default = "fraud-detection-eks"
}

variable "eks_kubernetes_version" {
  type    = string
  default = "1.34"
}

variable "eks_subnet_ids" {
  type    = list(string)
  default = []
}

variable "eks_node_instance_types" {
  type    = list(string)
  default = ["t3.large"]
}

variable "eks_node_min" {
  type    = number
  default = 1
}

variable "eks_node_desired" {
  type    = number
  default = 2
}

variable "eks_node_max" {
  type    = number
  default = 4
}