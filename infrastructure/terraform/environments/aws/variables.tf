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
