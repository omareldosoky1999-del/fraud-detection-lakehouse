variable "enable_gke" {
  type    = bool
  default = false
}

variable "gke_name" {
  type    = string
  default = "fraud-detection-gke"
}

variable "gke_network" {
  type    = string
  default = ""
}

variable "gke_subnetwork" {
  type    = string
  default = ""
}

variable "gke_node_count" {
  type    = number
  default = 2
}

variable "gke_machine_type" {
  type    = string
  default = "e2-standard-4"
}
