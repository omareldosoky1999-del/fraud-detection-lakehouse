variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique GCS bucket name."
}

variable "create_network" {
  type    = bool
  default = false
}

variable "gke_network_name" {
  type    = string
  default = "fraud-detection-vpc"
}

variable "gke_subnet_cidr" {
  type    = string
  default = "10.40.0.0/20"
}

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
