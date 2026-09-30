variable "name" {
  type = string
}

variable "project_id" {
  type = string
}

variable "location" {
  type = string
}

variable "network" {
  type = string
}

variable "subnetwork" {
  type = string
}

variable "release_channel" {
  type    = string
  default = "REGULAR"
}

variable "node_count" {
  type    = number
  default = 2
}

variable "machine_type" {
  type    = string
  default = "e2-standard-4"
}
