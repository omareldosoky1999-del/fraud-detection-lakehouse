variable "project_id" {
  type = string
}

variable "name" {
  type = string
}

variable "region" {
  type    = string
  default = "europe-west1"
}

variable "subnet_cidr" {
  type    = string
  default = "10.40.0.0/20"
}
