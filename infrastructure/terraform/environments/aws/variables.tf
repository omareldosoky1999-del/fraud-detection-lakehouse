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
