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
