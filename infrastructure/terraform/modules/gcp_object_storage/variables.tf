variable "project_id" {
  type        = string
  description = "GCP project ID."
}

variable "location" {
  type    = string
  default = "EU"
}

variable "bucket_name" {
  type        = string
  description = "Globally unique GCS bucket name."
}
